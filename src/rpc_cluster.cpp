#include "rpc_global.h"
#include "rpc_cluster.h"

#include <cinttypes>

#include "common.h"

#include "cjson/cjson.h"

#include "asyncpool.h"
#include "config.h"
#include "sentinel.h"
#include "database.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "names.h"
#include "http_serve.h"

//#include "trigger.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;


void RpcCluster::init(const openset::web::MessagePtr message, const RpcMapping& matches)
{
    const auto partitions = openset::globals::async;
    const auto partitionMax = message->getParamInt("partitions"s, 0);

    if (partitionMax < 1 || partitionMax > 1000)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "partitions must be >= 1 and <= 1000" },
                message);
        return;
    }

    if (partitions->isRunning())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "This instance is already part of a cluster" },
                message);
        return;
    }

    // remove any existing mapping
    globals::mapper->removeRoute(globals::running->nodeId);

    // update config
    {
        csLock lock(globals::running->cs);
        globals::running->setNodeName(openset::config::createName());
        globals::running->state = openset::config::NodeState_e::active;
        globals::running->partitionMax = partitionMax;
        Logger::get().info("Initialized as: '" + globals::running->nodeName + "'.");
    }

    openset::globals::mapper->partitionMap.clear();
    for (auto i = 0; i < partitionMax; ++i)
        openset::globals::mapper->partitionMap.setOwner(i, globals::running->nodeId);

    // set number of partitions
    partitions->setPartitionMax(partitionMax);
    // set them running - this return right away
    partitions->startAsync();

    partitions->mapPartitionsToAsyncWorkers();

    Logger::get().info(globals::running->nodeName + " configured for " + to_string(partitionMax) + " partitions.");

    cjson response;
    response.set("server_name", globals::running->nodeName);

    // routes are broadcast to nodes, we use the external host and port
    // so that nodes can find each other in containered situations where
    // the container doesn't know it's own IP and ports are mapped
    openset::globals::mapper->addRoute(
        globals::running->nodeName,
        globals::running->nodeId,
        globals::running->hostExternal,
        globals::running->portExternal);

    message->reply(http::StatusCode::success_ok, response);
}


void RpcCluster::join(const openset::web::MessagePtr message, const RpcMapping& matches)
{

    if (globals::running->state != openset::config::NodeState_e::active)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::route_error,
                "node_not_initialized" },
                message);
        return;
    }

    const auto host = message->getParamString("host");
    const auto port = message->getParamInt("port", 8080);

    const auto newNodeName = openset::config::createName();
    const auto newNodeId = MakeHash(newNodeName);

    if (!host.length() || !port)
    {
        Logger::get().error("invite node: missing params.");
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "missing host. Use param: host={host|ip}" },
                message);
        return;
    }

    // Step 1 - Verify that the remote node exists and is able to join
    {
        openset::web::Rest client(0, host + ":" + to_string(port));

        auto error = false;
        auto ready = false;

        cjson responseJson;

        client.request("GET", "/v1/internode/is_member", {}, nullptr, 0, [&error, &ready, &responseJson](http::StatusCode status, bool err, cjson json) mutable
        {
            error = err;

            if (!err)
                responseJson = std::move(json);

            ready = true;
        });

        // dumb loop, wait for callback
        while (!ready)
            ThreadSleep(50);

        if (error || responseJson.memberCount == 0)
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "target node could not be reached." },
                    message);
            return;
        }

        // node is already part of a cluster
        if (responseJson.xPathBool("/part_of_cluster", true))
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "target node already part of a cluster." },
                    message);
            return;
        }
    }


    // Step 2 - The remote node is open to being configured, lets send it the entire config
    {
        // TODO glue together the whole config
        cjson configBlock;

        configBlock.set("node_name", newNodeName);
        configBlock.set("node_id", newNodeId);
        configBlock.set("partition_max", cast<int64_t>(openset::globals::async->partitionMax));

        // make am array node called tables, push the tables, triggers, columns into the array
        auto tables = configBlock.setArray("tables");

        for (auto n : openset::globals::database->tables)
        {
            auto tableItem = tables->pushObject();

            tableItem->set("name", n.second->getName());

            // here we are just making nodes and passing them to
            // the serialize functions so the data becomes a series of
            // objects within the tables list/array created above
            n.second->serializeTable(tableItem->setObject("table"));
            n.second->serializeTriggers(tableItem->setObject("triggers"));
        }

        // make a node called routes, serialize the routes (nodes) under it
        openset::globals::mapper->serializeRoutes(configBlock.setObject("routes"));

        // make a node called cluster, serialize the partitionMap under it
        openset::globals::mapper->getPartitionMap()->serializePartitionMap(configBlock.setObject("cluster"));

        auto rpcJson = cjson::stringify(&configBlock);

        Logger::get().info("configuring node " + newNodeName + "@" + host + ":" + to_string(port) + ".");

        const auto hostPort = host + ":" + to_string(port);
        openset::web::Rest client(0, hostPort);

        auto error = false;
        auto ready = false;

        cjson responseJson;

        // send command that joins remote node to this cluster, this transfers all
        // config to the remote node.
        client.request(
            "POST",
            "/v1/internode/join_to_cluster",
            {},
            &rpcJson[0],
            rpcJson.length(),
            [&error, &ready, &responseJson](http::StatusCode status, bool err, cjson json)
        {
            error = err;

            if (!err)
                responseJson = std::move(json);

            ready = true;
        });

        // dumb loop, wait for callback
        while (!ready)
            ThreadSleep(50);


        if (error || responseJson.memberCount == 0)
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "target node could not be reached." },
                    message);
            return;
        }

        // copy to string
        if (!responseJson.xPathBool("/configured", false))
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "target node could not be configured." },
                    message);
            return;
        }

    }

    // add the new node to the local dispatchAsync list, then
    // fork out the node_add command to any other nodes
    {
        Logger::get().info("broadcasting membership for node " + newNodeName + " @" + host + ":" + to_string(port));

        // add the new route to the local route map, that way it will
        // receive the broadcast in the next step
        openset::globals::mapper->addRoute(newNodeName, newNodeId, host, port);

        // tell all the nodes (including our new node) about
        // the new node.
        cjson newNode;
        //params->set("config_version", globals::running->updateConfigVersion());
        newNode.set("node_name", newNodeName);
        newNode.set("node_id", newNodeId);
        newNode.set("host", host);
        newNode.set("port", port);

        auto newNodeJson = cjson::stringify(&newNode);

        auto addResponses = openset::globals::mapper->dispatchCluster(
            "POST",
            "/v1/internode/add_node",
            {},
            newNode,
            false);

        openset::globals::mapper->releaseResponses(addResponses);

    }

    // respond to client
    cjson response;
    response.set("node_joined", true);
    message->reply(http::StatusCode::success_ok, response);
}

