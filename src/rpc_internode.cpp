#include <stdexcept>
#include <cinttypes>
#include <regex>

#include "common.h"

#include "rpc_global.h"
#include "rpc_internode.h"
#include "cjson/cjson.h"
#include "oloop_property.h"
#include "asyncpool.h"
#include "config.h"
#include "sentinel.h"
#include "database.h"
#include "result.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "http_serve.h"
#include "sidelog.h"

//#include "trigger.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

enum class internodeFunction_e : int32_t
{
    init_config_node,
    cluster_member,
    node_add,
    transfer,
    map_change,
    cluster_lock,
    cluster_release,
};

void RpcInternode::is_member(const openset::web::MessagePtr& message, const RpcMapping&)
{
    cjson response;
    response.set("part_of_cluster", globals::running->state != openset::config::NodeState_e::ready_wait);
    message->reply(openset::http::StatusCode::success_ok, response);
}

void RpcInternode::join_to_cluster(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    globals::mapper->removeRoute(globals::running->nodeId);

    const auto request = message->getJSON();
    const auto nodeName = request.xPathString("/node_name", "");
    const auto nodeId = request.xPathInt("/node_id", 0);
    const auto partitionMax = request.xPathInt("/partition_max", 0);

    // TODO - error check here
    Logger::get().info("Joining cluster as: '" + nodeName + "'.");

    // assign a new node id
    {
        csLock lock(openset::globals::running->cs);
        globals::running->nodeId = nodeId;
        globals::running->state = openset::config::NodeState_e::active;
        globals::running->configVersion = 1;
        globals::running->partitionMax = partitionMax;
    }

    // create the routes
    openset::globals::mapper->deserializeRoutes(request.xPath("/routes"));

    // set number of partitions
    globals::async->setPartitionMax(partitionMax);
    // set them running - this return right away
    globals::async->startAsync();

    // set the partition map
    openset::globals::mapper->getPartitionMap()->deserializePartitionMap(request.xPath("/cluster"));
    globals::async->mapPartitionsToAsyncWorkers();

    globals::async->suspendAsync();
    // create the tables
    auto nodes = request.xPath("/tables")->getNodes();
    for (auto n : nodes)
    {
        auto tableName = n->xPathString("/name", "");

        if (!tableName.length())
            continue;

        auto table = openset::globals::database->newTable(tableName);

        table->deserializeTable(n->xPath("/table"));
        table->deserializeTriggers(n->xPath("/triggers"));
    }

    openset::globals::async->resumeAsync();

    Logger::get().info(globals::running->nodeName + " configured for " + to_string(partitionMax) + " partitions.");

    cjson response;
    response.set("configured", true);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::add_node(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto requestJson = message->getJSON();

    const auto nodeName = requestJson.xPathString("/node_name", "");
    const auto nodeId = requestJson.xPathInt("/node_id", 0);
    auto host = requestJson.xPathString("/host", "");
    const auto port = requestJson.xPathInt("/port", 0);

    if (host.length() && port && nodeId)
    {
        openset::globals::mapper->addRoute(nodeName, nodeId, host, port);
        Logger::get().info("added route " + globals::mapper->getRouteName(nodeId) + " @" + host + ":" + to_string(port) + ".");
    }
    else
    {
        Logger::get().error("change_cluster:node_add - missing params");
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "change_cluster:node_add missing params"
            },
            message);
        return;
    }

    cjson response;
    response.set("response", "thank you.");
    message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::transfer_init(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    const auto targetNode = message->getParamString("node");
    const auto partitionId = message->getParamInt("partition");

    std::vector<openset::db::Database::TablePtr> tables;

    { // get a list of tables
        csLock lock(globals::database->cs);
        for (const auto &t : globals::database->tables)
            tables.push_back(t.second);
    }

    Logger::get().info("transfer started for partition " + to_string(partitionId) + ".");

    globals::async->suspendAsync();

    for (const auto &t : tables)
    {
        auto part = t->getPartitionObjects(partitionId, false);

        if (part)
        {
            char* blockPtr;
            int64_t blockSize;

            {
                HeapStack mem;

                // we need to stick a header on this
                // the header needs the partition and the table name
                // the data belongs to.

                // grab 4 bytes and assign the partitionId
                *(recast<int32_t*>(mem.newPtr(sizeof(int32_t)))) = partitionId;

                // grab 4 bytes and set it the length of the table name
                const auto tableNameLength = recast<int32_t*>(mem.newPtr(sizeof(int32_t)));
                *tableNameLength = t->getName().length() + 1;

                // grab some bytes for the table name, and copy in the table name
                const auto name = mem.newPtr(*tableNameLength);
                strcpy(name, t->getName().c_str());

                // serialize the attributes
                part->attributes.serialize(&mem);

                // serialize the people
                part->people.serialize(&mem);

                blockPtr = mem.flatten();
                blockSize = mem.getBytes();
            } // HeapStack mem gets release here

            const auto targetNodeId = globals::mapper->getRouteId(targetNode);

            // TODO - test for target

            const auto responseMessage = openset::globals::mapper->dispatchSync(
                targetNodeId,
                "POST",
                "/v1/internode/transfer",
                {},
                blockPtr,
                blockSize);

            PoolMem::getPool().freePtr(blockPtr);

            if (!responseMessage)
                Logger::get().error("partition transfer error " + t->getName() + ".");
            else
                Logger::get().info(
                    "transferred for table " + t->getName() + " to " + openset::globals::mapper->getRouteName(partitionId) +
                    " (transfered " + to_string(blockSize) + " bytes).");
        }
    }

    globals::async->resumeAsync();

    Logger::get().info("transfer complete on partition " + to_string(partitionId) + ".");

    cjson response;
    response.set("response", "thank you.");
    message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::transfer_receive(const openset::web::MessagePtr& message, const RpcMapping& matches)
{

    // This is a binary message, it will contain an inbound table for a given partition.
    // in the header will be the partition, and table name. 

    Logger::get().info("transfer in (received " + to_string(message->getPayloadLength()) + " bytes).");

    auto read = message->getPayload();

    const auto partitionId = *recast<int32_t*>(read);
    read += 4;

    const auto tableNameLength = *recast<int32_t*>(read);
    read += 4;
    const std::string tableName(read);

    read += tableNameLength;

    openset::globals::async->suspendAsync();

    auto table = globals::database->getTable(tableName);

    // TODO - skipping this might be correct, and return false
    if (!table)
        table = globals::database->newTable(tableName);

    // make table partition objects
    auto parts = table->getPartitionObjects(partitionId, true);
    // make async partition object (loop, etc).
    openset::globals::async->initPartition(partitionId);

    read += parts->attributes.deserialize(read);
    read += parts->people.deserialize(read);

    openset::globals::async->resumeAsync();

    Logger::get().info("transfer comlete");

    // reply when done
    cjson response;
    response.set("transferred", true);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::transfer_translog(openset::web::MessagePtr& message, const RpcMapping& matches)
{
    Logger::get().info("translog transfer in (received " + to_string(message->getPayloadLength()) + " bytes).");

    const auto read = message->getPayload();

    SideLog::getSideLog().deserialize(read);

    Logger::get().info("transfer comlete");

    // reply when done
    cjson response;
    response.set("transferred", true);
    message->reply(http::StatusCode::success_ok, response);   
}

void RpcInternode::map_change(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    // These callbacks allow us to clean objects up when the map is altered.
    // The map doesn't have knowledge of these objects (and shouldn't) and these
    // objects are not in that scope so this is a nice tidy way to do this

    const auto addPartition = [&](int partitionId)
    {
        // add this partition to the async pool, it will add it to a loop
        globals::async->initPartition(partitionId);
        globals::async->assertAsyncLock(); // dbg - assert we are in a lock

        for (auto t : globals::database->tables)
        {
            const auto table = t.second.get();
            table->getPartitionObjects(partitionId, true);
            db::SideLog::getSideLog().resetReadHead(table, partitionId);
        }        
    };

    const auto removePartition = [&](int partitionId)
    {
        // drop this partition from the async engine
        globals::async->freePartition(partitionId);
        db::SideLog::getSideLog().removeReadHeadsByPartition(partitionId);

        globals::async->assertAsyncLock();

        // drop this partition from any table objects
        for (auto t : globals::database->tables)
            t.second->releasePartitionObjects(partitionId);
    };

    const auto removeRoute = [&](int64_t nodeId)
    {
        Logger::get().info("removing route via mapping change");
        globals::mapper->removeRoute(nodeId);
    };

    const auto addRoute = [&](std::string name, int64_t nodeId, std::string host, int port)
    {
        Logger::get().info("adding route '" + name + "' via mapping change");
        globals::mapper->addRoute(name, nodeId, host, port);
    };


    globals::async->suspendAsync();
    globals::async->assertAsyncLock();

    const auto requestJson = message->getJSON();

    globals::sentinel->setMapChanged();

    // map changes require the full undivided attention of the cluster!
    // nothing executing, means no goofy locks and no bad pointers
    openset::globals::mapper->changeMapping(
        requestJson,
        addPartition,
        removePartition,
        addRoute,
        removeRoute);

    openset::globals::async->balancePartitions();

    globals::async->resumeAsync();

    cjson response;
    response.set("response", "thank you.");
    message->reply(http::StatusCode::success_ok, response);
}
