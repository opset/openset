#include <cinttypes>
#include <regex>
#include <thread>
#include <random>

#include "common.h"
#include "rpc_global.h"
#include "rpc_insert.h"

#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"

#include "asyncpool.h"
#include "sentinel.h"
#include "sidelog.h"
#include "database.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "http_serve.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

void RpcInsert::insertRetry(const openset::web::MessagePtr& message, const RpcMapping& matches, const int retryCount)
{
    const auto database = openset::globals::database;
    const auto partitions = openset::globals::async;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto isFork = message->getParamBool("fork");

    /*     
    const auto relayString = message->getParamString("relay");
    const auto relayParts = split(relayString, ':');

    std::unordered_set<int64_t> alreadyRelayed;
    alreadyRelayed.insert(openset::globals::running->nodeId);

    for (const auto &relay : relayParts)
    {        
        alreadyRelayed.insert(stoll(relay));
    }
      
    if (!partitions->getPartitionMax())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::insert,
                openset::errors::errorCode_e::route_error,
                "node not initialized" },
                message);
        return;
    }
    */

    auto table = database->getTable(tableName);

    if (!table || table->deleted)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::insert,
                openset::errors::errorCode_e::general_error,
                "missing or invalid table name" },
                message);
        return;
    }

    //auto clusterErrors = false;
    const auto startTime = Now();

    // a cluster error (missing partition, etc), or a map changed happened
    // during this insert, then re-insert
    if (openset::globals::sentinel->wasDuringMapChange(startTime - 500, startTime))
    {
        const auto backOff = (retryCount * retryCount) * 20;
        ThreadSleep(backOff < 10000 ? backOff : 10000);

        insertRetry(message, matches, retryCount + 1);
        return;
    }

    auto rows = request.getNodes();
    Logger::get().info("Inserting " + to_string(rows.size()) + " events.");

    // vectors go gather locally inserted, or remotely distributed events from this set
    //std::unordered_map<int, std::vector<char*>> localGather;
    //std::unordered_map<int64_t, std::vector<char*>> remoteGather;

    SideLog::getSideLog().lock();

    for (auto row : rows)
    {
        const auto personNode = row->xPath("/id");
        if (!personNode || 
            (personNode->type() != cjson::Types_e::INT && personNode->type() != cjson::Types_e::STR))
            continue;

        // straight up numeric ID nodes don't need hashing, actually hashing would be very bad.
        // We can use numeric IDs (i.e. a customer id) directly.
        int64_t uuid = 0;

        if (personNode->type() == cjson::Types_e::STR)
        {
            auto uuString = personNode->getString();
            toLower(uuString);

            if (uuString.length())
                uuid = MakeHash(uuString);
        }
        else
            uuid = personNode->getInt();

        const auto destination = cast<int32_t>((std::abs(uuid) % 13337) % partitions->getPartitionMax());

        int64_t len;
        SideLog::getSideLog().add(table.get(), destination, cjson::stringifyCstr(row, len));
    }

    SideLog::getSideLog().unlock();

    const auto localEndTime = Now();
    
    if (!isFork && openset::globals::mapper->countActiveRoutes() > 1)
    {
        if (openset::globals::sentinel->wasDuringMapChange(startTime, localEndTime))
            ThreadSleep(1000);

        const auto method = message->getMethod();
        const auto path = message->getPath();
        auto newParams = message->getQuery();
        newParams.emplace("fork", "true");
        const auto payloadLength = message->getPayloadLength();
        const auto payload = static_cast<char*>(PoolMem::getPool().getPtr(payloadLength));
        memcpy(payload, message->getPayload(), payloadLength);       

        std::thread t([=](){

            while (true)
            {
                auto result = openset::globals::mapper->dispatchCluster(
                    method,
                    path,
                    newParams,
                    payload,
                    payloadLength,
                    false);

                const auto isGood = !result.routeError;

                openset::globals::mapper->releaseResponses(result);

                if (isGood)
                    break;
            }

            PoolMem::getPool().freePtr(payload);
        });

        t.detach();
    }

    cjson response;
    response.set("message", "yummy");

    // broadcast active nodes to caller - they may round-robin to these
    auto routesList = response.setArray("routes");
    {
        csLock lock(openset::globals::mapper->cs);
    	auto routes = openset::globals::mapper->routes;
        for (const auto &r : routes) {
            if (r.first == globals::running->nodeId) // fix for broadcast bug shouting local host and port
                routesList->push(globals::running->hostExternal + ":" + to_string(globals::running->portExternal));
            else
                routesList->push(r.second.first + ":" + to_string(r.second.second));
        }
    }

    message->reply(http::StatusCode::success_ok, response);      
}

void RpcInsert::insert(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    insertRetry(message, matches, 1);
}
