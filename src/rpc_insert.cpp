#include <cinttypes>
#include <regex>

#include "common.h"
#include "rpc_global.h"
#include "rpc_insert.h"

#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"
#include "oloop_column.h"

#include "asyncpool.h"
#include "config.h"
#include "sentinel.h"
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

void RpcInsert::insert(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    const auto database = openset::globals::database;
    const auto partitions = openset::globals::async;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto isFork = message->getParamBool("fork");

    if (!partitions->getPartitionMax())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::insert,
                openset::errors::errorCode_e::route_error,
                "node_not_initialized" },
                message);
        return;
    }

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

    auto rows = request.getNodes();

    Logger::get().info("Inserting " + to_string(rows.size()) + " events.");

    // vectors go gather locally inserted, or remotely distributed events from this set
    std::unordered_map<int, std::vector<char*>> localGather;
    std::unordered_map<int64_t, std::vector<char*>> remoteGather;

    const auto mapper = openset::globals::mapper->getPartitionMap();

    for (auto row : rows)
    {
        const auto personNode = row->xPath("/person");
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

        const auto mapInfo = globals::mapper->partitionMap.getState(destination, globals::running->nodeId);

        if (mapInfo == openset::mapping::NodeState_e::active_owner ||
            mapInfo == openset::mapping::NodeState_e::active_clone)
        {
            if (!localGather.count(destination))
                localGather.emplace(destination, vector<char*>{});

            int64_t len;
            localGather[destination].push_back(cjson::stringifyCstr(row, len));
        }

        if (!isFork)
        {
            const auto nodes = mapper->getNodesByPartitionId(destination);

            for (auto targetNode : nodes)
            {
                if (targetNode == openset::globals::running->nodeId)
                    continue;

                if (!remoteGather.count(targetNode))
                    remoteGather.emplace(targetNode, vector<char*>{});

                int64_t len;
                remoteGather[targetNode].push_back(cjson::stringifyCstr(row, len));
            }
        }

    }

    for (auto &g : localGather)
    {
        if (!g.second.size())
            continue;

        auto parts = table->getPartitionObjects(g.first);

        if (parts)
        {
            csLock lock(parts->insertCS); // lock once, then bulk queue

            parts->insertBacklog += g.second.size();
            parts->insertQueue.insert(
                parts->insertQueue.end(),
                std::make_move_iterator(g.second.begin()),
                std::make_move_iterator(g.second.end()));
        }
    }

    auto remoteCount = 0;

    const auto thankyouCb = [](http::StatusCode, bool, char*, size_t)
    {
        // TODO - we should probably handle this horrible possibility somehow.
        // delete message;
    };

    if (!isFork)
        for (auto &data : remoteGather)
        {
            ++remoteCount;

            const auto targetNode = data.first;
            const auto& events = data.second;

            // make an JSON array object
            cjson json(cjson::Types_e::ARRAY);

            for (auto e : events)
            {
                cjson::parse(e, json.pushObject(), true);
                PoolMem::getPool().freePtr(e);
            }

            auto jsonText = cjson::stringify(&json);

            auto newParams = message->getQuery();
            newParams.emplace("fork", "true");
            
            openset::globals::mapper->dispatchAsync(
                targetNode,
                "POST",
                "/v1/insert/" + tableName,
                newParams,
                jsonText.c_str(),
                jsonText.length(),
                thankyouCb);

        }

    // FLOW CONTROL - check for backlogging, delay the 
    // "thank you." response until backlog is acceptable
    for (auto &g : localGather)
    {
        const auto parts = table->getPartitionObjects(g.first);

        if (!parts)
            continue;

        auto sleepCount = 0;
        const auto sleepStart = Now();
        while (parts->insertBacklog > 7500)
        {
            ThreadSleep(10);
            ++sleepCount;
        }

        if (sleepCount)
            Logger::get().info("insert drain timer for " + to_string(Now() - sleepStart) + "ms on partition " +
                to_string(parts->partition) + ".");
    }

    cjson response;
    response.set("message", "yummy");
    message->reply(http::StatusCode::success_ok, response);
}
