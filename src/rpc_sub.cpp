#include "rpc_sub.h"

#include <cinttypes>
#include <regex>

#include "common.h"

#include "rpc.h"
#include "cjson/cjson.h"
#include "oloop_insert.h"

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

/*
void RpcSub::revent_create(const openset::web::MessagePtr message, const RpcMapping& matches)
{
    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto reventName = matches.find("name"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "missing /params/table" },
                message);

        return;
    }

    auto table = database->getTable(tableName);

    if (!table)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "table not found" },
                message);
        return;
    }

    if (!reventName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad re-event name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    { // scope for lock, saveConfig also locks, we don't nest locks

        csLock lock(globals::running->cs);

        // lets do some checking, are we making a new trigger
        // or updating an old one
        auto triggers = table->getTriggerConf();

        // does this trigger exist? If so this is an update!
        if (triggers->count(reventName))
        {
            auto t = triggers->at(reventName);
            t->script = std::string{ message->getPayload(), message->getPayloadLength() };

            // recompile script
            auto err = openset::revent::Revent::compileTriggers(
                table.get(),
                t->script,
                t->macros);

            table->forceReload();; // this will force a reload
        }
        else // it's new trigger
        {
            auto t = new openset::revent::reventSettings_s;

            t->name = reventName;
            t->id = MakeHash(t->name);
            t->script = std::string{ message->getPayload(), message->getPayloadLength() };;
            t->entryFunction = "on_insert"; // these may be configurable at some point
            t->entryFunctionHash = MakeHash(t->entryFunction);
            t->configVersion = 0;

            auto err = openset::revent::Revent::compileTriggers(
                table.get(),
                t->script,
                t->macros);

            if (err.inError())
            {
                RpcError(err, message);
                return;
            }

            triggers->insert(std::make_pair(reventName, t));
            table->forceReload(); // this updates the load version

            // note: async workers that are executing triggers will check the load version
            // and determine if they need to reload  triggers.
        }
    }

    Logger::get().info("set trigger '" + reventName + "' on table '" + tableName + "'.");

    cjson response;
    response.set("message", "created");
    response.set("table", tableName);
    response.set("reevent", reventName);
    message->reply(http::StatusCode::success_ok, response);
}
*/

/*
void RpcSub::revent_describe(const openset::web::MessagePtr message, const RpcMapping& matches)
{
    csLock lock(globals::running->cs);

    auto tableName = request->xPathString("/params/table", "");

    if (!tableName.size())
    {
    error(
    openset::errors::Error{
    openset::errors::errorClass_e::config,
    openset::errors::errorCode_e::general_config_error,
    "missing /params/table" },
    response);

    return;
    }

    auto table = database->getTable(tableName);

    if (!table)
    {
    error(
    openset::errors::Error{
    openset::errors::errorClass_e::config,
    openset::errors::errorCode_e::general_config_error,
    "table not found" },
    response);
    return;
    }

    const auto triggers = table->getTriggerConf();

    auto triggerNode = response->setArray("triggers");

    for (auto &t: *triggers)
    {
    auto trigNode = triggerNode->pushObject();
    trigNode->set("name", t.second->name);
    trigNode->set("entry", t.second->entryFunction);
    }

    const auto logLine = "describe triggers on table '" + tableName + "'.";
    Logger::get().info(logLine);
    response->set("message", logLine);
}
*/

void RpcSub::sub_delete(const openset::web::MessagePtr message, const RpcMapping& matches)
{
    csLock lock(globals::running->cs);

    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto segmentName = matches.find("segment"s)->second;
    const auto subName = matches.find("subname"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/{table_name}/.../..." },
            message);
        return;
    }

    if (!segmentName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/.../{segment_name}/..." },
            message);
        return;
    }

    if (!subName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/.../.../{subscriber_name}" },
            message);
        return;
    }

    auto table = database->getTable(tableName);

    if (!table)
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "table not found" },
            message);
        return;
    }

    if (!table->getMessages()->removeSubscriber(segmentName, subName))
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "subscriber name not found: '" + subName + "'" },
            message);
        return;
    }

    cjson response;
    response.set("message", "removed");
    response.set("table", tableName);
    response.set("segment", segmentName);
    response.set("subname", subName);
    message->reply(http::StatusCode::success_ok, response);
}


void RpcSub::sub_create(openset::web::MessagePtr message, const RpcMapping& matches)
{
    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto segmentName = matches.find("segment"s)->second;
    const auto subName = matches.find("subname"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/{table_name}/.../..." },
            message);
        return;
    }

    if (!segmentName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/.../{segment_name}/..." },
            message);
        return;
    }

    if (!subName.size())
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "missing /v1/trigger/.../.../{subscriber_name}" },
            message);
        return;
    }

    const auto table = database->getTable(tableName);

    if (!table)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "table not found" },
                message);
        return;
    }

    if (!segmentName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad triggerName name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    if (!subName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad subscriber name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    const auto config = message->getJSON();
    const auto retention = config.xPathInt("/retention", 10'800'000);
    const auto host = config.xPathString("/host", "");
    const auto port = config.xPathInt("/port", 80);
    const auto path = config.xPathString("/path", "/");

    if (!host.size() || !path.size() || !port)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "host is required (path and port are optional and default to / and 80 respectively." },
                message);
        return;
    }

    auto testAndCreate = [message, retention, host, port, path, table, tableName, segmentName, subName]()
    {
        const auto hostPort = host + ":" + to_string(port);
        auto rest = std::make_shared<openset::web::Rest>(0, hostPort);

        auto done_cb = [message, retention, host, port, path, table, tableName, segmentName, subName](
            const http::StatusCode status, const bool error, char* data, const size_t size)
        {
            if (status != http::StatusCode::success_ok || error)
            {
                RpcError(
                    openset::errors::Error{
                        openset::errors::errorClass_e::config,
                        openset::errors::errorCode_e::general_config_error,
                        "Expecting 2xx response from http://" + host + ":" + to_string(port) + path + "." },
                        message);
                return;
            }

            // lock the server up for a moment to avoid any bad behavior
            csLock lock(globals::running->cs);

            // lets do some checking, are we making a new trigger
            // or updating an old one
            const auto segments = table->getSegmentRefresh();

            // does this trigger exist? If not report the error
            if (!segments->count(segmentName))
            {
                RpcError(
                    openset::errors::Error{
                        openset::errors::errorClass_e::config,
                        openset::errors::errorCode_e::general_config_error,
                        "segment: '" + segmentName + "' not found." },
                        message);
                return;
            }

            // this will make or update our subscriber
            table->getMessages()->registerSubscriber(segmentName, subName, host, port, path, retention);

            cjson response;
            response.set("message", "created");
            response.set("table", tableName);
            response.set("segment", segmentName);
            response.set("subname", subName);
            message->reply(http::StatusCode::success_ok, response);
        };

        // try our rest endpoint, pass empty data, see if it's good-to-go in done_cb
        auto payload = R"s({"events": []})s"s;
        rest->request("POST", path, {}, &payload[0], payload.length(), done_cb);
    };

    // spin off the testing to a thread, because it can be slow
    std::thread tc(testAndCreate);
    tc.detach();
}
