#include "rpc_global.h"
#include "rpc_table.h"

#include <cinttypes>
#include <regex>

#include "common.h"

#include "cjson/cjson.h"
#include "oloop_insert.h"
#include "oloop_property.h"
#include "oloop_histogram.h"

#include "asyncpool.h"
#include "database.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "http_serve.h"

//#include "trigger.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

void RpcTable::table_create(const openset::web::MessagePtr& message, const RpcMapping& matches)
{

    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;
    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad table name" },
                message);
        return;
    }

    if (database->getTable(tableName))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "table already exists" },
                message);
        return;
    }

    // TODO - look for spaces, check length, look for symbols that aren't - or _ etc.

    const auto sourceProps = request.xPath("/properties");
    if (!sourceProps)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "properties definition required, missing /properties" },
                message);

        return;
    }

    const auto sourceEventOrder = request.xPath("/event_order");
    const auto sourceSettings = request.xPath("/settings");

    auto sourcePropsList = sourceProps->getNodes();

    // validate property names and types
    for (auto n : sourcePropsList)
    {
        const auto name = n->xPathString("/name", "");
        const auto type = n->xPathString("/type", "");

        if (!name.size() || !type.size())
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "missing properties type or name" },
                    message);
            return;
        }

        // bad type
        if (openset::db::PropertyTypes.find(type) == openset::db::PropertyTypes.end())
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "bad properties type: must be int|double|text|bool" },
                    message);
            return;
        }

        if (!openset::db::Properties::validPropertyName(name))
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "bad properties name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                    message);
            return;
        }

    }


    globals::async->suspendAsync();
    auto table = database->newTable(tableName);
    auto columns = table->getProperties();

    // lock the table object
    csLock lock(*table->getLock());

    // set the default required properties
    columns->setProperty(PROP_STAMP, "stamp", PropertyTypes_e::intProp, false);
    columns->setProperty(PROP_EVENT, "event", PropertyTypes_e::textProp, false);
    columns->setProperty(PROP_UUID, "id", PropertyTypes_e::intProp, false);
    columns->setProperty(PROP_SEGMENT, "__segment", PropertyTypes_e::textProp, false);
    columns->setProperty(PROP_SESSION, "session", PropertyTypes_e::intProp, false);

    int64_t columnEnum = 1000;

    for (auto n : sourcePropsList)
    {
        const auto name = n->xPathString("/name", "");
        const auto type = n->xPathString("/type", "");
        const auto isSet = n->xPathBool("/is_set", false);
        const auto isProp = n->xPathBool("/is_customer", false);

        PropertyTypes_e colType;

        if (type == "text")
            colType = PropertyTypes_e::textProp;
        else if (type == "int")
            colType = PropertyTypes_e::intProp;
        else if (type == "double")
            colType = PropertyTypes_e::doubleProp;
        else if (type == "bool")
            colType = PropertyTypes_e::boolProp;
        else
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "invalid property type" },
                    message);

            return;
        }

        columns->setProperty(columnEnum, name, colType, isSet, isProp);
        ++columnEnum;
    }

    if (sourceEventOrder)
    {
        auto eventOrderStrings = table->getEventOrderStrings();
        auto eventOrderHashes = table->getEventOrderHashes();

        auto sourceEventOrderStrings = sourceEventOrder->getNodes();

        auto idx = 0;
        for (auto n : sourceEventOrderStrings)
        {
            eventOrderStrings->emplace(n->getString(), idx);
            eventOrderHashes->emplace(MakeHash(n->getString()), idx);
            ++idx;
        }
    }

    if (sourceSettings)
    {
        table->deserializeSettings(sourceSettings);
    }

    globals::async->resumeAsync();

    Logger::get().info("table '" + tableName + "' created.");

    cjson response;
    response.set("message", "created");
    response.set("table", tableName);
    message->reply(http::StatusCode::success_ok, response);
}

void openset::comms::RpcTable::table_drop(const openset::web::MessagePtr& message, const RpcMapping & matches)
{
    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;
    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad table name" },
                message);
        return;
    }

    if (!database->getTable(tableName))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "table not found" },
                message);
        return;
    }

    database->dropTable(tableName);

    cjson response;
    response.set("message", "dropped");
    response.set("table", tableName);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::table_describe(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;

    if (!tableName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "missing table name" },
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

    // lock the table object
    csLock lock(*table->getLock());

    cjson response;

    response.set("table", tableName);

    auto columnNodes = response.setArray("properties");
    auto columns = table->getProperties();

    for (auto &c : columns->properties)
        if (c.idx > 6 && c.deleted == 0 && c.name.size() && c.type != PropertyTypes_e::freeProp)
        {
            auto columnRecord = columnNodes->pushObject();

            std::string type;

            switch (c.type)
            {
            case PropertyTypes_e::intProp:
                type = "int";
                break;
            case PropertyTypes_e::doubleProp:
                type = "double";
                break;
            case PropertyTypes_e::boolProp:
                type = "bool";
                break;
            case PropertyTypes_e::textProp:
                type = "text";
                break;
            default:
                continue;
            }

            columnRecord->set("name", c.name);
            columnRecord->set("type", type);
            if (c.isSet)
                columnRecord->set("is_set", true);
            if (c.isCustomerProperty)
                columnRecord->set("is_customer", true);
        }

    auto eventOrder = response.setArray("event_order");
    const auto eventOrderMap = table->getEventOrderStrings();
    std::vector<std::string> eventOrderList(eventOrderMap->size());

    for (auto &m: *eventOrderMap)
       eventOrderList[m.second] = m.first;

    for (const auto &z : eventOrderList)
        eventOrder->push(z);

    const auto settings = response.setObject("settings");
    table->serializeSettings(settings);

    Logger::get().info("describe table '" + tableName + "'.");
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_add(const openset::web::MessagePtr& message, const RpcMapping& matches)
{

    // this request must be forwarded to all the other nodes
    if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
        return;

    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto columnName = matches.find("name"s)->second;
    const auto columnType = message->getParamString("type"s);
    const auto isSet = message->getParamBool("is_set"s);
    const auto isProp = message->getParamBool("is_customer"s);

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

    if (!columnName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "missing or invalid property name" },
                message);
        return;
    }

    if (!openset::db::Properties::validPropertyName(columnName))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad property name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    if (openset::db::PropertyTypes.find(columnType) == openset::db::PropertyTypes.end())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad property type: must be int|double|text|bool" },
                message);
        return;
    }

    // lock the table object
    csLock lock(*table->getLock());

    auto columns = table->getProperties();

    int64_t lowest = 999;
    for (auto &c : columns->nameMap)
    {
        if (c.second->idx > lowest)
            lowest = c.second->idx;
    }

    ++lowest;

    PropertyTypes_e colType;

    if (columnType == "text")
        colType = PropertyTypes_e::textProp;
    else if (columnType == "int")
        colType = PropertyTypes_e::intProp;
    else if (columnType == "double")
        colType = PropertyTypes_e::doubleProp;
    else
        colType = PropertyTypes_e::boolProp;

    columns->setProperty(lowest, columnName, colType, isSet, isProp);

    Logger::get().info("added property '" + columnName + "' to table '" + tableName + "' created.");

    cjson response;
    response.set("message", "added");
    response.set("table", tableName);
    response.set("property", columnName);
    response.set("type", columnType);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_drop(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto columnName = matches.find("name"s)->second;

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

    if (!columnName.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "invalid property name" },
                message);
        return;
    }

    // lock the table object
    csLock lock(*table->getLock());

    const auto column = table->getProperties()->getProperty(columnName);

    if (!column || column->type == PropertyTypes_e::freeProp)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "property not found" },
                message);
        return;
    }

    // delete the actual property
    table->getProperties()->deleteProperty(column);

    Logger::get().info("dropped property '" + columnName + "' from table '" + tableName + "' created.");

    cjson response;
    response.set("message", "dropped");
    response.set("table", tableName);
    response.set("property", columnName);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::table_settings(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database = openset::globals::database;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto columnName = matches.find("name"s)->second;

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

    // lock the table object
    csLock lock(*table->getLock());

    table->deserializeSettings(&request);

    cjson response;
    table->serializeSettings(&response);
    message->reply(http::StatusCode::success_ok, response);
}

void openset::comms::RpcTable::table_list(const openset::web::MessagePtr & message, const RpcMapping & matches)
{
    // lock the table object

    auto database = openset::globals::database;
    const auto names = database->getTableNames();

    cjson response(cjson::Types_e::ARRAY);

    for (auto &name: names)
        response.push(name);

    message->reply(http::StatusCode::success_ok, response);
}
