#include "rpc_global.h"
#include "rpc_table.h"

#include <cinttypes>
#include <regex>

#include "common.h"

#include "cjson/cjson.h"
#include "oloop_insert.h"
#include "oloop_column.h"
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

void RpcTable::table_create(const openset::web::MessagePtr message, const RpcMapping& matches)
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

    const auto sourceColumns = request.xPath("/columns");
    if (!sourceColumns)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "column definition required, missing /columns" },
                message);

        return;
    }

    const auto sourceZOrder = request.xPath("/z_order");

    auto sourceColumnsList = sourceColumns->getNodes();

    // validate column names and types
    for (auto n : sourceColumnsList)
    {
        const auto name = n->xPathString("/name", "");
        const auto type = n->xPathString("/type", "");

        if (!name.size() || !type.size())
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "missing column type or name" },
                    message);
            return;
        }

        // bad type
        if (openset::db::ColumnTypes.find(type) == openset::db::ColumnTypes.end())
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "bad column type: must be int|double|text|bool" },
                    message);
            return;
        }

        if (!openset::db::Columns::validColumnName(name))
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "bad column name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                    message);
            return;
        }

    }


    globals::async->suspendAsync();
    auto table = database->newTable(tableName);
    auto columns = table->getColumns();

    // set the default required columns
    columns->setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false);
    columns->setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false);
    columns->setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false);
    columns->setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
    columns->setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
    columns->setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
    columns->setColumn(COL_SESSION, "__session", columnTypes_e::intColumn, false);

    int64_t columnEnum = 1000;

    for (auto n : sourceColumnsList)
    {
        const auto name = n->xPathString("/name", "");
        const auto type = n->xPathString("/type", "");
        const auto isSet = n->xPathBool("/is_set", false);

        columnTypes_e colType;

        if (type == "text")
            colType = columnTypes_e::textColumn;
        else if (type == "int")
            colType = columnTypes_e::intColumn;
        else if (type == "double")
            colType = columnTypes_e::doubleColumn;
        else if (type == "bool")
            colType = columnTypes_e::boolColumn;
        else
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::general_config_error,
                    "invalide column type" },
                    message);

            return;
        }

        columns->setColumn(columnEnum, name, colType, isSet);
        ++columnEnum;
    }

    if (sourceZOrder)
    {
        auto zOrderStrings = table->getZOrderStrings();
        auto zOrderHashes = table->getZOrderHashes();

        auto sourceZStrings = sourceZOrder->getNodes();

        auto idx = 0;
        for (auto n : sourceZStrings)
        {
            zOrderStrings->emplace(n->getString(), idx);
            zOrderHashes->emplace(MakeHash(n->getString()), idx);
            ++idx;
        }
    }

    globals::async->resumeAsync();

    Logger::get().info("table '" + tableName + "' created.");

    cjson response;
    response.set("message", "created");
    response.set("table", tableName);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::table_describe(const openset::web::MessagePtr message, const RpcMapping& matches)
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

    cjson response;

    response.set("table", tableName);

    auto columnNodes = response.setArray("columns");
    auto columns = table->getColumns();

    for (auto &c : columns->columns)
        if (c.idx > 6 && c.deleted == 0 && c.name.size() && c.type != columnTypes_e::freeColumn)
        {
            auto columnRecord = columnNodes->pushObject();

            std::string type;

            switch (c.type)
            {
            case columnTypes_e::intColumn:
                type = "int";
                break;
            case columnTypes_e::doubleColumn:
                type = "double";
                break;
            case columnTypes_e::boolColumn:
                type = "bool";
                break;
            case columnTypes_e::textColumn:
                type = "text";
                break;
            default:
                continue;
            }

            columnRecord->set("name", c.name);
            columnRecord->set("type", type);
            if (c.isSet)
                columnRecord->set("is_set", c.isSet);
        }

    Logger::get().info("describe table '" + tableName + "'.");
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_add(const openset::web::MessagePtr message, const RpcMapping& matches)
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
                "missing or invalid column name" },
                message);
        return;
    }

    if (!openset::db::Columns::validColumnName(columnName))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    if (openset::db::ColumnTypes.find(columnType) == openset::db::ColumnTypes.end())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column type: must be int|double|text|bool" },
                message);
        return;
    }

    auto columns = table->getColumns();

    int64_t lowest = 999;
    for (auto &c : columns->nameMap)
    {
        if (c.second->idx > lowest)
            lowest = c.second->idx;
    }

    ++lowest;

    columnTypes_e colType;

    if (columnType == "text")
        colType = columnTypes_e::textColumn;
    else if (columnType == "int")
        colType = columnTypes_e::intColumn;
    else if (columnType == "double")
        colType = columnTypes_e::doubleColumn;
    else if (columnType == "bool")
        colType = columnTypes_e::boolColumn;
    else
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "missing or invalid column type" },
                message);

        return; // TODO hmmm...
    }

    columns->setColumn(lowest, columnName, colType, isSet);

    Logger::get().info("added column '" + columnName + "' from table '" + tableName + "' created.");

    cjson response;
    response.set("message", "added");
    response.set("table", tableName);
    response.set("column", columnName);
    response.set("type", columnType);
    message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_drop(const openset::web::MessagePtr message, const RpcMapping& matches)
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
                "invalid column name" },
                message);
        return;
    }

    const auto column = table->getColumns()->getColumn(columnName);

    if (!column || column->type == columnTypes_e::freeColumn)
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "column not found" },
                message);
        return;
    }

    // delete the actual column
    table->getColumns()->deleteColumn(column);

    Logger::get().info("dropped column '" + columnName + "' from table '" + tableName + "' created.");

    cjson response;
    response.set("message", "dropped");
    response.set("table", tableName);
    response.set("column", columnName);
    message->reply(http::StatusCode::success_ok, response);
}
