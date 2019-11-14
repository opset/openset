#include <stdexcept>
#include <cinttypes>
#include <regex>
#include "rpc_global.h"
#include "rpc_query.h"
#include "common.h"
#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"
#include "oloop_query.h"
#include "oloop_segment.h"
#include "oloop_customer.h"
#include "oloop_customer_list.h"
#include "oloop_property.h"
#include "oloop_histogram.h"
#include "asyncpool.h"
#include "asyncloop.h"
#include "config.h"
#include "sentinel.h"
#include "querycommon.h"
#include "queryparserosl.h"
#include "database.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "names.h"
#include "http_serve.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

enum class queryFunction_e : int32_t
{
    none,
    status,
    query,
    count,
}; /*
* The magic FORK function.
*
* This will add a `is_fork: true` member to the request
* and forward the query to other nodes cluster.
*
* fork nodes will return binary result sets.
* the non-fork (or originating node) will call this function
* and wait for results to return, at which point it will merge them.
*
* Note: a single node could have any number of partitions, these partitions
* are merged into a single result by `is_fork` nodes before return the
* result set. This greatly reduces the number of data sets that need to be held
* in memory and marged by the originator.
*/
shared_ptr<cjson> forkQuery(
    const Database::TablePtr& table,
    const openset::web::MessagePtr& message,
    const int resultColumnCount,
    const int resultSetCount,
    const ResultSortMode_e sortMode   = ResultSortMode_e::column,
    const ResultSortOrder_e sortOrder = ResultSortOrder_e::Desc,
    const int sortColumn              = 0,
    const int trim                    = -1,
    const int64_t bucket              = 0,
    const int64_t forceMin            = std::numeric_limits<int64_t>::min(),
    const int64_t forceMax            = std::numeric_limits<int64_t>::max(),
    const int64_t retryCount          = 1)
{
    auto newParams = message->getQuery();
    newParams.emplace("fork", "true");
    const auto startTime = Now();

    // special case... if we ran this query during a map change, run it again (re-fork)
    if (openset::globals::sentinel->wasDuringMapChange(startTime - 1, startTime))
    {
        const auto backOff = (retryCount * retryCount) * 20;
        ThreadSleep(backOff < 10'000 ? backOff : 10'000);

        return forkQuery(
            table,
            message,
            resultColumnCount,
            resultSetCount,
            sortMode,
            sortOrder,
            sortColumn,
            trim,
            bucket,
            forceMin,
            forceMax,
            retryCount + 1);
    }

    const auto setCount = resultSetCount ? resultSetCount : 1;

    // call all nodes and gather results - JSON is what's coming back
    // NOTE - it would be fully possible to flatten results to binary
    auto result = openset::globals::mapper->dispatchCluster(
        message->getMethod(),
        message->getPath(),
        newParams,
        message->getPayload(),
        message->getPayloadLength(),
        true);

    const auto dispatchEndTime = Now();

    // special case... if we ran this query during a map change, run it again (re-fork)
    if (openset::globals::sentinel->wasDuringMapChange(startTime, dispatchEndTime))
    {
        const auto backOff = (retryCount * retryCount) * 20;
        ThreadSleep( backOff < 10000 ? backOff : 10000);
        return forkQuery(
            table,
            message,
            resultColumnCount,
            resultSetCount,
            sortMode,
            sortOrder,
            sortColumn,
            trim,
            bucket,
            forceMin,
            forceMax,
            retryCount + 1);
    }

    std::vector<ResultSet*> resultSets;
    for (auto& r : result.responses)
    {
        if (ResultMuxDemux::isInternode(r.data, r.length))
            resultSets.push_back(ResultMuxDemux::internodeToResultSet(r.data, r.length));
        else
        {
            // there is an error message from one of the participing nodes
            if (!r.data || !r.length)
            {
                result.routeError = true;
            }
            else if (r.code != openset::http::StatusCode::success_ok)
            {
                // try to capture a json error that has peculated up from the forked call.
                if (r.data && r.length && r.data[0] == '{')
                {
                    cjson error(std::string(r.data, r.length), cjson::Mode_e::string);
                    if (error.xPath("/error"))
                    {
                        message->reply(openset::http::StatusCode::client_error_bad_request, error);
                        // free up the responses
                        openset::globals::mapper->releaseResponses(result);

                        // clean up all those resultSet*
                        for (auto res : resultSets)
                            delete res;
                        return nullptr;
                    }
                    result.routeError = true;
                }
                else
                    result.routeError = true; // this will trigger the next error
            }
        }

        if (result.routeError)
        {
            RpcError(
                openset::errors::Error {
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::route_error,
                    "potential node failure - please re-issue the request"
                },
                message);

            // free up the responses
            openset::globals::mapper->releaseResponses(result);
            // clean up all those resultSet*
            for (auto res : resultSets)
                delete res;
            return nullptr;
        }
    }
    auto resultJson = make_shared<cjson>();
    ResultMuxDemux::resultSetToJson(resultColumnCount, setCount, resultSets, resultJson.get());

    // free up the responses
    openset::globals::mapper->releaseResponses(result);
    // clean up all those resultSet*
    for (auto res : resultSets)
        delete res;

    if (bucket)
        ResultMuxDemux::jsonResultHistogramFill(resultJson.get(), bucket, forceMin, forceMax);

    switch (sortMode)
    {
    case ResultSortMode_e::key:
        ResultMuxDemux::jsonResultSortByGroup(resultJson.get(), sortOrder);
        break;
    case ResultSortMode_e::column:
        ResultMuxDemux::jsonResultSortByColumn(resultJson.get(), sortOrder, sortColumn);
        break;
    default: ;
    }

    ResultMuxDemux::jsonResultTrim(resultJson.get(), trim); // local function to fill Meta data in result JSON
    const auto fillMeta = [](const openset::query::VarList& mapping, cjson* jsonArray)
    {
        for (auto c : mapping)
        {
            auto tNode = jsonArray->pushObject();
            if (c.modifier == openset::query::Modifiers_e::var)
            {
                tNode->set("mode", "var");
                tNode->set("name", c.alias);
                switch (c.value.typeOf())
                {
                case cvar::valueType::INT32: case cvar::valueType::INT64:
                    tNode->set("type", "int");
                    break;
                case cvar::valueType::FLT: case cvar::valueType::DBL:
                    tNode->set("type", "double");
                    break;
                case cvar::valueType::STR:
                    tNode->set("type", "text");
                    break;
                case cvar::valueType::BOOL:
                    tNode->set("type", "bool");
                    break;
                default:
                    tNode->set("type", "na");
                    break;
                }
            }
            else if (openset::query::isTimeModifiers.count(c.modifier))
            {
                auto mode = openset::query::ModifierDebugStrings.at(c.modifier);
                toLower(mode);
                tNode->set("mode", mode);
                tNode->set("name", c.alias);
                tNode->set("type", "int");
            }
            else
            {
                auto mode = openset::query::ModifierDebugStrings.at(c.modifier);
                toLower(mode);
                tNode->set("mode", mode);
                tNode->set("name", c.alias);
                tNode->set("property", c.actual);
                switch (c.schemaType)
                {
                case PropertyTypes_e::freeProp:
                    tNode->set("type", "na");
                    break;
                case PropertyTypes_e::intProp:
                    tNode->set("type", "int");
                    break;
                case PropertyTypes_e::doubleProp:
                    tNode->set("type", "double");
                    break;
                case PropertyTypes_e::boolProp:
                    tNode->set("type", "bool");
                    break;
                case PropertyTypes_e::textProp:
                    tNode->set("type", "text");
                    break;
                default: ;
                }
            }
        }
    }; // add status nodes to JSON document

    //auto metaJson = resultJson->setObject("info");
    //auto dataJson = metaJson->setObject("data");
    //fillMeta(queryMacros.vars.columnVars, dataJson->setArray("properties"));
    //fillMeta(queryMacros.vars.groupVars, dataJson->setArray("groups"));
    //metaJson->set("query_time", queryTime);
    //metaJson->set("pop_evaluated", population);
    //metaJson->set("pop_total", totalPopulation);
    //metaJson->set("compile_time", compileTime);
    //metaJson->set("serialize_time", serialTime);
    //metaJson->set("total_time", elapsed);
    Logger::get().info("RpcQuery on " + table->getName());
    return resultJson;
}

openset::query::ParamVars getInlineVaraibles(const openset::web::MessagePtr& message)
{
    /*
    * Build a map of variable names and vars that will
    * become the new default value for variables defined
    * in a pyql script (under the params headings).
    *
    * These will be reset upon each run of the script
    * to return it's state back to original
    */
    openset::query::ParamVars paramVars;
    for (auto p : message->getQuery())
    {
        cvar value = p.second;
        if (p.first.find("str_") != string::npos)
        {
            auto name = trim(p.first.substr(4));
            if (name.length())
                paramVars[name] = value;
        }
        else if (p.first.find("int_") != string::npos)
        {
            auto name = trim(p.first.substr(4));
            if (name.length())
                paramVars[name] = value.getInt64();
        }
        else if (p.first.find("dbl_") != string::npos)
        {
            auto name = trim(p.first.substr(4));
            if (name.length())
                paramVars[name] = value.getDouble();
        }
        else if (p.first.find("bool_") != string::npos)
        {
            auto name = trim(p.first.substr(4));
            if (name.length())
                paramVars[name] = value.getBool();
        }
    }
    return paramVars;
}

void RpcQuery::report(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database             = globals::database;
    const auto partitions     = globals::async;
    const auto request        = message->getJSON();
    const auto tableName      = matches.find("table"s)->second;
    const auto queryCode      = std::string { message->getPayload(), message->getPayloadLength() };
    const auto debug          = message->getParamBool("debug");
    const auto isFork         = message->getParamBool("fork");
    const auto useStampCounts = message->getParamBool("stamp_counts");
    const auto trimSize       = message->getParamInt("trim", -1);
    const auto sortOrder      = message->getParamString("order", "desc") == "asc"
        ? ResultSortOrder_e::Asc
        : ResultSortOrder_e::Desc;

    auto sortColumnName = ""s;
    auto sortMode       = ResultSortMode_e::column;
    if (message->isParam("sort"))
    {
        sortColumnName = message->getParamString("sort");
        if (sortColumnName == "group")
            sortMode = ResultSortMode_e::key;
    }

    const auto log = "Inbound events query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
    Logger::get().info(log);

    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    if (!queryCode.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing query code (POST query as text)"
            },
            message);
        return;
    }

    auto table = database->getTable(tableName);
    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    // override session time if provided, otherwise use table default
    const auto sessionTime     = message->getParamInt("session_time", table->getSessionTime());
    query::ParamVars paramVars = getInlineVaraibles(message);
    query::Macro_s queryMacros; // this is our compiled code block
    query::QueryParser p;
    try
    {
        p.compileQuery(queryCode.c_str(), table->getProperties(), queryMacros, &paramVars);
        queryMacros.useStampedRowIds = useStampCounts;
    }
    catch (const std::runtime_error& ex)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                std::string { ex.what() }
            },
            message);
        return;
    }

    if (p.error.inError())
    {
        Logger::get().error(p.error.getErrorJSON());
        message->reply(http::StatusCode::client_error_bad_request, p.error.getErrorJSON());
        return;
    }

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts             = split(segmentText, ',');
        queryMacros.segments.clear();
        for (const auto& part : parts)
        {
            const auto trimmedPart = trim(part);
            if (trimmedPart.length())
                queryMacros.segments.push_back(trimmedPart);
        }
        if (!queryMacros.segments.size())
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "no segment names specified"
                },
                message);
            return;
        }
    }

    // set the sessionTime (timeout) value, this will get relayed
    // through the to oloop_query, the customer object and finally the grid
    queryMacros.sessionTime = sessionTime;
    if (debug)
    {
        auto debugOutput = MacroDbg(queryMacros); // reply as text
        message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
        return;
    }
    auto sortColumn = 0;
    if (sortMode != ResultSortMode_e::key && sortColumnName.size())
    {
        auto set = false;
        auto idx = -1;
        for (auto& c : queryMacros.vars.columnVars)
        {
            ++idx;
            if (c.alias == sortColumnName)
            {
                set        = true;
                sortColumn = c.index;
                break;
            }
        }
        if (!set)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "sort property not found in query aggregates"
                },
                message);
            return;
        }
    }

    /*
    * We are originating the query.
    *
    * At this point in the function we have validated that the
    * script compiles, maps to the schema, is on a valid table,
    * etc.
    *
    * We will call our forkQuery function.
    *
    * forQuery will call all the nodes (including this one) with the
    * `is_fork` variable set to true.
    */
    if (!isFork)
    {
        const auto json = forkQuery(
            table,
            message,
            queryMacros.vars.columnVars.size(),
            queryMacros.segments.size(),
            sortMode,
            sortOrder,
            sortColumn,
            trimSize);
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }

    // We are a Fork!

    // create list of active_owner partitions for factory function
    auto activeList = globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
        globals::running->nodeId,
        {
            mapping::NodeState_e::active_owner
        });

    // Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
    //      we don't have to worry about locking anything shared between partitions in the same
    //      thread as they are executed serially, rather than in parallel.
    //
    //      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
    //      as well as generally reduce the number of ResultSets needed (especially when partition
    //      counts are high).
    //
    //      Note: These are heap objects because we lose scope, as this function
    //            exits before the result objects are used.
    //
    std::vector<ResultSet*> resultSets;
    resultSets.reserve(partitions->getWorkerCount());
    for (auto i = 0; i < partitions->getWorkerCount(); ++i)
        resultSets.push_back(
            new ResultSet(
                queryMacros.vars.columnVars.size() * (queryMacros.segments.size()
                  ? queryMacros.segments.size()
                  : 1)));

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer    = ResultMuxDemux::multiSetToInternode(
            queryMacros.vars.columnVars.size(),
            queryMacros.segments.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);
        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        Logger::get().info("event query on " + table->getName());
        for (auto resultSet : resultSets)
            delete resultSet;
        return;
    }

    /*
    *  this Shuttle will gather our result sets roll them up and spit them back
    *
    *  note that queryMacros are captured with a copy, this is because a reference
    *  version will have had it's destructor called when the function exits.
    *
    *  Note: ShuttleLamda comes in two versions,
    */
    const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [queryMacros, table, resultSets](
        vector<response_s<CellQueryResult_s>>& responses,
        web::MessagePtr message,
        voidfunc release_cb) mutable
        {
            // process the data and respond
            // check for errors, add up totals
            for (const auto& r : responses)
            {
                if (r.data.error.inError())
                {
                    // any error that is recorded should be considered a hard error, so report it
                    const auto errorMessage = r.data.error.getErrorJSON();
                    Logger::get().error(errorMessage);
                    message->reply(http::StatusCode::client_error_bad_request, errorMessage);
                    // clean up stray resultSets
                    for (auto resultSet : resultSets)
                        delete resultSet;

                    release_cb();
                    return;
                }
            }

            // 1. Merge the Macro Literals
            ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

            // 2. Merge the rows
            int64_t bufferLength = 0;
            const auto buffer    = ResultMuxDemux::multiSetToInternode(
                queryMacros.vars.columnVars.size(),
                queryMacros.segments.size(),
                //queryMacros.indexes.size(),
                resultSets,
                bufferLength);

            message->reply(http::StatusCode::success_ok, buffer, bufferLength);
            PoolMem::getPool().freePtr(buffer);

            Logger::get().info("event query on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;

            // this will delete the shuttle, and clear up the CellQueryResult_s vector
            release_cb();
        });

    auto instance = 0;

    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(
        activeList,
        [shuttle, table, queryMacros, resultSets, &instance](AsyncLoop* loop) -> OpenLoop*
        {
            instance++;
            return new OpenLoopQuery(shuttle, table, queryMacros, resultSets[loop->getWorkerId()], instance);
        });
}

void RpcQuery::customer_list(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database         = globals::database;
    const auto partitions = globals::async;
    const auto request    = message->getJSON();
    const auto tableName  = matches.find("table"s)->second;
    const auto queryCode  = std::string { message->getPayload(), message->getPayloadLength() };
    const auto debug      = message->getParamBool("debug");
    const auto isFork     = message->getParamBool("fork");
    const auto trimSize   = message->getParamInt("trim", -1);
    const auto sortMode = ResultSortMode_e::key;
    const auto sortOrder  = message->getParamString("order", "desc") == "asc"
        ? ResultSortOrder_e::Asc
        : ResultSortOrder_e::Desc;
    auto sortKeyString    = message->getParamString("sort", "");

    if (!sortKeyString.length())
        sortKeyString = "id";

    const auto log = "Inbound counts query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
    Logger::get().info(log);

    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    if (!queryCode.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing query code (POST query as text)"
            },
            message);
        return;
    }

    auto table = database->getTable(tableName);

    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    // override session time if provided, otherwise use table default
    const auto sessionTime     = message->getParamInt("session_time", table->getSessionTime());
    query::ParamVars paramVars = getInlineVaraibles(message);
    query::Macro_s queryMacros; // this is our compiled code block
    query::QueryParser p;

    try
    {
        // compile in customers mode
        p.compileQuery(queryCode.c_str(), table->getProperties(), queryMacros, &paramVars, query::ParseMode_e::customers);
    }
    catch (const std::runtime_error& ex)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                std::string { ex.what() }
            },
            message);
        return;
    }

    if (p.error.inError())
    {
        Logger::get().error(p.error.getErrorJSON());
        message->reply(http::StatusCode::client_error_bad_request, p.error.getErrorJSON());
        return;
    }

    // validate that sortKeys are in the select statement
    const auto sortKeyParts = split(sortKeyString, ',');

    auto index = 0;
    for (auto key : sortKeyParts)
    {
        key = trim(key);
        auto found = false;

        if (key.length())
        {
            for (auto& column : queryMacros.vars.columnVars)
            {
                if (column.alias == key)
                {
                    found = true;

                    queryMacros.vars.autoGrouping.push_back(index);

                    break;
                }
            }
        }

        if (key.length() == 0 || !found)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::general_error,
                    "sort key in query string not found in query script select statement"
                },
                message);
            return;
        }

        ++index;
    }



    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts             = split(segmentText, ',');
        queryMacros.segments.clear();
        for (const auto& part : parts)
        {
            const auto trimmedPart = trim(part);
            if (trimmedPart.length())
                queryMacros.segments.push_back(trimmedPart);
        }
        if (!queryMacros.segments.size())
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "no segment names specified"
                },
                message);
            return;
        }
    }

    // set the sessionTime (timeout) value, this will get relayed
    // through the to oloop_query, the customer object and finally the grid
    queryMacros.sessionTime = sessionTime;
    if (debug)
    {
        auto debugOutput = MacroDbg(queryMacros); // reply as text
        message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
        return;
    }
    auto sortColumn = 0;

    /*
    * We are originating the query.
    *
    * At this point in the function we have validated that the
    * script compiles, maps to the schema, is on a valid table,
    * etc.
    *
    * We will call our forkQuery function.
    *
    * forQuery will call all the nodes (including this one) with the
    * `is_fork` variable set to true.
    */

    if (!isFork)
    {
        const auto json = forkQuery(
            table,
            message,
            queryMacros.vars.columnVars.size(),
            queryMacros.segments.size(),
            sortMode,
            sortOrder,
            sortColumn,
            trimSize);
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }

    // We are a Fork!

    // create list of active_owner partitions for factory function
    auto activeList = globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
        globals::running->nodeId,
        {
            mapping::NodeState_e::active_owner
        });

    // Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
    //      we don't have to worry about locking anything shared between partitions in the same
    //      thread as they are executed serially, rather than in parallel.
    //
    //      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
    //      as well as generally reduce the number of ResultSets needed (especially when partition
    //      counts are high).
    //
    //      Note: These are heap objects because we lose scope, as this function
    //            exits before the result objects are used.
    //
    std::vector<ResultSet*> resultSets;
    resultSets.reserve(partitions->getWorkerCount());
    for (auto i = 0; i < partitions->getWorkerCount(); ++i)
        resultSets.push_back(
            new ResultSet(
                queryMacros.vars.columnVars.size() * (queryMacros.segments.size()
                  ? queryMacros.segments.size()
                  : 1)));

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer    = ResultMuxDemux::multiSetToInternode(
            queryMacros.vars.columnVars.size(),
            queryMacros.segments.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);
        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        Logger::get().info("event query on " + table->getName());
        for (auto resultSet : resultSets)
            delete resultSet;
        return;
    }

    /*
    *  this Shuttle will gather our result sets roll them up and spit them back
    *
    *  note that queryMacros are captured with a copy, this is because a reference
    *  version will have had it's destructor called when the function exits.
    *
    *  Note: ShuttleLamda comes in two versions,
    */
    const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [queryMacros, table, resultSets](
        vector<response_s<CellQueryResult_s>>& responses,
        web::MessagePtr message,
        voidfunc release_cb) mutable
        {
            // process the data and respond
            // check for errors, add up totals
            for (const auto& r : responses)
            {
                if (r.data.error.inError())
                {
                    // any error that is recorded should be considered a hard error, so report it
                    const auto errorMessage = r.data.error.getErrorJSON();
                    Logger::get().error(errorMessage);
                    message->reply(http::StatusCode::client_error_bad_request, errorMessage);
                    // clean up stray resultSets
                    for (auto resultSet : resultSets)
                        delete resultSet;

                    release_cb();
                    return;
                }
            }

            // 1. Merge the Macro Literals
            ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

            // 2. Merge the rows
            int64_t bufferLength = 0;
            const auto buffer    = ResultMuxDemux::multiSetToInternode(
                queryMacros.vars.columnVars.size(),
                queryMacros.segments.size(),
                //queryMacros.indexes.size(),
                resultSets,
                bufferLength);

            message->reply(http::StatusCode::success_ok, buffer, bufferLength);
            PoolMem::getPool().freePtr(buffer);

            Logger::get().info("event query on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;

            // this will delete the shuttle, and clear up the CellQueryResult_s vector
            release_cb();
        });

    auto instance = 0;

    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(
        activeList,
        [shuttle, table, queryMacros, resultSets, &instance](AsyncLoop* loop) -> OpenLoop*
        {
            instance++;
            return new OpenLoopCustomerList(shuttle, table, queryMacros, resultSets[loop->getWorkerId()], instance);
        });
}

void RpcQuery::segment(const openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database         = globals::database;
    const auto partitions = globals::async;
    const auto request    = message->getJSON();
    const auto tableName  = matches.find("table"s)->second;
    const auto queryCode  = std::string { message->getPayload(), message->getPayloadLength() };
    const auto debug      = message->getParamBool("debug");
    const auto isFork     = message->getParamBool("fork");
    const auto log        = "Inbound counts query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;

    Logger::get().info(log);

    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    if (!queryCode.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing query code (POST query as text)"
            },
            message);
        return;
    }

    auto table = database->getTable(tableName);

    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    query::ParamVars paramVars = getInlineVaraibles(message);
    // get the functions extracted and de-indented as named code blocks
    auto subQueries = query::QueryParser::extractSections(queryCode.c_str());
    query::QueryPairs queries;

    // loop through the extracted functions (subQueries) and compile them
    for (auto& r : subQueries)
    {
        if (r.sectionType != "segment")
            continue;
        query::Macro_s queryMacros; // this is our compiled code block
        query::QueryParser p;
        p.compileQuery(r.code.c_str(), table->getProperties(), queryMacros, &paramVars, query::ParseMode_e::segment);

        if (p.error.inError())
        {
            cjson response;
            const auto errorMessage = p.error.getErrorJSON();
            Logger::get().error(errorMessage);
            message->reply(http::StatusCode::client_error_bad_request, errorMessage);
            return;
        }

        if (r.flags.contains("ttl"))
        {
            queryMacros.segmentTTL = r.flags["ttl"];
            table->setSegmentTtl(r.sectionName, r.flags["ttl"]);
        }

        const auto zIndex    = r.flags.contains("z_index") ? r.flags["z_index"].getInt32() : 100;
        const auto onInsert  = r.flags.contains("on_insert") ? r.flags["on_insert"].getBool() : false;
        const auto useCached = r.flags.contains("use_cached") ? r.flags["use_cached"].getBool() : false;

        queryMacros.useCached = useCached;
        queryMacros.isSegment = true;

        if (r.flags.contains("refresh"))
        {
            queryMacros.segmentRefresh = r.flags["refresh"];
            table->setSegmentRefresh(r.sectionName, queryMacros, r.flags["refresh"], zIndex, onInsert);
        }
        else
        {
            // remove any segment caching that may have had a prior refresh setting
            table->removeSegmentRefresh(r.sectionName);
        }

        queries.emplace_back(std::pair<std::string, query::Macro_s> { r.sectionName, queryMacros });
    }

    // Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
    //      we don't have to worry about locking anything shared between partitions in the same
    //      thread as they are executed serially, rather than in parallel.
    //
    //      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
    //      as well as generally reduce the number of ResultSets needed (especially when partition
    //      counts are high).
    std::vector<ResultSet*> resultSets;
    if (debug)
    {
        std::string debugOutput;
        for (auto& m : queries)
            debugOutput += "Script: " + m.first +
                "\n=====================================================================================\n\n" +
                MacroDbg(m.second); // reply as text
        message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
        return;
    }

    /*
    * We are originating the query.
    *
    * At this point in the function we have validated that the
    * script compiles, maps to the schema, is on a valid table,
    * etc.
    *
    * We will call our forkQuery function.
    *
    * forQuery will call all the nodes (including this one) with the
    * `is_fork` variable set to true.
    *
    *
    */
    if (!queries.size())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::syntax_error,
                "no @segment sections could be found"
            },
            message);
        return;
    }

    if (!isFork)
    {
        const auto json = forkQuery(
            table,
            message,
            queries.front().second.vars.columnVars.size(),
            queries.front().second.segments.size());
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }

    // We are a Fork!
    auto activeList = globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
        globals::running->nodeId,
        {
            mapping::NodeState_e::active_owner
        });

    resultSets.reserve(partitions->getWorkerCount());

    for (auto i = 0; i < partitions->getWorkerCount(); ++i)
        resultSets.push_back(new ResultSet(1));

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queries.front().second, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(1, 1, resultSets, bufferLength);

        // reply is responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);
        PoolMem::getPool().freePtr(buffer);
        Logger::get().info("No active workers for " + table->getName());

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;

        return;
    }

    auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [queries, table, resultSets](
        const vector<response_s<CellQueryResult_s>>& responses,
        web::MessagePtr message,
        voidfunc release_cb) mutable
        {
            // check for errors, add up totals
            for (const auto& r : responses)
            {
                if (r.data.error.inError())
                {
                    // any error that is recorded should be considered a hard error, so report it
                    message->reply(http::StatusCode::client_error_bad_request, r.data.error.getErrorJSON());

                    // clean up stray resultSets
                    for (auto resultSet : resultSets)
                        delete resultSet;

                    release_cb();
                    return;
                }
            }

            // 1. Merge Macro Literals
            ResultMuxDemux::mergeMacroLiterals(queries.front().second, resultSets);

            // 2. Merge the rows
            int64_t bufferLength = 0;
            const auto buffer    = ResultMuxDemux::multiSetToInternode(
                queries.front().second.vars.columnVars.size(),
                queries.front().second.indexes.size(),
                resultSets,
                bufferLength);

            message->reply(http::StatusCode::success_ok, buffer, bufferLength);
            PoolMem::getPool().freePtr(buffer);

            Logger::get().info("segment query on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;

            release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector
        });

    auto instance = 0;
    auto workers  = 0;

    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(
        activeList,
        [shuttle, table, queries, resultSets, &workers, &instance](AsyncLoop* loop) -> OpenLoop*
        {
            ++instance;
            ++workers;
            return new OpenLoopSegment(shuttle, table, queries, resultSets[loop->getWorkerId()], instance);
        });

    Logger::get().info("Started " + to_string(workers) + " count worker async cells.");
}

void RpcQuery::property(openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database         = globals::database;
    const auto partitions = globals::async;
    const auto tableName  = matches.find("table"s)->second;
    const auto columnName = matches.find("name"s)->second;
    const auto isFork     = message->getParamBool("fork");
    const auto trimSize   = message->getParamInt("trim", -1);
    const auto sortOrder  = message->getParamString("order", "desc") == "asc"
                                ? ResultSortOrder_e::Asc
                                : ResultSortOrder_e::Desc;
    if (!tableName.size())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::config,
                errors::errorCode_e::general_config_error,
                "missing /params/table"
            },
            message);
        return;
    }

    const auto table = database->getTable(tableName);
    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::config,
                errors::errorCode_e::general_config_error,
                "table not found"
            },
            message);
        return;
    }

    if (!columnName.size())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::config,
                errors::errorCode_e::general_config_error,
                "invalid property name"
            },
            message);
        return;
    }

    const auto column = table->getProperties()->getProperty(columnName);

    if (!column || column->type == PropertyTypes_e::freeProp)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::config,
                errors::errorCode_e::general_config_error,
                "property not found"
            },
            message);
        return;
    }

    // We are a Fork!
    OpenLoopProperty::ColumnQueryConfig_s queryInfo;
    queryInfo.propName  = columnName;
    queryInfo.propType  = column->type;
    queryInfo.propIndex = column->idx;

    if (message->isParam("gt"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::gt;
        queryInfo.filterLow = message->getParamString("gt");
    }
    else if (message->isParam("gte"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::gte;
        queryInfo.filterLow = message->getParamString("gte");
    }
    else if (message->isParam("lt"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::lt;
        queryInfo.filterLow = message->getParamString("lt");
    }
    else if (message->isParam("lte"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::lte;
        queryInfo.filterLow = message->getParamString("lte");
    }
    else if (message->isParam("eq"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::eq;
        queryInfo.filterLow = message->getParamString("eq");
    }
    else if (message->isParam("between"))
    {
        queryInfo.mode       = OpenLoopProperty::PropertyQueryMode_e::between;
        queryInfo.filterLow  = message->getParamString("between");
        queryInfo.filterHigh = message->getParamString("and");
    }
    else if (message->isParam("rx"))
    {
        queryInfo.mode = OpenLoopProperty::PropertyQueryMode_e::rx;
        // bad regex blows this thing up... so lets catch any errors
        auto isError = false;

        try
        {
            queryInfo.rx = std::regex(message->getParamString("rx"));
        }
        catch (const std::runtime_error&)
        {
            isError = true;
        }
        catch (...)
        {
            isError = true;
        }

        if (isError)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "could not compile regular express: " + message->getParamString("rx")
                },
                message);
            return;
        }
    }
    else if (message->isParam("sub"))
    {
        queryInfo.mode      = OpenLoopProperty::PropertyQueryMode_e::sub;
        queryInfo.filterLow = message->getParamString("sub");
    }
    else
    {
        queryInfo.mode = OpenLoopProperty::PropertyQueryMode_e::all;
    }
    if (queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::all)
    {
        if (queryInfo.filterLow.getString().length() == 0)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "property filter requires a value"
                },
                message);
            return;
        }
    }
    if (queryInfo.mode == OpenLoopProperty::PropertyQueryMode_e::between)
    {
        if (queryInfo.filterHigh.getString().length() == 0)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "property query using 'between' requires an 'and' param"
                },
                message);
            return;
        }
    }
    if (message->isParam("bucket"))
        queryInfo.bucket = message->getParamString("bucket");

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts             = split(segmentText, ',');
        queryInfo.segments.clear();
        for (const auto& part : parts)
        {
            const auto trimmedPart = trim(part);
            if (trimmedPart.length())
                queryInfo.segments.push_back(trimmedPart);
        }
        if (!queryInfo.segments.size())
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "no segment names specified"
                },
                message);
            return;
        }
    }

    // here we are going to force typing depending on the property type
    // note: prior to conversion these will all be strings
    switch (queryInfo.propType)
    {
    case PropertyTypes_e::intProp:
        queryInfo.bucket = queryInfo.bucket.getInt64();
        queryInfo.filterLow  = queryInfo.filterLow.getInt64();
        queryInfo.filterHigh = queryInfo.filterHigh.getInt64();
        break;
    case PropertyTypes_e::doubleProp:
        // floating point data in the db is in scaled integers, scale our ranges and buckets
        queryInfo.bucket = static_cast<int64_t>(queryInfo.bucket.getDouble() * 10000.0);
        queryInfo.filterLow  = static_cast<int64_t>(queryInfo.filterLow.getDouble() * 10000.0);
        queryInfo.filterHigh = static_cast<int64_t>(queryInfo.filterHigh.getDouble() * 10000.0);
        break;
    case PropertyTypes_e::boolProp:
        queryInfo.filterLow = queryInfo.filterLow.getBool();
        break;
    case PropertyTypes_e::textProp:
        queryInfo.filterLow = queryInfo.filterLow.getString();
        break;
    default: ;
    }

    // now lets make sure the `propType` and the `mode` make sense together and
    // return an error if they do not.
    if (queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::all &&
        queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::eq)
    {
        switch (queryInfo.propType)
        {
        case PropertyTypes_e::intProp: case PropertyTypes_e::doubleProp:
            if (queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::between && queryInfo.mode != OpenLoopProperty::
                PropertyQueryMode_e::gt && queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::gte && queryInfo.mode !=
                OpenLoopProperty::PropertyQueryMode_e::lt && queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::lte)
            {
                RpcError(
                    errors::Error {
                        errors::errorClass_e::query,
                        errors::errorCode_e::syntax_error,
                        "specified filter type not compatible with integer or double property"
                    },
                    message);
                return;
            }
            break;
        case PropertyTypes_e::textProp:
            if (queryInfo.mode != OpenLoopProperty::PropertyQueryMode_e::rx && queryInfo.mode != OpenLoopProperty::
                PropertyQueryMode_e::sub)
            {
                RpcError(
                    errors::Error {
                        errors::errorClass_e::query,
                        errors::errorCode_e::syntax_error,
                        "specified filter type not compatible with string property"
                    },
                    message);
                return;
            }
            break;
        case PropertyTypes_e::boolProp: default: ;
        }
    }

    /*
    * We are originating the query.
    *
    * At this point in the function we have validated that the
    * script compiles, maps to the schema, is on a valid table,
    * etc.
    *
    * We will call our forkQuery function.
    *
    * forQuery will call all the nodes (including this one) with the
    * `is_fork` variable set to true.
    */
    if (!isFork)
    {
        const auto json = forkQuery(
            table,
            message,
            1,
            queryInfo.segments.size(),
            ResultSortMode_e::column,
            sortOrder,
            0,
            trimSize);
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }

    // create list of active_owner partitions for factory function
    const auto activeList = globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
        globals::running->nodeId,
        {
            mapping::NodeState_e::active_owner
        });

    // Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
    //      we don't have to worry about locking anything shared between partitions in the same
    //      thread as they are executed serially, rather than in parallel.
    //
    //      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
    //      as well as generally reduce the number of ResultSets needed (especially when partition
    //      counts are high).
    //
    //      Note: These are heap objects because we lose scope, as this function
    //            exits before the result objects are used.
    //
    std::vector<ResultSet*> resultSets;
    resultSets.reserve(partitions->getWorkerCount());

    for (auto i = 0; i < partitions->getWorkerCount(); ++i)
        resultSets.push_back(
            new ResultSet(queryInfo.segments.size() ? queryInfo.segments.size() : 1));

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer    = ResultMuxDemux::multiSetToInternode(
            1,
            queryInfo.segments.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);
        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;
        return;
    }

    /*
    *  this Shuttle will gather our result sets roll them up and spit them back
    *
    *  note that queryMacros are captured with a copy, this is because a reference
    *  version will have had it's destructor called when the function exits.
    *
    *  Note: ShuttleLambda comes in two versions,
    */
    const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [table, resultSets, queryInfo](
        vector<response_s<CellQueryResult_s>>& responses,
        web::MessagePtr message,
        voidfunc release_cb) mutable
        {
            // process the data and respond
            // check for errors, add up totals
            for (const auto& r : responses)
            {
                if (r.data.error.inError())
                {
                    // any error that is recorded should be considered a hard error, so report it
                    const auto errorMessage = r.data.error.getErrorJSON();
                    message->reply(http::StatusCode::client_error_bad_request, errorMessage);
                    // clean up stray resultSets
                    for (auto resultSet : resultSets)
                        delete resultSet;
                    release_cb();
                    return;
                }
            }

            // 1. Merge the rows
            int64_t bufferLength = 0;
            const auto buffer    = ResultMuxDemux::multiSetToInternode(
                1,
                queryInfo.segments.size(),
                resultSets,
                bufferLength);

            message->reply(http::StatusCode::success_ok, buffer, bufferLength);

            Logger::get().info("Fork query on " + table->getName());

            // clean up all those resultSet*
            for (auto r : resultSets)
                delete r;

            release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector
        });

    auto instance = 0;

    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(
        activeList,
        [shuttle, table, queryInfo, resultSets, &instance](AsyncLoop* loop) -> OpenLoop*
        {
            instance++;
            return new OpenLoopProperty(shuttle, table, queryInfo, resultSets[loop->getWorkerId()], instance);
        });
}

void RpcQuery::customer(openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto uuString = message->getParamString("id");

    if (!uuString.size())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "customer query must expecting id= parameter"
            },
            message);
        return;
    }

    const auto tableName = matches.find("table"s)->second;
    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    const auto table = globals::database->getTable(tableName);
    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    int64_t uuid = std::numeric_limits<int64_t>::min();

    if (table->numericCustomerIds)
    {
        try
        {
        uuid = stoll(uuString);
        }
        catch (...)
        {
            // error returned below in `if (uuid == ::min())
        }
    }
    else
    {
        toLower(uuString);
        uuid = MakeHash(uuString);
    }

    if (uuid == std::numeric_limits<int64_t>::min())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "invalid customer id"
            },
            message);
        return;
    }

    const auto partitions = globals::async;
    const auto targetPartition = cast<int32_t>((std::abs(uuid) % 13337) % partitions->getPartitionMax());
    const auto mapper = globals::mapper->getPartitionMap();
    auto owners = mapper->getNodesByPartitionId(targetPartition);

    int64_t targetRoute = -1;

    // find the owner
    for (auto o : owners)
    {
        if (mapper->isOwner(targetPartition, o))
        {
            targetRoute = o;
            break;
        }
    }

    if (targetRoute == -1)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::route_error,
                "potential node failure - please re-issue the request"
            },
            message);
        return;
    }

    // this query is local - we will fire up a single async get user task on this node
    if (targetRoute == globals::running->nodeId)
    {
        // lets use the super basic shuttle.
        const auto shuttle = new Shuttle<int>(message);
        auto loop          = globals::async->getPartition(targetPartition);
        if (!loop)
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::route_error,
                    "potential node failure - please re-issue the request"
                },
                message);
            return;
        }
        loop->queueCell(new OpenLoopCustomer(shuttle, table, uuid));
    }
    else // remote - we will route to the correct destination node
    {
        const auto response = globals::mapper->dispatchSync(
            targetRoute,
            message->getMethod(),
            message->getPath(),
            message->getQuery(),
            message->getPayload(),
            message->getPayloadLength());

        message->reply(response->code, response->data, response->length);
    }
}

void RpcQuery::histogram(openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database         = globals::database;
    const auto partitions = globals::async;
    const auto request    = message->getJSON();
    const auto tableName  = matches.find("table"s)->second;
    const auto groupName  = matches.find("name"s)->second;
    const auto queryCode  = std::string { message->getPayload(), message->getPayloadLength() };

    const auto debug      = message->getParamBool("debug");
    const auto isFork     = message->getParamBool("fork");
    const auto trimSize   = message->getParamInt("trim", -1);
    const auto sortOrder  = message->getParamString("order", "desc") == "asc"
        ? ResultSortOrder_e::Asc
        : ResultSortOrder_e::Desc;
    const auto sortMode = ResultSortMode_e::key;

    const auto log      = "Inbound events query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
    Logger::get().info(log);

    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    if (!queryCode.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing query code (POST query as text)"
            },
            message);
        return;
    }

    auto table = database->getTable(tableName);
    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    // override session time if provided, otherwise use table default
    const auto sessionTime     = message->getParamInt("session_time", table->getSessionTime());
    query::ParamVars paramVars = getInlineVaraibles(message);
    query::Macro_s queryMacros; // this is our compiled code block
    query::QueryParser p;

    try
    {
        p.compileQuery(queryCode.c_str(), table->getProperties(), queryMacros, &paramVars);
    }
    catch (const std::runtime_error& ex)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                std::string { ex.what() }
            },
            message);
        return;
    }

    if (p.error.inError())
    {
        Logger::get().error(p.error.getErrorJSON());
        message->reply(http::StatusCode::client_error_bad_request, p.error.getErrorJSON());
        return;
    }

    // Histogram querys must call tally
    if (queryMacros.marshalsReferenced.count(query::Marshals_e::marshal_tally))
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                "histogram queries should not call 'tally'. They should 'return' the value to store."
            },
            message);
        return;
    }

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts             = split(segmentText, ',');
        queryMacros.segments.clear();
        for (auto& part : parts)
        {
            auto trimmedPart = trim(part);
            if (trimmedPart.length())
                queryMacros.segments.push_back(trimmedPart);
        }
        if (!queryMacros.segments.size())
        {
            RpcError(
                errors::Error {
                    errors::errorClass_e::query,
                    errors::errorCode_e::syntax_error,
                    "no segment names specified"
                },
                message);
            return;
        }
    }

    // set the sessionTime (timeout) value, this will get relayed
    // through the to oloop_query, the customer object and finally the grid
    queryMacros.sessionTime = sessionTime;
    if (debug)
    {
        auto debugOutput = MacroDbg(queryMacros);
        // reply as text
        message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
        return;
    }

    int64_t bucket = 0;
    if (message->isParam("bucket"))
        bucket = static_cast<int64_t>(stod(message->getParamString("bucket", "0")) * 10000.0);

    auto forceMin = std::numeric_limits<int64_t>::min();
    if (message->isParam("min"))
        forceMin = static_cast<int64_t>(stod(message->getParamString("min", "0")) * 10000.0);

    auto forceMax = std::numeric_limits<int64_t>::min();
    if (message->isParam("max"))
        forceMax = static_cast<int64_t>(stod(message->getParamString("max", "0")) * 10000.0);

    /*
    * We are originating the query.
    *
    * At this point in the function we have validated that the
    * script compiles, maps to the schema, is on a valid table,
    * etc.
    *
    * We will call our forkQuery function.
    *
    * forQuery will call all the nodes (including this one) with the
    * `is_fork` variable set to true.
    */
    if (!isFork)
    {
        const auto json = forkQuery(
            table,
            message,
            1,
            queryMacros.segments.size(),
            sortMode,
            sortOrder,
            0,
            trimSize,
            bucket,
            forceMin,
            forceMax);
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }

    // We are a Fork!
    // create list of active_owner parititions for factory function
    auto activeList = globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
        globals::running->nodeId,
        {
            mapping::NodeState_e::active_owner
        });

    // Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
    //      we don't have to worry about locking anything shared between partitions in the same
    //      thread as they are executed serially, rather than in parallel.
    //
    //      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
    //      as well as generally reduce the number of ResultSets needed (especially when partition
    //      counts are high).
    //
    //      Note: These are heap objects because we lose scope, as this function
    //            exits before the result objects are used.
    //
    std::vector<ResultSet*> resultSets;
    resultSets.reserve(partitions->getWorkerCount());
    for (auto i = 0; i < partitions->getWorkerCount(); ++i)
        resultSets.push_back(
            new ResultSet(
                queryMacros.vars.columnVars.size() * (queryMacros.segments.size()
                    ? queryMacros.segments.size()
                    : 1)));

    // nothing active - return an empty set - not an error
    if (activeList.empty())
    {
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer    = ResultMuxDemux::multiSetToInternode(
            queryMacros.vars.columnVars.size(),
            queryMacros.segments.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);
        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;
        return;
    }

    /*
    *  this Shuttle will gather our result sets roll them up and spit them back
    *
    *  note that queryMacros are captured with a copy, this is because a reference
    *  version will have had it's destructor called when the function exits.
    *
    *  Note: ShuttleLamda comes in two versions,
    */
    const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [queryMacros, table, resultSets](
        vector<response_s<CellQueryResult_s>>& responses,
        web::MessagePtr message,
        voidfunc release_cb) mutable
        {
            // process the data and respond
            // check for errors, add up totals
            for (const auto& r : responses)
            {
                if (r.data.error.inError())
                {
                    // any error that is recorded should be considered a hard error, so report it
                    const auto errorMessage = r.data.error.getErrorJSON();
                    message->reply(http::StatusCode::client_error_bad_request, errorMessage);
                    // clean up stray resultSets
                    for (auto resultSet : resultSets)
                        delete resultSet;
                    release_cb();
                    return;
                }
            }
            int64_t bufferLength = 0;
            const auto buffer    = ResultMuxDemux::multiSetToInternode(
                1,
                queryMacros.segments.size(),
                resultSets,
                bufferLength);

            message->reply(http::StatusCode::success_ok, buffer, bufferLength);

            Logger::get().info("Fork query on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;
            PoolMem::getPool().freePtr(buffer);

            // this will delete the shuttle, and clear up the CellQueryResult_s vector
            release_cb();
        });

    auto forEach = message->isParam("foreach") ? message->getParamString("foreach") : ""s;
    auto instance = 0;

    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(
        activeList,
        [shuttle, table, queryMacros, resultSets, groupName, bucket, forEach, &instance](AsyncLoop* loop) -> OpenLoop*
        {
            instance++;
            return new OpenLoopHistogram(
                shuttle,
                table,
                queryMacros,
                groupName,
                forEach,
                bucket,
                resultSets[loop->getWorkerId()],
                instance);
        });
}

openset::mapping::Mapper::Responses queryDispatch(
    std::string tableName,
    openset::query::SegmentList segments,
    openset::query::QueryParser::SectionDefinitionList queries)
{
    const auto runMax = 1; // maximum concurrent queries allowed.
    CriticalSection cs;
    auto iter          = queries.begin();
    auto doneSending   = false;
    auto receivedCount = 0;
    auto sendCount     = 0; // number of queries sent
    auto running       = 0; // number currently running
    openset::mapping::Mapper::Responses result;
    const auto completeCallback = [&](const openset::http::StatusCode status, const bool, char* data, const size_t size)
    {
        csLock lock(cs);
        const auto dataCopy = static_cast<char*>(PoolMem::getPool().getPtr(size));
        memcpy(dataCopy, data, size);
        result.responses.emplace_back(openset::mapping::Mapper::DataBlock { dataCopy, size, status });
        --running;
        ++receivedCount;
    };
    const auto sendOne = [&]() -> bool
    {
        std::string method = "GET";
        std::string path;
        openset::web::QueryParams params;
        std::string payload;
        {
            if (doneSending)
                return false;

            csLock lock(cs);

            if (iter == queries.end() || result.routeError)
            {
                doneSending = true;
                return false;
            }
            ++running;
            ++sendCount;

            // convert captures in section definition and converts to REST params
            for (auto &p : *(iter->params.getDict()))
                if (p.first.getString() != "each")                             // missing a char* != ???
                    params.emplace(p.first.getString(), p.second.getString()); // add a segments param

            if (segments.size())
                params.emplace("segments"s, join(segments)); // make queries

            if (iter->sectionType == "segment")
            {
                method              = "POST";
                path                = "/v1/query/" + tableName + "/segment";
                std::string segline = "@segment " + iter->sectionName + " ";
                for (auto f : *(iter->flags.getDict()))
                    segline += f.first.getString() + "=" + f.second.getString() + " ";
                segline += "\n";
                payload += segline + std::move(iter->code); // eat it
            }
            else if (iter->sectionType == "property")
            {
                path    = "/v1/query/" + tableName + "/property/" + iter->sectionName;
                payload = std::move(iter->code); // eat it
            }
            else if (iter->sectionType == "histogram")
            {
                method  = "POST";
                path    = "/v1/query/" + tableName + "/histogram/" + iter->sectionName;
                payload = std::move(iter->code); // eat it
            }
            ++iter;
        }

        // fire these queries off
        const auto success = openset::globals::mapper->dispatchAsync(
            openset::globals::running->nodeId,
            // fork to self
            method,
            path,
            params,
            payload,
            completeCallback);

        if (!success)
            result.routeError = true;

        return true;
    };

    while (sendOne())
    {
        while (running > runMax)
            ThreadSleep(55);
    }

    while (!doneSending && sendCount != receivedCount)
        ThreadSleep(50); // TODO replace with semaphore

    return result;
}

void RpcQuery::batch(openset::web::MessagePtr& message, const RpcMapping& matches)
{
    auto database        = globals::database;
    const auto request   = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto queryCode = std::string { message->getPayload(), message->getPayloadLength() };
    const auto debug     = message->getParamBool("debug");

    Logger::get().info("Inbound multi query"s);

    if (!tableName.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing or invalid table name"
            },
            message);
        return;
    }

    if (!queryCode.length())
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "missing query code (POST query as text)"
            },
            message);
        return;
    }

    const auto table = database->getTable(tableName);
    if (!table)
    {
        RpcError(
            errors::Error {
                errors::errorClass_e::query,
                errors::errorCode_e::general_error,
                "table could not be found"
            },
            message);
        return;
    }

    thread runner(
        [=]()
        {
            // get the functions extracted and de-indented as named code blocks
            auto subQueries = query::QueryParser::extractSections(queryCode.c_str());
            query::QueryParser::SectionDefinitionList segmentList;
            query::QueryParser::SectionDefinitionList queryList;
            query::QueryParser::SectionDefinition_s useSection;
            query::SegmentList segments;

            for (auto& s : subQueries)
            {
                if (s.sectionType == "segment")
                    segmentList.push_back(s);
                else if (s.sectionType == "use")
                    useSection = s;
                else
                    queryList.push_back(s);
            }

            if (useSection.sectionType == "use" && useSection.sectionName.length())
            {
                segments.push_back(useSection.sectionName);
                for (const auto& p : *useSection.params.getDict())
                    segments.push_back(p.first);
            }

            if (segmentList.size())
            {
                auto results = queryDispatch(tableName, segments, segmentList);
                for (auto& r : results.responses)
                {
                    if (r.code != http::StatusCode::success_ok)
                    {
                        // try to capture a json error that has perculated up from the forked call.
                        if (r.data && r.length && r.data[0] == '{')
                        {
                            cjson error(std::string(r.data, r.length), cjson::Mode_e::string);
                            if (error.xPath("/error"))
                            {
                                message->reply(http::StatusCode::client_error_bad_request, error);
                                return;
                            }
                            results.routeError = true;
                        }
                        else
                            results.routeError = true; // this will trigger the next error
                    }
                }
                if (results.routeError)
                {
                    RpcError(
                        errors::Error {
                            errors::errorClass_e::config,
                            errors::errorCode_e::route_error,
                            "potential node failure - please re-issue the request"
                        },
                        message);
                    return;
                }
            }

            if (queryList.size())
            {
                auto results = queryDispatch(tableName, segments, queryList);
                for (auto& r : results.responses)
                {
                    if (r.code != http::StatusCode::success_ok)
                    {
                        // try to capture a json error that has perculated up from the forked call.
                        if (r.data && r.length && r.data[0] == '{')
                        {
                            cjson error(std::string(r.data, r.length), cjson::Mode_e::string);
                            if (error.xPath("/error"))
                            {
                                message->reply(http::StatusCode::client_error_bad_request, error);
                                return;
                            }
                            results.routeError = true;
                        }
                        else
                            results.routeError = true; // this will trigger the next error
                    }
                }

                if (results.routeError)
                {
                    RpcError(
                        errors::Error {
                            errors::errorClass_e::config,
                            errors::errorCode_e::route_error,
                            "potential node failure - please re-issue the request"
                        },
                        message);
                    return;
                }

                cjson responseJson;
                auto resultBranch = responseJson.setArray("_");
                for (auto& r : results.responses)
                {
                    const auto insertAt = resultBranch->pushObject();
                    cjson resultItemJson { std::string { r.data, r.length }, cjson::Mode_e::string };
                    if (const auto item = resultItemJson.xPath("/_/0"); item)
                        cjson::parse(cjson::stringify(item), insertAt, true);
                }

                message->reply(http::StatusCode::success_ok, responseJson);
            }
        });

    runner.detach();
}
