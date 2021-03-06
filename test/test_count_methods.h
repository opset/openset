#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../lib/str/strtools.h"
#include "../lib/var/var.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/properties.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryinterpreter.h"
#include "../src/queryparserosl.h"
#include "../src/internoderouter.h"
#include "../src/result.h"

#include <unordered_set>

// Our tests
inline Tests test_count_methods()
{

    // An array of JSON events to insert, we are going to
    // insert these out of order and count on zorder to
    // sort them.
    // we will set zorder for action to "alpha", "beta", "cappa", "delta", "echo"
    auto user1_raw_inserts = R"raw_inserts(
    [
        {
            "id": "user1@test.com",
            "stamp": 1458800000,
            "event": "some event",
            "_":{
                "some_val": 100,
                "some_thing": "rabbit",
                "some_color": "orange"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458800000,
            "event": "some event",
            "_":{
                "some_val": 100,
                "some_thing": "rabbit",
                "some_color": "purple"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458801000,
            "event": "some event",
            "_":{
                "some_val": 200,
                "some_thing": "goat",
                "some_color": "green"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458801000,
            "event": "some event",
            "_":{
                "some_val": 200,
                "some_thing": "goat",
                "some_color": "golden"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458801000,
            "event": "some event",
            "_":{
                "some_val": 200,
                "some_thing": "goat",
                "some_color": "cyan"
            }
        },
    ]
    )raw_inserts";

    auto test1_pyql = R"pyql(
    agg:
        count id
        count some_thing
        count some_color
        sum some_val

    for row in rows:
        tally(row['some_thing'], row['some_color'])

    )pyql";


    /* In order to make the engine start there are a few required objects as
     * they will get called in the background during testing:
     *
     *  - cfg::manager must exist // cfg::initConfig)
     *  - __AsyncManager must exist // new OpenSet::async::AyncPool(...)
     *  - Databse must exist // databases contain tabiles
     *
     *  These objects will be created on the heap, although in practice during
     *  the construction phase these are created as local objects to other classes.
     */

    // need config objects to run this
    openset::config::CommandlineArgs args;
    openset::globals::running = new openset::config::Config(args);

    // stop load/save objects from doing anything
    openset::globals::running->testMode = true;

    // we need an async engine, although we won't really be using it,
    // it's wired into the into features such as tablePartitioned (shared locks mostly)
    openset::async::AsyncPool* async = new openset::async::AsyncPool(1, 1); // 1 worker

    openset::mapping::PartitionMap partitionMap;
    // this must be on heap to keep it in scope
    openset::mapping::Mapper* mapper = new openset::mapping::Mapper();
    mapper->startRouter();


    // put engine in a wait state otherwise we will throw an exception
    async->suspendAsync();

    //auto database = new Database();

    return {
        /*
        {
            "test_count_methods: create and prepare a table", [=] {

                // prepare our table
                auto table = openset::globals::database->newTable("__testcountmethods__");

                // add some properties
                auto properties = table->getProperties();
                ASSERT(properties != nullptr);

                // content (adding to 2000 range, these typically auto enumerated on create)
                properties->setProperty(2000, "some_val", openset::db::PropertyTypes_e::intProp, false);
                properties->setProperty(2001, "some_thing", openset::db::PropertyTypes_e::textProp, false);
                properties->setProperty(2002, "some_color", openset::db::PropertyTypes_e::textProp, false);

                auto parts = table->getPartitionObjects(0, true); // partition zero for test
                auto personRaw = parts->people.createCustomer("user1@test.com");

                Customer customer; // Customer overlay for personRaw;

                customer.mapTable(table.get(), 0); // will throw in DEBUG if not called before mount
                customer.mount(personRaw);

                // parse the user1_raw_inserts raw JSON text block
                cjson insertJSON(user1_raw_inserts, cjson::Mode_e::string);

                // get vector of cjson nodes for each element in root array
                auto events = insertJSON.getNodes();

                for (auto e : events)
                {
                    ASSERT(e->xPathInt("/stamp", 0) != 0);
                    ASSERT(e->xPath("/_") != nullptr);

                    customer.insert(e);
                }

                customer.commit();

            }
        },
        {
            "test_count_methods: normal count", [=]
            {
                auto database = openset::globals::database;

                auto table = openset::globals::database->getTable("__testcountmethods__");
                auto parts = table->getPartitionObjects(0, true); // partition zero for test

                openset::query::Macro_s queryMacros; // this is our compiled code block
                openset::query::QueryParser p;

                // compile this
                p.compileQuery(test1_pyql.c_str(), table->getProperties(), queryMacros);
                ASSERT(p.error.inError() == false);

                // mount the compiled query to an interpretor
                auto interpreter = new openset::query::Interpreter(queryMacros);

                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.createCustomer("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the properties in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries

                Customer customer; // Customer overlay for personRaw;
                customer.mapTable(table.get(), 0, mappedColumns);

                customer.mount(personRaw); // this tells the customer object where the raw compressed data is
                customer.prepare(); // this actually decompresses

                                  // this mounts the now decompressed data (in the customer overlay)
                                  // into the interpreter
                interpreter->mount(&customer);

                // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);

                // just getting a pointer to the results for nicer readability
                auto result = interpreter->result;

                ASSERT(result->results.size() != 0);

                // we are going to sort the list, this is done for merging, but
                // being we have one partition in this test we won't actually be merging.
                result->makeSortedList();

                // the merger was made to merge a fancy result structure, we
                // are going to manually stuff our result into this
                std::vector<openset::result::ResultSet*> resultSets;

                // populate or vector of results, so we can merge
                //responseData.push_back(&res);
                resultSets.push_back(interpreter->result);

                // this is the merging object, it merges results from multiple
                // partitions into a result that can serialized to JSON, or to
                // binary for distributed queries
                openset::result::ResultMuxDemux merger;

                // we are going to populate this
                cjson resultJSON;

                // make some JSON
                //auto rows = merger.mergeResultSets(queryMacros.vars.columnVars.size(), 1, resultSets);
                //merger.mergeMacroLiterals(queryMacros, resultSets);
                //auto text = merger.mergeResultText(resultSets);
                merger.resultSetToJson(queryMacros.vars.columnVars.size(), 1, resultSets, &resultJSON);

                // sort descending on second property (1)
                merger.jsonResultSortByColumn(&resultJSON, openset::result::ResultSortOrder_e::Desc, 1);

                // NOTE - uncomment if you want to see the results
                // cout << cjson::stringify(&resultJSON, true) << endl;

                ASSERTDEBUGLOG(interpreter->debugLog);

                auto underScoreNode = resultJSON.xPath("/_");
                ASSERT(underScoreNode != nullptr);

                auto dataNodes = underScoreNode->getNodes();
                ASSERT(dataNodes.size() == 2);

                auto totalsNode1 = dataNodes[0]->xPath("/c");
                auto values1 = cjson::stringify(totalsNode1);

                auto totalsNode2 = dataNodes[1]->xPath("/c");
                auto values2 = cjson::stringify(totalsNode2);

                ASSERT(values1 == "[1,3,3,600]" && values2 == "[1,2,2,200]");
            }
        },
        {
            "test_count_methods: date count", [=]
            {
                auto database = openset::globals::database;

                auto table = openset::globals::database->getTable("__testcountmethods__");
                auto parts = table->getPartitionObjects(0, true); // partition zero for test

                openset::query::Macro_s queryMacros; // this is our compiled code block
                openset::query::QueryParser p;

                // compile this
                p.compileQuery(test1_pyql.c_str(), table->getProperties(), queryMacros);
                ASSERT(p.error.inError() == false);

                // count using time stamp based row identifies (allows for multiple rows to be treated as one)
                queryMacros.useStampedRowIds = true;

                // mount the compiled query to an interpretor
                auto interpreter = new openset::query::Interpreter(queryMacros);

                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.createCustomer("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the properties in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries

                Customer customer; // Customer overlay for personRaw;
                customer.mapTable(table.get(), 0, mappedColumns);

                customer.mount(personRaw); // this tells the customer object where the raw compressed data is
                customer.prepare(); // this actually decompresses

                                  // this mounts the now decompressed data (in the customer overlay)
                                  // into the interpreter
                interpreter->mount(&customer);

                // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);

                // just getting a pointer to the results for nicer readability
                auto result = interpreter->result;

                ASSERT(result->results.size() != 0);

                // we are going to sort the list, this is done for merging, but
                // being we have one partition in this test we won't actually be merging.
                result->makeSortedList();

                // the merger was made to merge a fancy result structure, we
                // are going to manually stuff our result into this
                std::vector<openset::result::ResultSet*> resultSets;

                // populate or vector of results, so we can merge
                //responseData.push_back(&res);
                resultSets.push_back(interpreter->result);

                // this is the merging object, it merges results from multiple
                // partitions into a result that can serialized to JSON, or to
                // binary for distributed queries
                openset::result::ResultMuxDemux merger;

                // we are going to populate this
                cjson resultJSON;

                // make some JSON
                //auto rows = merger.mergeResultSets(queryMacros.vars.columnVars.size(), 1, resultSets);
                //merger.mergeMacroLiterals(queryMacros, resultSets);
                //auto text = merger.mergeResultText(resultSets);
                merger.resultSetToJson(queryMacros.vars.columnVars.size(), 1, resultSets, &resultJSON);

                // sort descending on third property (2)
                merger.jsonResultSortByColumn(&resultJSON, openset::result::ResultSortOrder_e::Desc, 2);

                // NOTE - uncomment if you want to see the results
                //cout << cjson::stringify(&resultJSON, true) << endl;

                ASSERTDEBUGLOG(interpreter->debugLog);

                auto underScoreNode = resultJSON.xPath("/_");
                ASSERT(underScoreNode != nullptr);

                auto dataNodes = underScoreNode->getNodes();
                ASSERT(dataNodes.size() == 2);

                auto totalsNode1 = dataNodes[0]->xPath("/c");
                auto values1 = cjson::stringify(totalsNode1);

                auto totalsNode2 = dataNodes[1]->xPath("/c");
                auto values2 = cjson::stringify(totalsNode2);

                ASSERT(values1 == "[1,1,3,200]" && values2 == "[1,1,2,100]");
            }
        },
*/
    };

}
