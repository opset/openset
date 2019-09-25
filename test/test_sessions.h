#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/columns.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryparser.h"
#include "../src/internoderouter.h"
#include "test_helper.h"

// Our tests
inline Tests test_sessions()
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
                "some_str": "rabbit"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458800100,
            "event": "some event",
            "_":{
                "some_val": 101,
                "some_str": "train"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458800200,
            "event": "some event",
            "_":{
                "some_val": 102,
                "some_str": "cat"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1545220000,
            "event": "some event",
            "_":{
                "some_val": 103,
                "some_str": "dog"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1545220100,
            "event": "some event",
            "_":{
                "some_val": 104,
                "some_str": "cat"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1545220900,
            "event": "some event",
            "_":{
                "some_val": 105,
                "some_str": "rabbit"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1631600000,
            "event": "some event",
            "_":{
                "some_val": 106,
                "some_str": "train"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1631600400,
            "event": "some event",
            "_":{
                "some_val": 107,
                "some_str": "plane"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1631601200,
            "event": "some event",
            "_":{
                "some_val": 108,
                "some_str": "automobile"
            }
        },
    ]
    )raw_inserts";

    auto test1_pyql = openset::query::QueryParser::fixIndent(R"pyql(
    agg:
        count id
        count session
        count some_val

    for row in rows:
        tally("all", row['some_str'])
        if session == 2:
            debug(True)

    debug(session_count == 3)

    )pyql");


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
        {
            "test_sessions: create and prepare a table", [=] {

                // prepare our table
                auto table = openset::globals::database->newTable("__testsessions__");

                // add some columns
                auto columns = table->getColumns();
                ASSERT(columns != nullptr);

                // content (adding to 2000 range, these typically auto enumerated on create)
                columns->setColumn(2000, "some_val", openset::db::columnTypes_e::intColumn, false);
                columns->setColumn(2001, "some_str", openset::db::columnTypes_e::textColumn, false);

                auto parts = table->getPartitionObjects(0, true); // partition zero for test
                auto personRaw = parts->people.getMakePerson("user1@test.com");

                Person person; // Person overlay for personRaw;

                person.mapTable(table.get(), 0); // will throw in DEBUG if not called before mount
                person.mount(personRaw);

                // parse the user1_raw_inserts raw JSON text block
                cjson insertJSON(user1_raw_inserts, cjson::Mode_e::string);

                // get vector of cjson nodes for each element in root array
                auto events = insertJSON.getNodes();

                for (auto e : events)
                {
                    ASSERT(e->xPathInt("/stamp", 0) != 0);
                    ASSERT(e->xPath("/_") != nullptr);

                    person.insert(e);
                }

                auto grid = person.getGrid();
                auto json = grid->toJSON(); // non-condensed

                person.commit();

            }
        },
        {
            "test OSL each_row .from",
            []
            {
                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    select
                      count id
                      count session
                      count some_val
                    end

                    log(cursor)

                    each_row where event.row(== "some event")

                      << "all", some_str

                      log(stamp, session)

                      if session == 2
                        debug(true)
                      end

                    end

                    debug(session_count == 3)

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__testsessions__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERTDEBUGLOG(debug);

                auto json = ResultToJson(interpreter);
                cout << cjson::stringify(&json, true) << endl;

                auto underScoreNode = json.xPath("/_");
                ASSERT(underScoreNode != nullptr);

                auto dataNodes = underScoreNode->getNodes();
                ASSERT(dataNodes.size() == 1);

                auto totalsNode = dataNodes[0]->xPath("/c");
                auto values = cjson::stringify(totalsNode);

                ASSERT(values == "[1,3,9]");

                delete interpreter;
            }
        },
    };
}
