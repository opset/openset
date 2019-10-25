#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/properties.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryinterpreter.h"
#include "../src/internoderouter.h"

#include "test_helper.h"

#include <unordered_set>
#include "../src/queryindexing.h"

// Our tests
inline Tests test_db()
{
    // An array of JSON events to insert.
    auto user1_raw_inserts =
        R"raw_inserts(
    [
        {
            "id": "user1@test.com",
            "stamp": 1458820830,
            "event": "page_view",
            "page": "blog"
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820840,
            "event": "page_view",
            "page": "home page",
            "referral_source": "google.co.uk",
            "referral_search": ["big", "floppy", "slippers"],
            "prop_set": ["orange", "huge"],
            "prop_txt": "rabbit",
            "prop_int": 543,
            "prop_float": 543.21
            "prop_bool": false,
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820841,
            "event": "page_view",
            "page": "home page",
            "referral_source": "google.co.uk",
            "referral_search": ["silly", "floppy", "ears"]
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820900,
            "event": "page_view",
            "page": "about"
        }
    ]
    )raw_inserts";


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

    return {
        {
            "db: create and prepare a table",
            [=]
            {
                // prepare our table
                auto table = openset::globals::database->newTable("__test001__", false);

                // add some properties
                auto columns = table->getProperties();
                ASSERT(columns != nullptr);

                // content (adding to 2000 range, these typically auto enumerated on create)
                columns->setProperty(2000, "page", PropertyTypes_e::textProp, false);
                // referral (adding to 3000 range)
                columns->setProperty(3000, "referral_source", PropertyTypes_e::textProp, false);
                columns->setProperty(3001, "referral_search", PropertyTypes_e::textProp, true);


                columns->setProperty(4000, "prop_set", PropertyTypes_e::textProp, true, true);
                columns->setProperty(4001, "prop_txt", PropertyTypes_e::textProp, false, true);
                columns->setProperty(4002, "prop_bool", PropertyTypes_e::boolProp, false, true);
                columns->setProperty(4003, "prop_int", PropertyTypes_e::intProp, false, true);
                columns->setProperty(4004, "prop_float", PropertyTypes_e::doubleProp, false, true);

                // do we have 10 properties (7 built ins plus 3 we added)
                ASSERT(table->getProperties()->propertyCount == 13);

                // built-ins
                ASSERT(table->getProperties()->nameMap.count("id"));

                // properties we've added
                ASSERT(table->getProperties()->nameMap.count("page"));
                ASSERT(table->getProperties()->nameMap.count("referral_source"));
                ASSERT(table->getProperties()->nameMap.count("referral_search"));
                //auto names = table.getProperties()->nameMap();
            }
        },
        {
            "db: add events to user",
            [=]()
            {
                auto table = openset::globals::database->getTable("__test001__");
                ASSERT(table != nullptr);

                auto parts = table->getPartitionObjects(0, true); // partition zero for test
                ASSERT(parts != nullptr);

                auto personRaw = parts->people.createCustomer("user1@test.com");
                ASSERT(personRaw != nullptr);
                ASSERT(personRaw->getIdStr() == "user1@test.com");
                ASSERT(personRaw->id == MakeHash("user1@test.com"));
                ASSERT(personRaw->bytes == 0);
                ASSERT(personRaw->linId == 0); // first user in this partition should be zero

                Customer person; // Customer overlay for personRaw;

                person.mapTable(table.get(), 0); // will throw in DEBUG if not called before mount
                person.mount(personRaw);

                // parse the user1_raw_inserts raw JSON text block
                cjson insertJSON(user1_raw_inserts, cjson::Mode_e::string);

                // get vector of cjson nodes for each element in root array
                auto events = insertJSON.getNodes();

                for (auto e : events)
                {
                    ASSERT(e->xPathInt("/stamp", 0) != 0);
                    person.insert(e);
                }

                // write back any dirty change bits from the insert
                parts->attributes.clearDirty();

                auto grid = person.getGrid();

                auto json = grid->toJSON(); // non-condensed

                // NOTE - uncomment if you want to see the results
                //cout << cjson::stringify(&json, true) << endl;

                std::unordered_set<int64_t> timeStamps;
                std::unordered_set<std::string> referral_sources;
                std::unordered_set<std::string> referral_searches;
                std::unordered_set<std::string> pages;

                auto rows = json.xPath("events");

                ASSERT(rows != nullptr);

                auto rowVector = rows->getNodes();

                ASSERT(rowVector.size() == 4);

                for (auto r : rowVector)
                {
                    if (r->find("stamp"))
                        timeStamps.insert(r->xPath("stamp")->getInt());

                    auto attr = r->xPath("_");

                    if (attr->find("referral_source"))
                        referral_sources.insert(attr->xPath("referral_source")->getString());
                    if (attr->find("referral_search"))
                    {
                        auto rsNodes = attr->xPath("referral_search")->getNodes();
                        for (auto n : rsNodes)
                            referral_searches.insert(n->getString());
                    }
                    if (attr->find("page"))
                        pages.insert(attr->xPath("page")->getString());
                }

                ASSERT(timeStamps.size() == 4);
                ASSERT(referral_sources.size() == 1);
                ASSERT(referral_searches.size() == 5);
                ASSERT(pages.size() == 3);

                // store this customer
                person.commit();

                const auto attr = parts->attributes.get(4000, "huge");
                ASSERT(attr != nullptr);
                const auto bits = attr->getBits();
                ASSERT(bits != nullptr);
                const auto pop = bits->population(parts->people.customerCount());
                ASSERT(pop == 1);

            }
        },
        {
            "db: iterate a Set column in row",
            []
            {
                const auto testScript =
                R"osl(
                    select
                        count id
                        count session
                        count page
                        count referral_source
                    end

                    prop_set = set()

                    # set some props
                    prop_set = prop_set + 'hello'
                    prop_set = prop_set + 'goodbye'

                    prop_txt = 'poodle'
                    prop_bool = true
                    prop_int = 123
                    prop_float = 123.456

                    counter = 0

                    # referral_search is nil in two rows, the `for` loop should skip those
                    # even if we don't put a `&& referral_search.row(!= nil)` in the `each_row`

                    each_row where page.is(!= nil) #
                        log(stamp, page, referral_search)
                        for ref in referral_search
                            counter = counter + 1
                            << id, referral_source, ref
                        end
                    end
                    debug(counter == 6)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                // clear dirty bits (set by props)
                interpreter->interpreter->attrs->clearDirty();

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 1);
                ASSERTDEBUGLOG(debug);

                auto json = ResultToJson(interpreter);

                const auto underScoreNode = json.xPath("/_");
                ASSERT(underScoreNode != nullptr);

                auto dataNodes = underScoreNode->getNodes();
                ASSERT(dataNodes.size() == 1);

                const auto totalsNode = dataNodes[0]->xPath("/c");
                const auto values     = cjson::stringify(totalsNode);

                ASSERT(values == "[1,1,2,2]");

                cout << cjson::stringify(&json, true) << endl;

                delete interpreter;
            }
        },

        {
            "db: are props still set",
            []
            {
                const auto testScript =
                R"osl(

                    if 'hello' in prop_set
                      debug(true)
                    end

                    if prop_txt == 'poodle'
                      debug(true)
                    end

                    if prop_bool == true
                      debug(true)
                    end

                    if prop_int == 123
                      debug(true)
                    end

                    if prop_float == 123.456
                      debug(true)
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                // clear dirty bits (set by props)
                interpreter->interpreter->attrs->clearDirty();

                const auto database = openset::globals::database;
                const auto table    = database->getTable("__test001__");
                const auto parts = table->getPartitionObjects(0, true); // partition zero for test

                auto attr = interpreter->interpreter->attrs->get(4000, "hello");
                ASSERT(attr != nullptr);
                auto bits = attr->getBits();
                ASSERT(bits != nullptr);
                auto pop = bits->population(parts->people.customerCount());
                ASSERT(pop == 1);

                attr = interpreter->interpreter->attrs->get(4000, "huge");
                ASSERT(attr == nullptr);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 5);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },

        {
            "db: iterate a Set column in row",
            []
            {
                const auto testScript =
                R"osl(
                    select
                        count id
                        count page
                    end

                    each_row.reverse().limit(1) where page == 'home page'
                        match_stamp = stamp

                        each_row.continue().next().reverse().within(10_seconds, match_stamp)
                            where event == "page_view"
                          << 'test1', 'home_page', page
                        end
                    end

                    each_row.reverse().limit(1) where page == 'home page'
                        match_stamp = stamp

                        each_row.continue().next().reverse().within(100_seconds, match_stamp)
                            where event == "page_view"
                          << 'test2', 'home_page', page
                        end
                    end
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                auto json = ResultToJson(interpreter);

                const auto underScoreNode = json.xPath("/_");
                ASSERT(underScoreNode != nullptr);

                /* This test runs two nearly identical matches.
                 *
                 * The difference the `iter_within` timing, in "test1" it checks
                 * within 10 seconds, and there can be only one match.
                 *
                 * In "test2" it checks within 100 seconds and there are two matches.
                 *
                 * The results are sorted, so the second test shows up first.
                 *
                 * On the root "_" node the first "c" should be [1,2]
                 * In the second row "c" should be [1,1]
                 *
                 * Note: you can see it by uncommenting the stringify above)
                 */

                auto dataNodes = underScoreNode->getNodes();
                ASSERT(dataNodes.size() == 2);

                auto totalsNode = dataNodes[0]->xPath("/c");
                auto values     = cjson::stringify(totalsNode);
                ASSERT(values == "[1,2]");

                totalsNode = dataNodes[1]->xPath("/c");
                values     = cjson::stringify(totalsNode);
                ASSERT(values == "[1,1]");


                cout << cjson::stringify(&json, true) << endl;

                delete interpreter;
            }
        },
        {
            "db: index compiler basic",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page.is(!= "blog")
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 3);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                delete interpreter;
            }
        },
        {
            "db: index compiler cull session",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page.is(!= "blog") && session.is(== 2)
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 3);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                delete interpreter;
            }
        },
        {
            "db: index compiler cull user variable",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    some_var = 4

                    each_row where page.is(!= "blog") && session.is(== 2) && some_var == 4
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 3);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                delete interpreter;
            }
        },
        {
            "db: index compiler culling gong show",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    some_var = 4
                    other_var = 1

                    third_var = "hello"

                    # the following if will be excluded because it doesn't have a table column in it's logic
                    if (some_var == -1)
                        third_var = "good-bye"
                    end

                    each_row where (other_var == 1 || page.is(!= "blog")) && (session.is(== 2) && (some_var == 4 || other_var == 2))
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 3);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                delete interpreter;
            }
        },

        {
            "db: index compiler row, ever, never",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page.is(!= "blog") || (page.never(=="blog") && referral_search.ever(contains "red"))
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 11);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[3].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[3].value == "page"s);

                ASSERT(queryMacros.index[4].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[4].value == "blog"s);

                ASSERT(queryMacros.index[5].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[6].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[6].value == "referral_search"s);

                ASSERT(queryMacros.index[7].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[7].value == "red"s);

                ASSERT(queryMacros.index[8].op == openset::query::HintOp_e::EQ);

                ASSERT(queryMacros.index[9].op == openset::query::HintOp_e::BIT_AND);

                ASSERT(queryMacros.index[10].op == openset::query::HintOp_e::BIT_OR);

                delete interpreter;
            }
        },

        {
            "db: index compiler with table vars not using row, ever, never",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page != "blog" || (page == "blog" && referral_search contains "red")
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 11);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[3].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[3].value == "page"s);

                ASSERT(queryMacros.index[4].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[4].value == "blog"s);

                ASSERT(queryMacros.index[5].op == openset::query::HintOp_e::EQ);

                ASSERT(queryMacros.index[6].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[6].value == "referral_search"s);

                ASSERT(queryMacros.index[7].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[7].value == "red"s);

                ASSERT(queryMacros.index[8].op == openset::query::HintOp_e::EQ);

                ASSERT(queryMacros.index[9].op == openset::query::HintOp_e::BIT_AND);

                ASSERT(queryMacros.index[10].op == openset::query::HintOp_e::BIT_OR);

                delete interpreter;
            }
        },

        {
            "db: index compiler basic with prop (index count of 1)",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page.is(!= "blog") && prop_set contains 'hello'
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 7);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[3].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[3].value == "prop_set"s);

                ASSERT(queryMacros.index[4].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[4].value == "hello");

                ASSERT(queryMacros.index[5].op == openset::query::HintOp_e::EQ);

                ASSERT(queryMacros.index[6].op == openset::query::HintOp_e::BIT_AND);

                const auto database = openset::globals::database;
                const auto table    = database->getTable("__test001__");
                const auto parts = table->getPartitionObjects(0, true); // partition zero for test

                const auto maxLinearId = parts->people.customerCount();

                openset::query::Indexing indexing;
                indexing.mount(table.get(), queryMacros, 0, maxLinearId);

                bool countable;
                const auto index      = indexing.getIndex("_", countable);
                const auto population = index->population(maxLinearId);

                ASSERT(population == 1);

                delete interpreter;
            }
        },
        {
            "db: index compiler basic with prop (index count of 0 - deleted item)",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page.is(!= "blog") && prop_set contains 'orange'
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 7);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[3].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[3].value == "prop_set"s);

                ASSERT(queryMacros.index[4].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[4].value == "orange"); // should be replaced and de-indexed

                ASSERT(queryMacros.index[5].op == openset::query::HintOp_e::EQ);

                ASSERT(queryMacros.index[6].op == openset::query::HintOp_e::BIT_AND);

                const auto database = openset::globals::database;
                const auto table    = database->getTable("__test001__");
                const auto parts = table->getPartitionObjects(0, true); // partition zero for test

                const auto maxLinearId = parts->people.customerCount();

                openset::query::Indexing indexing;
                indexing.mount(table.get(), queryMacros, 0, maxLinearId);

                bool countable;
                const auto index      = indexing.getIndex("_", countable);
                const auto population = index->population(maxLinearId);

                ASSERT(population == 0);

                delete interpreter;
            }
        },

        {
            "db: index compiler basic with prop (not equal and not equal)",
            []
            {
                const auto testScript =
                R"osl(

                    select
                        count id
                    end

                    each_row where page != "blog" && prop_txt != 'rabbit'
                        << page
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();

                ASSERT(queryMacros.index.size() == 7);

                ASSERT(queryMacros.index[0].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[0].value == "page"s);

                ASSERT(queryMacros.index[1].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[1].value == NONE);

                ASSERT(queryMacros.index[2].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[3].op == openset::query::HintOp_e::PUSH_TBL);
                ASSERT(queryMacros.index[3].value == "prop_txt"s);

                ASSERT(queryMacros.index[4].op == openset::query::HintOp_e::PUSH_VAL);
                ASSERT(queryMacros.index[4].value == NONE); // should be replaced and de-indexed

                ASSERT(queryMacros.index[5].op == openset::query::HintOp_e::NEQ);

                ASSERT(queryMacros.index[6].op == openset::query::HintOp_e::BIT_AND);

                const auto database = openset::globals::database;
                const auto table    = database->getTable("__test001__");
                const auto parts = table->getPartitionObjects(0, true); // partition zero for test

                const auto maxLinearId = parts->people.customerCount();

                openset::query::Indexing indexing;
                indexing.mount(table.get(), queryMacros, 0, maxLinearId);

                bool countable;
                const auto index      = indexing.getIndex("_", countable);
                const auto population = index->population(maxLinearId);

                // index is super broad because it contains `not equals` conditions (against properties/props)
                ASSERT(population == 1);

                delete interpreter;
            }
        },

    };
}
