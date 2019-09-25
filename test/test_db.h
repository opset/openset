#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/columns.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryinterpreter.h"
#include "../src/internoderouter.h"

#include "test_helper.h"

#include <unordered_set>

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
            "_":{
                "page": "blog"
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820840,
            "event": "page_view",
            "_":{
                "page": "home page",
                "referral_source": "google.co.uk",
`				"referral_search": ["big", "floppy", "slippers"]
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820841,
            "event": "page_view",
            "_":{
                "page": "home page",
                "referral_source": "google.co.uk",
                "referral_search": ["silly", "floppy", "ears"]
            }
        },
        {
            "id": "user1@test.com",
            "stamp": 1458820900,
            "event": "page_view",
            "_":{
                "page": "about"
            }
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
                auto table = openset::globals::database->newTable("__test001__");

                // add some columns
                auto columns = table->getColumns();
                ASSERT(columns != nullptr);

                // content (adding to 2000 range, these typically auto enumerated on create)
                columns->setColumn(2000, "page", columnTypes_e::textColumn, false);
                // referral (adding to 3000 range)
                columns->setColumn(3000, "referral_source", columnTypes_e::textColumn, false);
                columns->setColumn(3001, "referral_search", columnTypes_e::textColumn, true);

                // do we have 10 columns (7 built ins plus 3 we added)
                ASSERT(table->getColumns()->columnCount == 10);

                // built-ins
                ASSERT(table->getColumns()->nameMap.count("__triggers"));
                ASSERT(table->getColumns()->nameMap.count("id"));
                ASSERT(table->getColumns()->nameMap.count("__emit"));

                // columns we've added
                ASSERT(table->getColumns()->nameMap.count("page"));
                ASSERT(table->getColumns()->nameMap.count("referral_source"));
                ASSERT(table->getColumns()->nameMap.count("referral_search"));
                //auto names = table.getColumns()->nameMap();
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

                auto personRaw = parts->people.getMakePerson("user1@test.com");
                ASSERT(personRaw != nullptr);
                ASSERT(personRaw->getIdStr() == "user1@test.com");
                ASSERT(personRaw->id == MakeHash("user1@test.com"));
                ASSERT(personRaw->bytes == 0);
                ASSERT(personRaw->linId == 0); // first user in this partition should be zero

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

                // NOTE - uncomment if you want to see the results
                //cout << cjson::stringify(&json, true) << endl;

                std::unordered_set<int64_t> timeStamps;
                std::unordered_set<std::string> referral_sources;
                std::unordered_set<std::string> referral_searches;
                std::unordered_set<std::string> pages;

                auto rows = json.xPath("rows");

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

                // store this person
                person.commit();
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

                    if ('test' in props) == false
                        props['test'] = {}
                    end

                    # set some props
                    props['test']['this'] = 'hello'
                    some_var = props['test']['this']

                    props['fav_beers'] = set('cold', 'free')
                    props['opposites'] = {
                        'bows': 'arrows',
                        'up': 'down',
                        'inside': 'outside'
                    }

                    log(props)

                    counter = 0

                    # referral_search is nil in two rows, the `for` loop should skip those
                    # even if we don't put a `&& referral_search.row(!= nil)` in the `each_row`

                    each_row where page.row(!= nil) #
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

                    if 'test' in props
                      debug(true)
                    end

                    if 'this' in props['test']
                      debug(true)
                    end

                    if 'cold' in props['fav_beers']
                      debug(true)
                    end

                    log(props)

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test001__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 3);
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

    };
}
