#pragma once
#include "testing.h"
#include "../lib/cjson/cjson.h"
#include "../lib/var/var.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/columns.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/internoderouter.h"
#include "../src/result.h"
#include "test_helper.h"
#include <unordered_set>

// Our tests
inline Tests test_osl_language()
{
    // An array of JSON events to insert.
    auto user1_raw_inserts =
        R"raw_inserts(
    [
        {
            "id": "user1_@test.com",
            "stamp": 1458820830,
            "event" : "purchase",
            "_":{
                "fruit": "orange",
                "price": 5.55
            }
        },
        {
            "id": "user1_@test.com",
            "stamp": 1458820831,
            "event" : "purchase",
            "_":{
                "fruit": "apple",
                "price": 9.95
            }
        },
        {
            "id": "user1_@test.com",
            "stamp": 1458820832,
            "event" : "purchase",
            "_":{
                "fruit": "pear",
                "price": 12.49
            }
        },
        {
            "id": "user1_@test.com",
            "stamp": 1458820833,
            "event" : "purchase",
            "_":{
                "fruit": "banana",
                "price": 2.49
            }
        },
        {
            "id": "user1_@test.com",
            "stamp": 1458820834,
            "event" : "purchase",
            "_":{
                "fruit": "orange",
                "price": 5.55
            }
        }
    ]
    )raw_inserts";

    auto filler = ""s;

    /* In order to make the engine start there are a few required objects as
        * they will get called in the background during testing:
        *
        *  - cfg::manager must exist // cfg::initConfig)
        *  - __AsyncManager must exist // new OpenSet::async::AyncPool(...)
        *  - Database must exist // databases contain tables
        *
        *  These objects will be created on the heap, although in practice during
        *  the construction phase these are created as local objects to other classes.
        */
    return {
        {
            "test_pyql_language: insert test data",
            [user1_raw_inserts]
            {
                auto database = openset::globals::database;        // prepare our table
                auto table    = database->newTable("__test003__"); // add some columns
                auto columns  = table->getColumns();

                ASSERT(columns != nullptr);

                int col = 1000;
                columns->setColumn(++col, "fruit", columnTypes_e::textColumn, false, false);
                columns->setColumn(++col, "price", columnTypes_e::doubleColumn, false, false);

                auto parts     = table->getPartitionObjects(0, true); // partition zero for test
                auto personRaw = parts->people.getMakePerson("user1@test.com");
                Person person;

                // Person overlay for personRaw;
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
                // cout << cjson::Stringify(&json, true) << endl;
                // store this person
                person.commit();
            }
        },

        {
            "test OSL basic assign and multiply",
            []
            {
                const auto testScript =
                R"osl(
                    test_value = 123
                    new_value = test_value * 2
                    debug(test_value == 123)
                    debug(new_value == 246)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 2);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },
        {
            "test OSL basic containers",
            []
            {
                const auto testScript =
                R"osl(
                    test_value = ["apple", "pear", "orange"]
                    debug(test_value[0] == "apple")
                    debug(test_value[1] != "apple")
                    debug(test_value[2] == "orange")
                    debug(len(test_value) == 3)
                    debug("apple" in test_value)
                    debug((test_value contains "donkey") == false)
                    debug(test_value contains ["apple", "pear"])
                    debug((test_value contains ["apple", "duck"]) == false)
                    debug(test_value any ["donkey", "apple", "bear"])
                    debug((test_value any ["donkey", "duck", "bear"]) == false)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 10);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },
        {
            "test OSL basic dictionary",
            []
            {
                const auto testScript =
                R"osl(
                    test_value = {
                        fruits: ["apple", "orange", "pear", "banana"],
                        animals: ["zebra", "unicorn", "donkey"],
                        a_boolean: true
                    }

                    debug(len(test_value) == 3)
                    debug(len(test_value["fruits"]) == 4)
                    debug(test_value["animals"][1] == "unicorn")

                    test_value["animals"][1] == "dog"
                    debug(test_value["animals"][1] == "unicorn")

                    for key in test_value
                       debug(key in ["fruits", "animals", "a_boolean"])
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 7);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },
        {
            "test OSL basic logic",
            []
            {
                const auto testScript =
                R"osl(

                    test_value = 123
                    some_list = ["apple", "orange", "pear", "banana"]

                    if test_value == 123
                       debug(true)
                    end

                    if test_value != 321
                       debug(true)
                    end

                    if test_value == 123 && ("peach" in some_list || "apple" in some_list)
                       debug(true)
                    end

                    if "peach" in some_list || "plum" in some_list
                       debug(true)
                    end

                    some_list = ["dog", ["cat", "tiger"], "hamster"]

                    if some_list[1][0] == "cat" && (id == 1 + 2 && "apple" == fruit) && fruit.never(== "pear") &&
                           fruit == (4 + ((7*2) / 3)) && test_value == 123
                        debug(true)
                    end

                    if fruit in ["apple", "orange"] || ["banana", "peach", "pumpkin"] contains id
                        debug(true)
                    end


                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },
        {
            "test OSL each",
            []
            {
                const auto testScript =
                R"osl(

                    each_row where fruit.is(== "banana") && fruit.ever(== "donkey")
                        debug(true)
                    end

                    each_row where fruit.is(== "banana") && fruit.ever(== "pear")
                        debug(true)
                    end

                    each_row where fruit.is(== "banana") && fruit.never(== "pear")
                        debug(true)
                    end

                    each_row where fruit.is(== "banana")
                        debug(true)
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 2);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },

        {
            "test OSL break and continue",
            []
            {
                const auto testScript =
                R"osl(

                    source_list = ["one", "two", "three", "four", "five", "six", "seven"]

                    debug(len(source_list) == 7)

                    counter = 0
                    for item in source_list
                       counter = counter + 1
                       if counter == 3
                          break
                       end
                    end

                    debug(counter == 3)

                    counter = 0
                    after_count = 0
                    for item in source_list
                       counter = counter + 1
                       if counter >= 3
                          continue
                       end
                       after_count = after_count + 1
                    end

                    debug(counter == 7)
                    debug(after_count == 2)

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },

        {
            "test OSL break with depth",
            []
            {
                const auto testScript =
                R"osl(

                    number_list = ["one", "two", "three", "four", "five", "six", "seven"]
                    letter_list = ["a", "b", "c", "d"]

                    debug(len(number_list) == 7)
                    debug(len(letter_list) == 4)

                    counter = 0
                    for number in number_list

                      for letter in letter_list
                        if number == "three" && letter == "c"
                          break(2)
                        end
                        counter = counter + 1
                      end

                    end

                    debug(counter == 10)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 3);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },


        {
            "test OSL each_row with limit",
            []
            {
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.limit(2) where event == "purchase"
                      counter = counter + 1
                    end

                    debug(counter == 2)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 1);
                ASSERTDEBUGLOG(debug);

                delete interpreter;
            }
        },

        {
            "test OSL each_row .range",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.range("2016-03-24T12:00:30+00:00", "2016-03-24T12:00:32+00:00") where event == "purchase"
                      counter = counter + 1
                      debug(stamp)
                    end

                    debug(counter == 3)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERT(debug[0] < debug[2]);
                ASSERT(debug[3] == true);

                delete interpreter;
            }
        },

        {
            "test OSL each_row .range .reverse",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.reverse().range("2016-03-24T12:00:30+00:00", "2016-03-24T12:00:32+00:00") where event == "purchase"
                      counter = counter + 1
                      debug(stamp)
                    end

                    debug(counter == 3)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERT(debug[0] > debug[2]);
                ASSERT(debug[3] == true);

                delete interpreter;
            }
        },

        {
            "test OSL each_row .continue (no advance)",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.limit(1) where event.is(== "purchase")
                      each_row.continue() where event.is(== "purchase")
                        counter = counter + 1
                        debug(stamp)
                      end
                    end

                    debug(counter == 5)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 6);
                ASSERT(debug[5] == true);

                delete interpreter;
            }
        },

        {
            "test OSL each_row .continue .next (with advance)",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.limit(1) where event.is(== "purchase")
                      each_row.continue().next() where event.is(== "purchase")
                        counter = counter + 1
                        debug(stamp)
                      end
                    end

                    debug(counter == 4)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 5);
                ASSERT(debug[4] == true);

                delete interpreter;
            }
        },

        {
            "test OSL each_row .from",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(

                    counter = 0

                    each_row.from(2) where event.is(== "purchase")
                      counter = counter + 1
                      debug(stamp)
                    end

                    debug(counter == 3)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 4);
                ASSERT(debug[3] == true);

                delete interpreter;
            }
        },

        {
            "test OSL containers",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(
                    someVar = "3.14"
                    debug(someVar == 3.14)

                    someDict = {
                        "hello": "goodbye",
                        "many": [1,2,3,4]
                    }

                    someDict = someDict + {"another": "thing"}

                    debug(someDict["hello"] == "goodbye")
                    debug(someDict["many"][1] == 2)
                    debug(someDict["another"] == "thing")

                    debug(len(someDict) == 3)

                    someDict = someDict - ["hello", "many"]
                    debug(len(someDict) == 1)

                    someSet = set()
                    someSet = someSet + "hello"
                    someSet = someSet + "goodbye"
                    someSet = someSet + "what"
                    someSet = someSet + "hello"

                    debug(len(someSet) == 3)

                    someSet = someSet - "hello"
                    debug(len(someSet) == 2)
                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 8);
                ASSERTDEBUGLOG(debug);

                delete interpreter;            }
        },

        {
            "test OSL containers and operators",
            []
            {

                // date ranges are inclusive
                const auto testScript =
                R"osl(
                    someDict = {
                        "hello": "goodbye",
                        "many": [1,2,3,4]
                    }

                    someDict = someDict + {"fresh": "prince"}

                    debug(len(someDict) == 3)

                    otherDict = {"objective": "apples"} + {"hunt": "red october"}

                    debug(len(otherDict) == 2)

                    otherDict = otherDict + {"angels": "sang"}
                    log(otherDict)

                    debug(len(otherDict) == 3)

                    someDict = someDict - "hello"

                    debug(len(someDict) == 2)

                    someDict["cheese"] = {
                        "orange" : ["chedder"],
                        "soft": ["mozza", "cream"]
                    }

                    someDict["cheese"] = someDict["cheese"] - "orange"

                    debug(len(someDict["cheese"]) == 1)

                    some_string = "merry"
                    some_string = some_string + " new year"

                    debug(some_string == "merry new year")

                    otherDict["angels"] = otherDict["angels"] + " in awe"

                    debug(otherDict["angels"] == "sang in awe")

                    some_set = set("one", "two", "three")

                    debug(len(some_set) == 3)

                    some_set = some_set - "two"

                    debug(len(some_set) == 2)

                    nested = {}
                    nested['yellow'] = {}
                    nested['yellow']['green'] = 'this is green'

                    debug(nested['yellow']['green'] == 'this is green')

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 10);
                ASSERTDEBUGLOG(debug);

                delete interpreter;            }
        },

        {
            "test OSL number functions",
            []
            {
                // date ranges are inclusive
                const auto testScript =
                R"osl(
                    debug(round(33.544,2) == 33.54)
                    debug(round(8.3854,2) == 8.39)
                    debug(round(12.4912,2) == 12.49)
                    debug(round(5.545,2) == 5.55)

                    debug(bucket(513, 25) == 500)
                    debug(bucket(525, 25) == 525)
                    debug(bucket(551, 25) == 550)
                    debug(bucket(5.11, 0.25) == 5.00)
                    debug(bucket(5.25, 0.25) == 5.25)
                    debug(bucket(5.51, 0.25) == 5.50)

                    debug(fix(0.01111, 2) == "0.01")
                    debug(fix(0.015, 2) == "0.02")
                    debug(fix(1234.5678, 2) == "1234.57")
                    debug(fix(1234.5678, 0) == "1235")
                    debug(fix(-0.01111, 2) == "-0.01")
                    debug(fix(-0.015, 2) == "-0.02")
                    debug(fix(-1234.5678, 2) == "-1234.57")
                    debug(fix(-1234.5678, 0) == "-1235")

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog();
                ASSERT(debug.size() == 18);
                ASSERTDEBUGLOG(debug);

                delete interpreter;            }
        },
    };
}

/*
     # test slicing lists
    some_array = ['zero', 'one', 'two', 'three', 'four', 'five']

    new_array = some_array[1:3]
    # 1
    debug(len(new_array) == 2 and new_array[0] == 'one' and new_array[1] == 'two')

    new_array = some_array[:2]
    # 2
    debug(len(new_array) == 2 and new_array[0] == 'zero' and new_array[1] == 'one')

    new_array = some_array[2:]
    # 3
    debug(len(new_array) == 4 and new_array[0] == 'two')

    new_array = some_array[:]
    # 4
    debug(len(new_array) == 6 and new_array[0] == 'zero' and new_array[5] == 'five')

    new_array = some_array[-1:]
    # 5
    debug(len(new_array) == 1 and new_array[0] == 'five')

    new_array = some_array[-3:-2]
    # 6
    debug(len(new_array) == 1 and new_array[0] == 'three')

    # test slicing strings
    some_string = 'the rain in spain'

    new_string = some_string[-5:]
    # 7
    debug(new_string == 'spain')

    new_string = some_string[:3]
    # 8
    debug(new_string == 'the')

    new_string = some_string[4:8]
    # 9
    debug(new_string == 'rain')

    # test find and rfind
    index = some_string.find('rain')
    # 10
    debug(index == 4)

    index = some_string.find('teeth')
    # 11
    debug(index == -1)

    index = some_string.find('in', 8)
    # 12
    debug(index == 9)

    index = some_string.rfind('in', 0)
    # 13
    debug(index == 15)

    index = some_string.rfind('the')
    # 14
    debug(index == 0)

    index = some_string.rfind('rain', 8)
    # 15
    debug(index == 4)

    index = some_string.find('rain', 0, 7)
    # 16
    debug(index == -1)

    # test split
    some_string = 'see spot run'
    parts = some_string.split(' ')
    # 17
    debug(parts[0] == 'see' and parts[1] == 'spot' and parts[2] == 'run')

    some_string = 'this::is::fun'
    parts = some_string.split('::')
    # 18
    debug(parts[0] == 'this' and parts[1] == 'is' and parts[2] == 'fun')

    some_string = "this won't split"
    parts = some_string.split('|')
    # 19
    debug(parts[0] == some_string)

    # test strip

    some_string = '\t  this is a string \r\n'
    clean = some_string.strip()
    # 20
    debug(clean == 'this is a string')

    some_string = "\t \n \r"
    clean = some_string.strip()
    # 21
    debug(clean == '')

    some_url = "http://somehost.com/this/is/the/path?param1=one&param2=two&param3"
    parts = url_decode(some_url)

    # 22
    debug(parts['host'] == 'somehost.com')
    # 23
    debug(parts['path'] == '/this/is/the/path')
    # 24
    debug(parts['query'] == 'param1=one&param2=two&param3')
    # 25
    debug(len(parts['params']) == 3)
    # 26
    debug(parts['params']['param1'] == 'one')
    # 27
    debug(parts['params']['param2'] == 'two')
    # 28
    debug(parts['params']['param3'] == True)

    some_url = "/this/is/the/path?param1=one"
    parts = url_decode(some_url)
    # 29
    debug(parts['host'] == None)
    # 30
    debug(parts['path'] == '/this/is/the/path')
    # 31
    debug(len(parts['params']) == 1)
    # 32
    debug(parts['params']['param1'] == 'one')

    some_url = "/this/is/the/path"
    parts = url_decode(some_url)
    # 34
    debug(parts['host'] == None)
    # 35
    debug(parts['path'] == '/this/is/the/path')
    # 36
    debug(len(parts['params']) == 0)

    )pyql");
    // test sdk functions (1)
    auto test18_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

    # bucket always rounds to the lower bucket
    # it is useful when generating distributions

    test = {
        "favorite_bands": set("the hip", "run dmc"),
        "toothpaste": ["crest", "colgate", "arm and hammer"],
        "age": 44
    }

    test["age"] = 45

    debug(test['age'] == 45)

    test["favorite_bands"] += "ABBA"

    debug("ABBA" in test['favorite_bands'])

    test["toothpaste"] = ["none", "water"]

    debug("crest" not in test['toothpaste'])

    part = test['toothpaste']

    part += "sand"

    debug("sand" not in test['toothpaste'])

    # pyql at this time does not use references so
    # changed sub-objects must be reassigned to the
    # parent object

    test['toothpaste'] = part

    debug("sand" in test['toothpaste'])

    # log(test)

 */
