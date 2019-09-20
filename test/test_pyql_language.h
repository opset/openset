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
inline Tests test_pyql_language()
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

    // test loop
    auto test1_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

    select
      count id as customer_id
      count session
      count fruit
      count event
      max price as max_price
      min price as min_price
      sum price as total
      avg price as avg_price
    end

    total_spend_red_outdoor = sum(total * quantity).within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")

    some_row = row.reverse().within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")  

    is_red_outdoor = test.reverse().within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")

    count_red_outdoor = count(product).within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")

    max_quantity_red_outdoor = max(quantity).within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")

    avg_quantity_red_outdoor = avg(quantity).within(1_year) where catalog.row(== "outdoor") && tag.row(contains "red")

    if fruit.ever(contains test_set)
        << "blah"
    end

    if fruit.ever(any test_set)
        << "blah"
    end

    if fruit.never(in test_set) == false
        << "blah"
    end

    some_bool = true
    some_bool = false
    some_bool = nil

    each.range(fromStart, from_end) where ( ( fruit.ever(== "Germany") && teeth == "yellow" ) || fruit.ever(== "tomato") )
        << true
    end

    test_set = set("pig", "goat", "donkey", "mule", "horse")

    if.range( (1234 + (22 / 3)) , 4567) (id.ever(== "klara") + 1) == (test - 2)
        << "blah"
    end

    test_list = [123, "test", now(), (2+2), 4+4, now(345), [4,5,6]]

    test = 4
    x = (id + 1) == (test - 2)

    now(id)
    now(total, "money")
  
	counter = 0

    some_value = 23 + (((45 * 72) / 3) - 2) * 3) - 1

    test = "some (string with <stuff>> in, it"

    some_value = test_list[0]
    some_value = test_list[6][1]

    empty_list = []

    empty_list[4] = "blah"

    test_dict = {
       blah: [123,456,789],
       foo: {
          bar: now(),
          eat: "food"
       }
    }

    test_dict[blah][0] = 234

    empty_dict = {}

    t = 5_ms
    t = 5_seconds
    t = 5_minutes
    t = 5_hours
    t = 5_days
    t = 5_weeks
    t = 5_months
    t = 5_years
   
    if.within(3_months, now(1234)) id.ever(== "test") || (id.ever(== true) && frog == "green") || frog == "red" && (4 * (34 + 23 / (10 / 2))) == 55
       bogus = 10 
       << "total"
    end

    if id.ever(== "seth") 
      test = this
    end

    if id == "tommy"
      x = now()
    end

    if id.within(3_months, from_start).ever(== "test")
        monkey = true != false
        << "total", id
    end

	for row in rows
		<< id
		counter = counter + 1
    end

	debug(counter)

	)pyql");
    // test loop with break
    auto test2_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	counter = 0

	for row in rows:
		tally(id)
		counter = counter + 1
		break

	debug(counter)

	)pyql");
    // test nested loop with breaks
    auto test3_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	outercount = 0
	innercount = 0

	for row in rows:

		tally(id)

		continue for sub_row_1 in rows:

			tally(id)
			innercount = innercount + 1
			if innercount == 2 or innercount == 4:
				break

		outercount = outercount + 1
		if outercount == 2:
			break

	debug(outercount)  # should be 2
	debug(innercount)  # should be 4

	)pyql");
    // test nested loops, break with depth
    auto test4_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	outercount = 0
	innercount = 0

	for row in rows:

		continue for sub_row_1 in rows:

			tally(person)
			innercount = innercount + 1
			if innercount == 2 or innercount == 4:
				break 2

		outercount = outercount + 1

		if outercount == 2:
			break

	debug(outercount)  # should be 0
	debug(innercount)  # should be 2

	)pyql");
    // test nested loops, 'break top'
    auto test5_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	outercount = 0
	innercount = 0

	for row in rows:
		tally(id)

		continue for sub_row_1 in rows:
			tally(id)

			continue for sub_row_2 in rows:

				tally(id)

				innercount = innercount + 1
				break top

		outercount = outercount + 1

	debug(outercount)  # should be 3
	debug(innercount)  # should be 5

	)pyql");
    // test nested loops, 'break all'
    auto test6_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	outercount = 0
	innercount = 0

	for row in rows:
		tally(id)

		continue for sub_row_1 in rows:
			tally(id)

			continue for sub_row_2 in rows:
				tally(id)
				innercount = innercount + 1
				break all

		outercount = outercount + 1

	debug(outercount)  # should be 0
	debug(innercount)  # should be 1

	)pyql");
    // test nested loops, 'continue'
    auto test7_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	outercount = 0
	innercount = 0

	for row in rows:
		tally(id)
	    # log(__group, " level 1")

		continue for sub_row_1 in rows:
			tally(id)
			# log(__group, " level 2")

			continue for sub_row_2 in rows:
				tally(id)
				# log(__group, " level 3")
				innercount = innercount + 1

		continue

		outercount = outercount + 1

	debug(outercount)  # should be 0
	debug(innercount)  # should be 10

	)pyql");
    // test nested loops, 'break ###' to deep
    auto test8_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	for row in rows:
		tally(id)

		continue for sub_row_1 in rows:
			tally(id)

			continue for sub_row_2 in rows:
				tally(id)
				break 9

	)pyql");
    // test event manipulators
    auto test9_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	debug(row_count()) # should be 5

	counter = 0

	for row in rows:
		counter = counter + 1

	debug(counter); # should be 5

	)pyql");
    // test over advance
    auto test10_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	counter = 0

	for 2 row in rows:
		counter = counter + 1

	debug(counter) # should be 2

	)pyql");
    // test over advance - silent mainloop exit
    auto test11_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(
	agg:
		count id

	counter = 0

	for 1 row in rows:

        continue for sub_row in rows:
		    counter = counter + 1

	debug(counter) # should exit at 4

	)pyql");
    // test container types
    auto test12_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

	someVar = "3.14"
	debug(someVar == 3.14)

	someDict = {
		"hello": "goodbye",
		"many": [1,2,3,4]
	}

	someDict = someDict + {"another": "thing"}

    debug(someDict["hello"] == "goodbye")
	debug(someDict["many"][1] is 2)
	debug(someDict["another"] == "thing")

	debug(len(someDict) == 3)

	someDict = someDict - ["hello", "many"]
	debug(len(someDict) == 1)

	someSet = set()
	someSet = someSet + "hello"
	someSet = someSet + "goodbye"
    someSet = someSet + "what"
	someSet = someSet + "hello"

	# should be three, as "hello" can only be added once
	debug(len(someSet) == 3)

	someSet = someSet - "hello"
	debug(len(someSet) == 2)

	)pyql");
    // test container type members
    // Note: we are actually testing that they
    // get converted into functions
    auto test13_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

	someDict = {
		"hello": "goodbye",
		"many": [1,2,3,4]
	}

	someDict.append({"fresh": "prince"})

	debug(len(someDict) == 3)

	otherDict = {"objective": "apples"} + {"hunt": "red october"}

	debug(len(otherDict) == 2)

	otherDict += {"angles": "sang"}

	debug(len(otherDict) == 3)

	del someDict["hello"]

	debug(len(someDict) == 2)

	someDict["cheese"] = {
		"orange" : ["chedder"],
		"soft": ["mozza", "cream"]
	}

	del someDict["cheese"]["orange"]

	debug(len(someDict["cheese"]) == 1)

	some_string = "merry"
	some_string += " new year"

	debug(some_string == "merry new year")

	otherDict["angles"] += " in awe"

    debug(otherDict["angles"] == "sang in awe")

	some_set = set("one", "two", "three")

	debug(len(some_set) == 3)

	some_set.remove("two")

	debug(len(some_set) == 2)

    test = {}
    test['yellow'] = {}
    test['yellow']['green'] = 'this is green'
    del test['yellow']['green']
    del test['yellow']['orange']
    del test['yellow']['orange']


	)pyql");
    // test container iterators
    auto test14_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

	someDict = {
		"hello": "goodbye",
		"many": [1,2,3,4],
		"feet": "shoes"
    }

	keys = []
    for k in someDict:
		keys += k

	debug(keys[0] in ['hello', 'many', 'feet'])
    debug(keys[1] in ['hello', 'many', 'feet'])
    debug(keys[2] in ['hello', 'many', 'feet'])

    keys = []
    values = []
    for k,v in someDict:
		keys += k
        values.append(v) # append will push objects like the list

	debug(keys[0] in ['hello', 'many', 'feet'])
    debug(keys[1] in ['hello', 'many', 'feet'])
    debug(keys[2] in ['hello', 'many', 'feet'])

	# debug(values[0] == 'goodbye')
    # debug(values[1][1] == 2)
    # debug(values[2] == 'shoes')

    debug(someDict['many'][1] == 2)

	some_set = set('tree', 'flower', 'mushroom', 'grass')

	if 'spot' in ['see', 'spot', 'run']:
		debug(True)

	keys = []
	for k in some_set:
		keys += k

	debug(len(keys) == 4)
	debug('tree' in keys)
    debug('flower' in keys)
    debug('mushroom' in keys)
    debug('grass' in keys)
    debug('beaver' not in keys)

	some_set = set('one', 'two', 'three')
	thing = some_set.pop()
	debug(len(some_set) == 2)
	debug(thing == 'one' or thing == 'two' or thing == 'three')

	some_list = list('one', 'two', 'three')
	thing = some_list.pop()
	debug(len(some_list) == 2)
	debug(thing == 'three')

	some_dict = {
		"hello": "goodbye",
		"many": [1,2,3,4],
		"feet": "shoes"
    }

	keys = some_dict.keys()
	debug(len(keys) == 3)
	debug(keys[0] in ['hello', 'many', 'feet'])
	debug(keys[1] in ['hello', 'many', 'feet'] and keys[1] is not keys[0] and keys[1] is not keys[2])
	debug(keys[2] in ['hello', 'many', 'feet'])

	)pyql");
    // test inline accumulators `sum/count/avg/min/max where`
    auto test15_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

	capture_stuff( 1 + 2, COUNT DISTINCT fruit if \
		fruit is not 'banana', "rain" + " in " + "spain")

	capture_stuff2( 1 + 2, (3 + 4) / 2.0, COUNT DISTINCT fruit if \
		fruit is not 'banana' and (2 + 2 == 4))

	def capture_stuff(junk1, the_sum, junk2):
		debug(the_sum == 3)

	def capture_stuff2(is3, is35, the_sum):
		debug(the_sum == 3 and is3 == 3 and is35 == 3.5)

	test_sum = SUM price if \
		fruit is not 'banana'

	test_avg = AVG price if \
		fruit is not 'banana'

	test_max = MAX price if \
		fruit is not 'banana'

	test_min = MIN price if \
		fruit is not 'banana'

	test_count = COUNT fruit if \
		fruit is not 'banana'

	test_distinct = COUNT DISTINCT fruit if \
		fruit is not 'banana'

    matched_row = LAST ROW if fruit != "orange"
    row_content = get_row(matched_row) # fix someday - allow get_row(matched_row)['fruit'] without a temp

    debug(matched_row == 3)
    debug(row_content['fruit'] == 'banana')
 
    matched_row = FIRST ROW where fruit != "orange"
    row_content = get_row(matched_row)
    debug(matched_row == 1)
    debug(row_content['fruit'] == 'apple') 

    row_content = get_row(FIRST ROW where fruit == "pear")
    debug(row_content['fruit'] == 'pear') 

    last_fruit = LAST VALUE fruit where fruit != 'orange'
    debug(last_fruit == 'banana')

    first_fruit = FIRST VALUE fruit where fruit != 'orange'
    debug(first_fruit == 'apple')

	test_distinct2 = COUNT DISTINCT fruit

	debug(round(test_sum,2) == 33.54)
	debug(round(test_avg,2) == 8.39)
	debug(round(test_max,2) == 12.49)
	debug(round(test_min,2) == 5.55)
	debug(test_count == 4)
	debug(test_distinct == 3)
	debug(test_distinct2 == 4)

	)pyql");
    // test sdk functions (1)
    auto test16_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

	# bucket always rounds to the lower bucket
	# it is useful when generating distributions

	debug(bucket(513, 25) == 500)
	debug(bucket(525, 25) == 525)
	debug(bucket(551, 25) == 550)
	debug(bucket(5.11, 0.25) == 5.00)
	debug(bucket(5.25, 0.25) == 5.25)
	debug(bucket(5.51, 0.25) == 5.50)

	# fix fixes a floating point number to
    # a rounded set number of decimals and
	# returns a string. Fix is useful for
	# grouping where you likely want
    # a consistent fixed precision group
    # name.

	debug(fix(0.01111, 2) == "0.01")
	debug(fix(0.015, 2) == "0.02")
	debug(fix(1234.5678, 2) == "1234.57")
	debug(fix(1234.5678, 0) == "1235")
	debug(fix(-0.01111, 2) == "-0.01")
	debug(fix(-0.015, 2) == "-0.02")
	debug(fix(-1234.5678, 2) == "-1234.57")
	debug(fix(-1234.5678, 0) == "-1235")

	)pyql");
    // test slicing of strings and arrays
    auto test17_pyql = openset::query::QueryParser::fixIndent(
        R"pyql(

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
    return {
        {
            "test_pyql_language: test parser helper functions",
            []
            {
                using namespace openset::query;
                const auto EscapingAndBracketsInText = R"raw(this "is ('some text' \") \\ \"\t'" other '\"\'[()]\'")raw";
                auto parts = QueryParser::breakLine(EscapingAndBracketsInText);

                ASSERT(parts.size() == 4);
                const auto goodBrackets = "this[that[((thing{that}){more})(here[there]{everywhere})]]";
                parts = QueryParser::breakLine(goodBrackets);
                ASSERT(QueryParser::checkBrackets(parts));
                const auto badBrackets = "this[that[((thing{that}{more})(here[there]{everywhere})]]";
                parts = QueryParser::breakLine(badBrackets);
                ASSERT(!QueryParser::checkBrackets(parts)); // returns false
                const auto testLineMiddle = "somevar = this['is']['a'][container['nested']] + blah"s;
                parts = QueryParser::breakLine(testLineMiddle);
                ASSERT(parts.size() == 17);

                int reinsertIdx;
                auto capture = QueryParser::extractVariable(parts, 2, reinsertIdx);
                ASSERT(reinsertIdx == 2);
                ASSERT(capture.size() == 13);
                ASSERT(parts.size() == 4); // back track from last ] and capture var and deref

                parts   = QueryParser::breakLine(testLineMiddle);
                capture = QueryParser::extractVariableReverse(parts, 14, reinsertIdx);
                ASSERT(reinsertIdx == 2);
                ASSERT(capture.size() == 13);
                ASSERT(parts.size() == 4);
            }
        },
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
                Person person;                   // Person overlay for personRaw;
                person.mapTable(table.get(), 0); // will throw in DEBUG if not called before mount
                person.mount(personRaw);         // parse the user1_raw_inserts raw JSON text block
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

                auto& debug = interpreter->debugLog;
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

                auto& debug = interpreter->debugLog;
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

                auto& debug = interpreter->debugLog;
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

                auto& debug = interpreter->debugLog;
                ASSERT(debug.size() == 5);
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

                    each_row where fruit.row(== "banana") && fruit.ever(== "donkey")
                        debug(true)
                    end

                    each_row where fruit.row(== "banana") && fruit.ever(== "pear")
                        debug(true)
                    end

                    each_row where fruit.row(== "banana") && fruit.never(== "pear")
                        debug(true)
                    end

                    each_row where fruit.row(== "banana")
                        debug(true)
                    end

                )osl"s;

                openset::query::Macro_s queryMacros;
                const auto interpreter = TestScriptRunner("__test003__", testScript, queryMacros, true);

                auto& debug = interpreter->debugLog;
                ASSERT(debug.size() == 2);
                ASSERTDEBUGLOG(debug);

                delete interpreter;                               
            }
        },

        {
            "test_pyql_language: loop",
            [test1_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       

                openset::query::QueryParser2 p2;

                const auto input = std::string(test1_pyql);
                p2.compileQuery(test1_pyql, table->getColumns(), queryMacros, nullptr);
                
                // compile this
                p.compileQuery(test1_pyql.c_str(), table->getColumns(), queryMacros);
                cout << openset::query::MacroDbg(queryMacros) << endl;

                ASSERT(p.error.inError() == false);

                
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 1);
                ASSERT(debug->at(0) == 5);
            }
        },
        {
            "test_pyql_language: break in loop",
            [test2_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this
                p.compileQuery(test2_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: breaks in nested loops",
            [test3_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this
                p.compileQuery(test3_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 2);
                ASSERT(debug->at(1) == 4);
            }
        },
        {
            "test_pyql_language: nested loops break with depth",
            [test4_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this
                p.compileQuery(test4_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 0);
                ASSERT(debug->at(1) == 2);
            }
        },
        {
            "test_pyql_language: nested loops with 'break top'",
            [test5_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test5_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter

                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 5);
                ASSERT(debug->at(1) == 3);
            }
        },
        {
            "test_pyql_language: nested loops with 'break all'",
            [test6_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test6_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 0);
                ASSERT(debug->at(1) == 1);
            }
        },
        {
            "test_pyql_language: nested loops with 'continue'",
            [test7_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test7_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 0);
                ASSERT(debug->at(1) == 10);
            }
        },
        {
            "test_pyql_language: nested loops with 'break ##' too-deep error",
            [test8_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test8_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);

                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == true);
            }
        },
        {
            "test_pyql_language: event manipulators",
            [test9_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test9_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << OpenSet::query::MacroDbg(queryMacros) << endl;

                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 2);
                ASSERT(debug->at(0) == 5);
                ASSERT(debug->at(1) == 5);
            }
        },
        {
            "test_pyql_language: test over advance",
            [test10_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test10_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); // cout << OpenSet::query::MacroDbg(queryMacros) << endl;

                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 1);
                ASSERT(debug->at(0) == 2);
            }
        },
        {
            "test_pyql_language: test over advance - mainloop silient exit",
            [test11_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                
                p.compileQuery(test11_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                // cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERT(interpreter->error.inError() == false);
                auto debug = &interpreter->debugLog;
                ASSERT(debug->size() == 1);
                ASSERT(debug->at(0) == 4);
            }
        },
        {
            "test_pyql_language: test var and container",
            [test12_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this                
                p.compileQuery(test12_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); 
                
                //cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERT(interpreter->debugLog.size() == 8);
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: test member conversion",
            [test13_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this
                p.compileQuery(test13_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERT(p.error.inError() == false); //cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERT(interpreter->debugLog.size() == 9);
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: test container iterators",
            [test14_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       
                
                // compile this
                p.compileQuery(test14_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());
                
                //cout << openset::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter

                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERT(interpreter->debugLog.size() == 22);
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: test inline accumulators",
            [test15_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this

                p.compileQuery(test15_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());
                
                //cout << openset::query::MacroDbg(queryMacros) << endl;

                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries

                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERT(interpreter->debugLog.size() == 16);
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: test inline accumulators",
            [test16_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test

                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test16_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

                //cout << OpenSet::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter
                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: test slicing of lists and strings",
            [test17_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this

                p.compileQuery(test17_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

                //cout << openset::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter

                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);



                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();
                
                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses
                
                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        },
        {
            "test_pyql_language: modify dictionary",
            [test18_pyql]
            {
                auto database = openset::globals::database;
                auto table    = database->getTable("__test003__");
                auto parts    = table->getPartitionObjects(0, true); 
                
                // partition zero for test
                openset::query::Macro_s queryMacros;                 // this is our compiled code block
                openset::query::QueryParser p;                       // compile this
                p.compileQuery(test18_pyql.c_str(), table->getColumns(), queryMacros);
                ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

                // cout << openset::query::MacroDbg(queryMacros) << endl;
                // mount the compiled query to an interpreter

                auto interpreter = new openset::query::Interpreter(queryMacros);
                openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
                interpreter->setResultObject(&resultSet);
                auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
                ASSERT(personRaw != nullptr);
                auto mappedColumns = interpreter->getReferencedColumns();

                // MappedColumns? Why? Because the basic mapTable function (without a
                // columnList) maps all the columns in the table - which is what we want when
                // inserting or updating rows but means more processing and less data affinity
                // when performing queries
                Person person; // Person overlay for personRaw;
                person.mapTable(table.get(), 0, mappedColumns);
                person.mount(personRaw); // this tells the person object where the raw compressed data is
                person.prepare();        // this actually decompresses

                // this mounts the now decompressed data (in the person overlay)
                // into the interpreter
                interpreter->mount(&person); // run it
                interpreter->exec();
                ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
                ASSERTDEBUGLOG(interpreter->debugLog);
            }
        }
    };
}
