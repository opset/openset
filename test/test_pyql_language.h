#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../lib/var/var.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/columns.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryinterpreter.h"
#include "../src/queryparser.h"
#include "../src/internoderouter.h"
#include "../src/result.h"

#include <unordered_set>

// Our tests
inline Tests test_pyql_language()
{
	// An array of JSON events to insert. 
	auto user1_raw_inserts = R"raw_inserts(
	[
		{
			"person": "user1_@test.com",
			"stamp": 1458820830,
			"action" : "purchase", 
			"attr":{				
				"fruit": "orange",
				"price": 5.55
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820831,
			"action" : "purchase", 
			"attr":{
				"fruit": "apple",
				"price": 9.95
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820832,
			"action" : "purchase", 
			"attr":{
				"fruit": "pear",
				"price": 12.49
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820833,
			"action" : "purchase", 
			"attr":{
				"fruit": "banana",
				"price": 2.49
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820834,
			"action" : "purchase", 
			"attr":{
				"fruit": "orange",
				"price": 5.55
			}
		}
	]
	)raw_inserts";

	// test loop
	auto test1_pyql = fixIndent(R"pyql(
	agg:
		count person

	counter = 0

	match:
		tally(person)
		counter = counter + 1

	debug(counter)

	)pyql");

	// test loop with break
	auto test2_pyql = fixIndent(R"pyql(
	agg:
		count person

	counter = 0

	match:
		tally(person)
		counter = counter + 1
		break

	debug(counter)

	)pyql");

	// test nested loop with breaks
	auto test3_pyql = fixIndent(R"pyql(
	agg:
		count person

	outercount = 0
	innercount = 0

	match:
		tally(person)
		match:

			tally(person)
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
	auto test4_pyql = fixIndent(R"pyql(
	agg:
		count person

	outercount = 0
	innercount = 0

	match:
		# push(__group, person)
		match:

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
	auto test5_pyql = fixIndent(R"pyql(
	agg:
		count person

	outercount = 0
	innercount = 0

	match:
		tally(person)
		match:
			tally(person)
			match:
				tally(person)
				innercount = innercount + 1
				break top

		outercount = outercount + 1

	debug(outercount)  # should be 4
	debug(innercount)  # should be 4

	)pyql");

	// test nested loops, 'break all'
	auto test6_pyql = fixIndent(R"pyql(
	agg:
		count person

	outercount = 0
	innercount = 0

	match:
		tally(person)
		match:
			tally(person)
			match:
				tally(person)
				innercount = innercount + 1
				break all

		outercount = outercount + 1

	debug(outercount)  # should be 0
	debug(innercount)  # should be 1

	)pyql");

	// test nested loops, 'continue'
	auto test7_pyql = fixIndent(R"pyql(
	agg:
		count person

	outercount = 0
	innercount = 0

	match:
		tally(person)
	    # log(__group, " level 1")
		match:
			tally(person)
			# log(__group, " level 2")
			match:
				tally(person)
				# log(__group, " level 3")
				innercount = innercount + 1			

		continue

		outercount = outercount + 1

	debug(outercount)  # should be 0
	debug(innercount)  # should be 35

	)pyql");

	// test nested loops, 'break ###' to deep
	auto test8_pyql = fixIndent(R"pyql(
	agg:
		count person

	match:
		tally(person)
		match:
			tally(person)
			match:
				tally(person)
				break 9

	)pyql");

	// test event manipulators
	auto test9_pyql = fixIndent(R"pyql(
	agg:
		count person

	debug(event_count()) # should be 5

	counter = 0

	iter_next() # advance one row leaving 4
		
	match:
		counter = counter + 1

	debug(counter); # should be 4

	)pyql");

	// test over advance
	auto test10_pyql = fixIndent(R"pyql(
	agg:
		count person

	counter = 0
	
	match:
		iter_next()
		counter = counter + 1
		iter_next()
		counter = counter + 1
		iter_next()
		counter = counter + 1
		iter_next()
		counter = counter + 1	
		iter_next() # should fail
		counter = counter + 1	

	debug(counter) # should be 4
	
	)pyql");

	// test over advance - silent mainloop exit
	auto test11_pyql = fixIndent(R"pyql(
	agg:
		count person

	count = 0
	
	iter_next()
	debug(1)
	iter_next()
	debug(2)
	iter_next()
	debug(3)
	iter_next()
	debug(4)
	iter_next()
	debug(5)

	# should exit at 4

	)pyql");

	// test container types
	auto test12_pyql = fixIndent(R"pyql(
	
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
	auto test13_pyql = fixIndent(R"pyql(

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

	)pyql");


	// test container iterators
	auto test14_pyql = fixIndent(R"pyql(

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
	auto test15_pyql = fixIndent(R"pyql(

	capture_stuff( 1 + 2, DISTINCT fruit where \
		fruit is not 'banana', "rain" + " in " + "spain")

	capture_stuff2( 1 + 2, (3 + 4) / 2.0, DISTINCT fruit where \
		fruit is not 'banana' and (2 + 2 == 4))

	def capture_stuff(junk1, the_sum, junk2):
		debug(the_sum == 3)

	def capture_stuff2(junk1, junk2, the_sum):
		debug(the_sum == 3)

	test_sum = SUM price where \
		fruit is not 'banana'

	test_avg = AVG price where \
		fruit is not 'banana'

	test_max = MAX price where \
		fruit is not 'banana'

	test_min = MIN price where \
		fruit is not 'banana'

	test_count = COUNT fruit where \
		fruit is not 'banana'

	test_distinct = DISTINCT fruit where \
		fruit is not 'banana'

	test_distinct2 = DISTINCT fruit

	debug(round(test_sum,2) == 33.54)
	debug(round(test_avg,2) == 8.39)
	debug(round(test_max,2) == 12.49)
	debug(round(test_min,2) == 5.55)
	debug(test_count == 4)
	debug(test_distinct == 3)
	debug(test_distinct2 == 4)

	)pyql");

	// test sdk functions (1)
	auto test16_pyql = fixIndent(R"pyql(

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
					"test_pyql_language: test parser helper functions", [] {

						using namespace openset::query;
						auto escapeing_and_brackets_in_text = 
							R"raw(this "is ('some text' \") \\ \"\t'" other '\"\'[()]\'")raw";
						auto parts = QueryParser::breakLine(escapeing_and_brackets_in_text);
						ASSERT(parts.size() == 4);

						auto goodBrackets = "this[that[((thing{that}){more})(here[there]{everywhere})]]";
						parts = QueryParser::breakLine(goodBrackets);
						ASSERT(QueryParser::checkBrackets(parts));

						auto badBrackets = "this[that[((thing{that}{more})(here[there]{everywhere})]]";
						parts = QueryParser::breakLine(badBrackets);
						ASSERT(!QueryParser::checkBrackets(parts)); // returns false

						auto testLineMiddle = "somevar = this['is']['a'][container['nested']] + blah"s;
						parts = QueryParser::breakLine(testLineMiddle);
						ASSERT(parts.size() == 17);

						int reinsertIdx;
						auto capture = QueryParser::extractVariable(
							parts, 2, reinsertIdx);

						ASSERT(reinsertIdx == 2);
						ASSERT(capture.size() == 13);
						ASSERT(parts.size() == 4);

						// back track from last ] and capture var and deref
						parts = QueryParser::breakLine(testLineMiddle);

						capture = QueryParser::extractVariableReverse(
							parts, 14, reinsertIdx);

						ASSERT(reinsertIdx == 2);
						ASSERT(capture.size() == 13);
						ASSERT(parts.size() == 4);
				}
				},
				{
					"test_pyql_language: insert test data", [user1_raw_inserts]
				{
					auto database = openset::globals::database;

					// prepare our table
					auto table = database->newTable("__test003__");

					// add some columns
					auto columns = table->getColumns();
					ASSERT(columns != nullptr);

					int col = 1000;

					// Add event column - TODO: make a primary column type
					columns->setColumn(++col, "event", columnTypes_e::textColumn, false, 0);
					columns->setColumn(++col, "fruit", columnTypes_e::textColumn, false, 0);
					columns->setColumn(++col, "price", columnTypes_e::doubleColumn, false, 0);

					auto parts = table->getPartitionObjects(0); // partition zero for test
					auto personRaw = parts->people.getmakePerson("user1@test.com");

					Person person; // Person overlay for personRaw;

					person.mapTable(table, 0); // will throw in DEBUG if not called before mount
					person.mount(personRaw);

					// parse the user1_raw_inserts raw JSON text block
					cjson insertJSON(user1_raw_inserts, strlen(user1_raw_inserts));

					// get vector of cjson nodes for each element in root array
					auto events = insertJSON.getNodes();

					for (auto e : events)
					{
						ASSERT(e->xPathInt("/stamp", 0) != 0);
						ASSERT(e->xPath("/attr") != nullptr);

						person.insert(e);
					}

					auto grid = person.getGrid();
					auto json = grid->toJSON(false); // non-condensed

					// NOTE - uncomment if you want to see the results
					// cout << cjson::Stringify(&json, true) << endl;

					// store this person
					person.commit();
				}
				},
				{
					"test_pyql_language: loop", [test1_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test1_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 1);
					ASSERT(debug->at(0) == 5);
				}
				},
				{
					"test_pyql_language: break in loop", [test2_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test2_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 1);
					ASSERT(debug->at(0) == 1);
				}
				},
				{
					"test_pyql_language: breaks in nested loops", [test3_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test3_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 2);
					ASSERT(debug->at(1) == 4);
				}
				},
				{
					"test_pyql_language: nested loops break with depth", [test4_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test4_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 0);
					ASSERT(debug->at(1) == 2);
				}
				},
				{
					"test_pyql_language: nested loops with 'break top'", [test5_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test5_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 5);
					ASSERT(debug->at(1) == 5);
				}
				},
				{
					"test_pyql_language: nested loops with 'break all'", [test6_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test6_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 0);
					ASSERT(debug->at(1) == 1);
				}
				},
				{
					"test_pyql_language: nested loops with 'continue'", [test7_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test7_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 0);
					ASSERT(debug->at(1) == 35);
				}
				},
				{
					"test_pyql_language: nested loops with 'break ##' too-deep error", [test8_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test8_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == true);
				}
				},
				{
					"test_pyql_language: event manipulators", [test9_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test9_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 2);
					ASSERT(debug->at(0) == 5);
					ASSERT(debug->at(1) == 4);
				}
				},
				{
					"test_pyql_language: test over advance", [test10_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test10_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 1);
					ASSERT(debug->at(0) == 4);
				}
				},
				{
					"test_pyql_language: test over advance - mainloop silient exit", [test11_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test11_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					// cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

									  // this mounts the now decompressed data (in the person overlay)
									  // into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERT(interpreter->error.inError() == false);

					auto debug = &interpreter->debugLog;

					ASSERT(debug->size() == 4);					
				}
				},
				{
					"test_pyql_language: test var and container", [test12_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test12_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					//cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

					// this mounts the now decompressed data (in the person overlay)
					// into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());
					
					ASSERT(interpreter->debugLog.size() == 8);
					ASSERT(testAllTrue(interpreter->debugLog));
				}
				},
				{
					"test_pyql_language: test member conversion", [test13_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test13_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERT(p.error.inError() == false);

					//cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

									  // this mounts the now decompressed data (in the person overlay)
									  // into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());

					ASSERT(interpreter->debugLog.size() == 9);
					ASSERT(testAllTrue(interpreter->debugLog));
				}
				},
				{
					"test_pyql_language: test container iterators", [test14_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test14_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

					//cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

									  // this mounts the now decompressed data (in the person overlay)
									  // into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());

					ASSERT(interpreter->debugLog.size() == 22);
					ASSERT(testAllTrue(interpreter->debugLog));
				}
			},
			{
				"test_pyql_language: test inline accumulators", [test15_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test15_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

					//cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

									  // this mounts the now decompressed data (in the person overlay)
									  // into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());

					ASSERT(interpreter->debugLog.size() == 9);
					ASSERT(testAllTrue(interpreter->debugLog));
				}
			},
			{
				"test_pyql_language: test inline accumulators", [test16_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test003__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::macro_s queryMacros; // this is our compiled code block
					openset::query::QueryParser p;

					// compile this
					p.compileQuery(test16_pyql.c_str(), table->getColumns(), queryMacros);
					ASSERTMSG(p.error.inError() == false, p.error.getErrorJSON());

					//cout << OpenSet::query::MacroDbg(queryMacros) << endl;

					// mount the compiled query to an interpretor
					auto interpreter = new openset::query::Interpreter(queryMacros);

					openset::result::ResultSet resultSet;
					interpreter->setResultObject(&resultSet);

					auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
					ASSERT(personRaw != nullptr);
					auto mappedColumns = interpreter->getReferencedColumns();

					// MappedColumns? Why? Because the basic mapTable function (without a 
					// columnList) maps all the columns in the table - which is what we want when 
					// inserting or updating rows but means more processing and less data affinity
					// when performing queries

					Person person; // Person overlay for personRaw;
					person.mapTable(table, 0, mappedColumns);

					person.mount(personRaw); // this tells the person object where the raw compressed data is
					person.prepare(); // this actually decompresses

									  // this mounts the now decompressed data (in the person overlay)
									  // into the interpreter
					interpreter->mount(&person);

					// run it
					interpreter->exec();
					ASSERTMSG(interpreter->error.inError() == false, interpreter->error.getErrorJSON());

					ASSERT(testAllTrue(interpreter->debugLog));
				}
			}
	};
}
