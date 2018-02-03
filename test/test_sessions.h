#pragma once

#include "testing.h"

#include "../lib/cjson/cjson.h"
#include "../lib/str/strtools.h"
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
inline Tests test_sessions()
{
	
	// An array of JSON events to insert, we are going to 
	// insert these out of order and count on zorder to
	// sort them.
	// we will set zorder for action to "alpha", "beta", "cappa", "delta", "echo"
	auto user1_raw_inserts = R"raw_inserts(
	[
		{
			"uuid": "user1@test.com",
			"stamp": 1458800000,
			"action": "some event",
			"_":{				
				"some_val": 100,
	            "some_str": "rabbit"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458800100,
			"action": "some event",
			"_":{				
				"some_val": 101,
	            "some_str": "train"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458800200,
			"action": "some event",
			"_":{				
				"some_val": 102,
	            "some_str": "cat"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1545220000,
			"action": "some event",
			"_":{				
				"some_val": 103,
	            "some_str": "dog"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1545220100,
			"action": "some event",
			"_":{				
				"some_val": 104,
	            "some_str": "cat"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1545220900,
			"action": "some event",
			"_":{				
				"some_val": 105,
	            "some_str": "rabbit"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1631600000,
			"action": "some event",
			"_":{				
				"some_val": 106,
	            "some_str": "train"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1631600400,
			"action": "some event",
			"_":{				
				"some_val": 107,
	            "some_str": "plane"
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1631601200,
			"action": "some event",
			"_":{				
				"some_val": 108,
	            "some_str": "automobile"
			}
		},
	]
	)raw_inserts";
	
	auto test1_pyql = openset::query::QueryParser::fixIndent(R"pyql(
	agg:
		count person
		count session
        count some_val

	match:
		tally("all", some_str)
		if session == 2:
			debug(true)

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

	auto database = new Database();

	return {
		{
			"test_sessions: create and prepare a table", [database, user1_raw_inserts] {

				// prepare our table
				auto table = database->newTable("__test004__");

				// add some columns
				auto columns = table->getColumns();
				ASSERT(columns != nullptr);

				// content (adding to 2000 range, these typically auto enumerated on create)
				columns->setColumn(2000, "some_val", openset::db::columnTypes_e::intColumn, false);
				columns->setColumn(2001, "some_str", openset::db::columnTypes_e::textColumn, false);
			
				auto parts = table->getPartitionObjects(0); // partition zero for test
				auto personRaw = parts->people.getmakePerson("user1@test.com");

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
			"test_sessions: loop", [test1_pyql]
			{
				auto database = openset::globals::database;

				auto table = database->getTable("__test004__");
				auto parts = table->getPartitionObjects(0); // partition zero for test				

				openset::query::Macro_s queryMacros; // this is our compiled code block
				openset::query::QueryParser p;

				// compile this
				p.compileQuery(test1_pyql.c_str(), table->getColumns(), queryMacros);
				ASSERT(p.error.inError() == false);

				// mount the compiled query to an interpretor
				auto interpreter = new openset::query::Interpreter(queryMacros);

				openset::result::ResultSet resultSet(queryMacros.vars.columnVars.size());
				interpreter->setResultObject(&resultSet);

				auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
				ASSERT(personRaw != nullptr);
				auto mappedColumns = interpreter->getReferencedColumns();

				// MappedColumns? Why? Because the basic mapTable function (without a 
				// columnList) maps all the columns in the table - which is what we want when 
				// inserting or updating rows but means more processing and less data affinity
				// when performing queries

				Person person; // Person overlay for personRaw;
				person.mapTable(table.get(), 0, mappedColumns);

				person.mount(personRaw); // this tells the person object where the raw compressed data is
				person.prepare(); // this actually decompresses

								  // this mounts the now decompressed data (in the person overlay)
								  // into the interpreter
				interpreter->mount(&person);

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

				// NOTE - uncomment if you want to see the results
				//cout << cjson::Stringify(&resultJSON, true) << endl;

				ASSERTDEBUGLOG(interpreter->debugLog);

				auto underScoreNode = resultJSON.xPath("/_");
				ASSERT(underScoreNode != nullptr);

				auto dataNodes = underScoreNode->getNodes();
				ASSERT(dataNodes.size() == 1);

				auto totalsNode = dataNodes[0]->xPath("/c");
				auto values = cjson::stringify(totalsNode);

				ASSERT(values == "[1,3,9]");

			}
		},

	};

}
