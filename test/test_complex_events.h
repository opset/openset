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
inline Tests test_complex_events()
{
	// An array of JSON events to insert. 
	auto user1_raw_inserts = R"raw_inserts(
	[
		{
			"person": "user1_@test.com",
			"stamp": 1458820830,
			"action" : "purchase",
			"attr":{				
				"total": 237.50,
				"tax": 11.22,
				"shipping": 7.85,
				"shipper": "fedex",
				"status": "pending",
				"items": 2,
				"_": [
					{
						"product_name": "grommet",
						"product_price": 94.74,
						"product_tag": ["red", "small", "rubber"],
						"product_group": ["kitchen", "bathroom"]
					},
					{
						"product_name": "shag rug",
						"product_price": 27.99,
						"product_tag": ["red", "shaggy", "retro"],
						"_": [
							{ 
							"product_group": "bedroom"
							},
							{ 
							"product_group": "bathroom"
							}
						]
					}
				]
			}
		}
	]
	)raw_inserts";

	auto test1_pyql = openset::query::QueryParser::fixIndent(R"pyql(
	agg:
		count person
		sum product_price distinct product_name
		count product_name distinct product_name
		count product_tag distinct product_tag
		value product_name as pname
		var bogus << 1

	match:
		tally(person, product_group, product_tag, product_name)
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
					"complex_events: insert complex data", [user1_raw_inserts]
				{
					auto database = openset::globals::database;

					// prepare our table
					auto table = database->newTable("__test002__");

					// add some columns
					auto columns = table->getColumns();
					ASSERT(columns != nullptr);
										
					int col = 1000;

					// Add event column - TODO: make a primary column type
					columns->setColumn(++col, "total", columnTypes_e::doubleColumn, false, 0);
					columns->setColumn(++col, "tax", columnTypes_e::doubleColumn, false, 0);
					columns->setColumn(++col, "shipping", columnTypes_e::doubleColumn, false, 0);
					columns->setColumn(++col, "shipper", columnTypes_e::textColumn, false, 0);
					columns->setColumn(++col, "product_name", columnTypes_e::textColumn, false, 0);
					columns->setColumn(++col, "product_price", columnTypes_e::doubleColumn, false, 0);
					columns->setColumn(++col, "product_tag", columnTypes_e::textColumn, false, 0);
					columns->setColumn(++col, "product_group", columnTypes_e::textColumn, false, 0);

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
					"complex_events: query complex data test 1", [test1_pyql]
				{
					auto database = openset::globals::database;

					auto table = database->getTable("__test002__");
					auto parts = table->getPartitionObjects(0); // partition zero for test				

					openset::query::Macro_s queryMacros; // this is our compiled code block
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
					ASSERT(p.error.inError() == false);

					// just getting a pointer to the results for nicer readability
					auto result = interpreter->result;

					ASSERT(result->results.size() != 0);

                    result->setAccTypesFromMacros(queryMacros);

					// we are going to sort the list, this is done for merging, but
					// being we have one partition in this test we won't actually be merging.
					result->makeSortedList();

					// the merger was made to merge a fancy result structure, we
					// are going to manually stuff our result into this
					vector<openset::result::ResultSet*> resultSets;

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
                    merger.resultSetToJson(queryMacros.vars.columnVars.size(), 1, resultSets, &resultJSON);
                    /*
					auto rows = merger.mergeResultSets(queryMacros.vars.columnVars.size(), 1, resultSets);
                    merger.mergeMacroLiterals(queryMacros, resultSets);
					auto text = merger.mergeResultText(resultSets);
					merger.resultSetToJSON(queryMacros, table, &resultJSON, rows, text);
                    */

					// NOTE - uncomment if you want to see the results
					//cout << cjson::Stringify(&resultJSON, true) << endl;

					auto underScoreNode = resultJSON.xPath("/_");

					auto dataNodes = underScoreNode->getNodes();

					auto totalsNode = dataNodes[0]->xPath("/c");
					auto values = cjson::Stringify(totalsNode);

					ASSERT(values == "[1,122.7300000,2,5,\"shag rug\",1]");
				}
				}
		};
}
