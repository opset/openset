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
inline Tests test_db()
{
	
	// An array of JSON events to insert. 
	auto user1_raw_inserts = R"raw_inserts(
	[
		{
			"person": "user1_@test.com",
			"stamp": 1458820830,
			"action": "page_view",
			"attr":{				
				"page": "blog"
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820840,
			"action": "page_view",
			"attr":{				
				"page": "home page",
				"referral_source": "google.co.uk",
				"referral_search": ["big", "floppy", "slippers"]
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820841,
			"action": "page_view",
			"attr":{				
				"page": "home page",
				"referral_source": "google.co.uk",
				"referral_search": ["silly", "floppy", "ears"]
			}
		},
		{
			"person": "user1_@test.com",
			"stamp": 1458820900,
			"action": "page_view",
			"attr":{				
				"page": "about"
			}
		}
	]
	)raw_inserts";
	

	auto test1_pyql = fixIndent(R"pyql(
	agg:
		count person
		count page
		count referral_source
		count referral_search

	match:
		tally(person, page, referral_source, referral_search)
	)pyql");

	auto test_pluggable_pyql = fixIndent(R"pyql(
	agg:
		count person
		count {{attr_page}}
		{{attr_ref}} << 1
		{{attr_keyword}} << 1

	match:
		tally(person, \
             {{attr_page}}, \
             {{attr_ref}}, \
             {{attr_keyword}})
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
			"db: create and prepare a table", [database] {

				// prepare our table
				auto table = database->newTable("__test001__");

				// add some columns
				auto columns = table->getColumns();
				ASSERT(columns != nullptr);

				// content (adding to 2000 range, these typically auto enumerated on create)
				columns->setColumn(2000, "page", openset::db::columnTypes_e::textColumn, false, 0);
				// referral (adding to 3000 range)
				columns->setColumn(3000, "referral_source", openset::db::columnTypes_e::textColumn, false, 0);
				columns->setColumn(3001, "referral_search", openset::db::columnTypes_e::textColumn, false, 0);

				// do we have 9 columns (5 built ins plus 4 we added)
				ASSERT(table->getColumns()->columnCount == 8);
			
				// built-ins
				ASSERT(table->getColumns()->nameMap.count("__triggers"));
				ASSERT(table->getColumns()->nameMap.count("__uuid"));
				ASSERT(table->getColumns()->nameMap.count("__emit"));

				// columns we've added
				ASSERT(table->getColumns()->nameMap.count("page"));
				ASSERT(table->getColumns()->nameMap.count("referral_source"));
				ASSERT(table->getColumns()->nameMap.count("referral_search"));
				//auto names = table.getColumns()->nameMap();

			}
		},
		{
			"db: add events to user", [database, user1_raw_inserts]() {

				auto table = database->getTable("__test001__");
				ASSERT(table != nullptr);

				auto parts = table->getPartitionObjects(0); // partition zero for test
				ASSERT(parts != nullptr);

				auto personRaw = parts->people.getmakePerson("user1@test.com");
				ASSERT(personRaw != nullptr);
				ASSERT(personRaw->getIdStr() == "user1@test.com");
				ASSERT(personRaw->id == MakeHash("user1@test.com"));
				ASSERT(personRaw->bytes == 0);
				ASSERT(personRaw->linId == 0); // first user in this partition should be zero

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
				//cout << cjson::Stringify(&json, true) << endl;

				std::unordered_set<int64_t> timeStamps;
				std::unordered_set<std::string> referral_sources;
				std::unordered_set<std::string> referral_searches;
				std::unordered_set<std::string> pages;

				auto rows = json.xPath("rows");

				ASSERT(rows != nullptr);

				auto rowVector = rows->getNodes();

				ASSERT(rowVector.size() == 4);

				for (auto r: rowVector)
				{				

					if (r->find("stamp"))
						timeStamps.insert(r->xPath("stamp")->getInt());

					auto attr = r->xPath("attr");
					
					vector<cjson*> subRows;

					if (attr->type() == cjsonType::ARRAY)
						subRows = attr->getNodes(); // get the array of sub-nodes
					else
						subRows.push_back(attr); // fill subRows with our one node

					for (auto sr : subRows)
					{
						if (sr->find("referral_source"))
							referral_sources.insert(sr->xPath("referral_source")->getString());
						if (sr->find("referral_search"))
							referral_searches.insert(sr->xPath("referral_search")->getString());
						if (sr->find("page"))
							pages.insert(sr->xPath("page")->getString());
					}
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
			"db: query a user", [database, test1_pyql]() {

				auto table = database->getTable("__test001__");
				auto parts = table->getPartitionObjects(0); // partition zero for test				

				openset::query::macro_s queryMacros; // this is our compiled code block
				openset::query::QueryParser p;

				// compile this
				p.compileQuery(test1_pyql.c_str(), table->getColumns(), queryMacros);
				ASSERT(!p.error.inError());

				// mount the compiled query to an interpretor
				auto interpreter = new openset::query::Interpreter(queryMacros);

				openset::result::ResultSet resultSet;
				interpreter->setResultObject(&resultSet);
				
				auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
				ASSERT(personRaw != nullptr);
				auto mappedColumns = interpreter->getReferencedColumns();
				ASSERT(mappedColumns.size() == 6);

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
				auto rows = merger.mergeResultSets(queryMacros, table, resultSets);
				auto text = merger.mergeText(queryMacros, table, resultSets);
				merger.resultSetToJSON(queryMacros, table, &resultJSON, rows, text);

				// NOTE - uncomment if you want to see the results
				// cout << cjson::Stringify(&resultJSON, true) << endl;

				auto underScoreNode = resultJSON.xPath("/_");
				ASSERT(underScoreNode != nullptr);

				auto dataNodes = underScoreNode->getNodes();
				ASSERT(dataNodes.size() == 1);
				
				auto totalsNode = dataNodes[0]->xPath("/c");				
				auto values = cjson::Stringify(totalsNode);

				ASSERT(values == "[1,4,2,2]");

			}
		},
		{
			"db: query another user", [database, test_pluggable_pyql]() {

				openset::query::ParamVars params;

				params["attr_page"] = "page";
				params["attr_ref"] = "referral_source";
				params["attr_keyword"] = "referral_search";
				params["root_name"] = "root";
				
				auto table = database->getTable("__test001__");
				auto parts = table->getPartitionObjects(0); // partition zero for test				

				openset::query::macro_s queryMacros; // this is our compiled code block
				openset::query::QueryParser p;

				// compile this - passing in the template vars
				p.compileQuery(test_pluggable_pyql.c_str(), table->getColumns(), queryMacros, &params);

				// mount the compiled query to an interpretor
				auto interpreter = new openset::query::Interpreter(queryMacros);

				openset::result::ResultSet resultSet;
				interpreter->setResultObject(&resultSet);

				auto personRaw = parts->people.getmakePerson("user1@test.com"); // get a user			
				ASSERT(personRaw != nullptr);
				auto mappedColumns = interpreter->getReferencedColumns();
				ASSERT(mappedColumns.size() == 6);

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

				//ASSERT(result->results.size() != 0);

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
				cjson resultJSON; // we are going to populate this

  			    // make some JSON
				auto rows = merger.mergeResultSets(queryMacros, table, resultSets);
				auto text = merger.mergeText(queryMacros, table, resultSets);
				merger.resultSetToJSON(queryMacros, table, &resultJSON, rows, text);

				// NOTE - uncomment if you want to see the results
				// cout << cjson::Stringify(&resultJSON, true) << endl;

				auto underScoreNode = resultJSON.xPath("/_");
				ASSERT(underScoreNode != nullptr);

				auto dataNodes = underScoreNode->getNodes();
				ASSERT(dataNodes.size() == 1);

				auto totalsNode = dataNodes[0]->xPath("/c");
				auto values = cjson::Stringify(totalsNode);

				ASSERT(values == "[1,4,2,2]");

			}
		}
	};

}
