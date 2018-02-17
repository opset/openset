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
inline Tests test_zorder()
{
	
	// An array of JSON events to insert, we are going to 
	// insert these out of order and count on zorder to
	// sort them.
	// we will set zorder for action to "alpha", "beta", "cappa", "delta", "echo"
	auto user1_raw_inserts = R"raw_inserts(
	[
		{
			"uuid": "user1@test.com",
			"stamp": 1458820830,
			"action": "delta",
			"attr":{				
				"some_val": 4
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820830,
			"action": "cappa",
			"attr":{				
				"some_val": 3
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820830,
			"action": "beta",
			"attr":{				
				"some_val": 2
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820830,
			"action": "alpha",
			"attr":{				
				"some_val": 1
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820830,
			"action": "beta",
			"attr":{				
				"some_val": 2222
			}
		},

		{
			"uuid": "user1@test.com",
			"stamp": 1458820840,
			"action": "delta",
			"attr":{				
				"some_val": 4
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820840,
			"action": "cappa",
			"attr":{				
				"some_val": 3
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820840,
			"action": "beta",
			"attr":{				
				"some_val": 2
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "alpha",
			"attr":{				
				"some_val": 1
			}
		},

		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "delta",
			"attr":{				
				"some_val": 4
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "cappa",
			"attr":{				
				"some_val": 3
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "beta",
			"attr":{				
				"some_val": 2
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "alpha",
			"attr":{				
				"some_val": 2
			}
		},
		{
			"uuid": "user1@test.com",
			"stamp": 1458820820,
			"action": "echo",
			"attr":{				
				"some_val": 5
			}
		},
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
			"z-order: test event z-order", [=] {

				// prepare our table
				auto table = openset::globals::database->newTable("__testzorder__");

				// add some columns
				auto columns = table->getColumns();
				ASSERT(columns != nullptr);

				// content (adding to 2000 range, these typically auto enumerated on create)
				columns->setColumn(2000, "some_val", openset::db::columnTypes_e::intColumn, false, 0);

				auto zOrderStrings = table->getZOrderStrings();
				auto zOrderInts = table->getZOrderHashes();
				
				// add zOrdering
				zOrderStrings->emplace("alpha", 0);
				zOrderInts->emplace(MakeHash("alpha"), 0);

				zOrderStrings->emplace("beta", 1);
				zOrderInts->emplace(MakeHash("beta"), 1);

				zOrderStrings->emplace("cappa", 2);
				zOrderInts->emplace(MakeHash("cappa"), 2);
				
				auto parts = table->getPartitionObjects(0, true); // partition zero for test
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
					ASSERT(e->xPath("/attr") != nullptr);

					person.insert(e);
				}

				auto grid = person.getGrid();
				auto json = grid->toJSON(); // non-condensed

				// TODO - finish test

			}
		}
	};

}
