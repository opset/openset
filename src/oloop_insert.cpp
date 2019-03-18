#include "oloop_insert.h"
#include "cjson/cjson.h"
#include "str/strtools.h"

#include "people.h"
#include "person.h"
#include "database.h"
#include "table.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "sidelog.h"
#include "internoderouter.h"

using namespace std;
using namespace openset::async;
using namespace openset::db;

OpenLoopInsert::OpenLoopInsert(openset::db::Database::TablePtr table) :
    OpenLoop(table->getName()),
	table(table),
    tablePartitioned(nullptr),
	runCount(0)
{}

void OpenLoopInsert::prepare()
{
    tablePartitioned = table->getPartitionObjects(loop->partition, false);

    if (!tablePartitioned)
    {
        suicide();
        return;
    }

//	queueIter = localQueue.end();
    Logger::get().info("insert job started for " + table->getName() + " on partition " + std::to_string(tablePartitioned->partition));
}

bool OpenLoopInsert::run()
{       
    const auto mapInfo = globals::mapper->partitionMap.getState(tablePartitioned->partition, globals::running->nodeId);

	if (mapInfo != openset::mapping::NodeState_e::active_owner &&
		mapInfo != openset::mapping::NodeState_e::active_clone)
	{
		// if we are not in owner or clone state we are just going to backlog
		// the inserts until our state changes, then we will perform inserts
		Logger::get().info("skipping partition " + to_string(tablePartitioned->partition) + " not active or clone.");
		this->scheduleFuture(1000);
        sleepCounter = 0;
		return false;
	}

    int64_t readHandle = 0;
    auto inserts = SideLog::getSideLog().read(table.get(), loop->partition, inBypass() ? 5 : 25, readHandle);
    auto insertIter = inserts.begin();

	if (inserts.empty())
	{
        SideLog::getSideLog().updateReadHead(table.get(), loop->partition, readHandle);
		scheduleFuture((sleepCounter > 10 ? 10 : sleepCounter) * 100); // lazy back-off function
		++sleepCounter; // inc after, this will make it run one more time before sleeping
		return false;
	}

	sleepCounter = 0;
	
	// reusable object representing a person
	Person person;

	++runCount;
       	
	// map a table, partition and entire schema to the Person object
	if (!person.mapTable(tablePartitioned->table, loop->partition))
	{
        // deleted partition - remove worker loop
	    suicide();
        return false;
	}
    
	// we are going to convert the events into JSON, and in the process
	// we are going to group the events by their user_ids. 
	// We will then insert all the events for a given person in one
	// pass. This can greatly reduce redundant calls to Mount and Commit
	// which can be expensive as they both call LZ4 (which is fast, but still
	// has it's overhead)
	std::unordered_map < std::string, std::vector<cjson>> evtByPerson;

	for (; insertIter != inserts.end(); ++insertIter)
	{
		cjson row(*insertIter, cjson::Mode_e::string);		

		// we will take profile or table to specify the table name
		auto uuidString = row.xPathString("/id", "");
		toLower(uuidString);

	    const auto attr = row.xPath("/_");

		// do we have what we need to insert?
		if (attr && uuidString.length())
			evtByPerson[uuidString].emplace_back(std::move(row));
	}

    // after we have processed the data, move the head forward
    SideLog::getSideLog().updateReadHead(table.get(), loop->partition, readHandle);

	// now insert without locks
	for (auto& uuid : evtByPerson)
	{
	    const auto personData = tablePartitioned->people.getmakePerson(uuid.first);
		person.mount(personData);
		person.prepare();

		auto triggers = tablePartitioned->triggers->getTriggerMap();
		for (auto trigger : triggers)
		{
			trigger.second->mount(&person);
			trigger.second->preInsertTest();
		}

		// insert events for this uuid
		for (auto &json : uuid.second)
			person.insert(&json);

		// check status after insert
		for (auto trigger : triggers)
			trigger.second->postInsertTest();

		person.commit();
	}

	tablePartitioned->attributes.clearDirty();

    return true;
}
