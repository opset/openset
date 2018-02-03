#include "oloop_insert.h"
#include "cjson/cjson.h"
#include "str/strtools.h"

#include "people.h"
#include "person.h"
#include "database.h"
#include "table.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
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
    tablePartitioned = table->getPartitionObjects(loop->partition);
	queueIter = localQueue.end();
    Logger::get().info("insert job started for " + table->getName() + " on partition " + std::to_string(tablePartitioned->partition));
}

void OpenLoopInsert::run()
{

	if ((localQueue.empty() || queueIter == localQueue.end()) &&
		!tablePartitioned->insertQueue.empty())
	{
		if (!tablePartitioned->insertCS.tryLock())
		{
			if (sleepCounter > 50)
				sleepCounter = 50;
			scheduleFuture(sleepCounter * 10); // lazy backoff function
			++sleepCounter; // inc after, this will make it run one more time before sleeping
			return;
		}

		// swap the queues, so fast, so spiffy
		localQueue.clear();
		localQueue = std::move(tablePartitioned->insertQueue);
		tablePartitioned->insertQueue = {};

		tablePartitioned->insertCS.unlock();

		queueIter = localQueue.begin();
	}

	if (queueIter == localQueue.end())
	{
		if (sleepCounter > 50)
			sleepCounter = 50;
		scheduleFuture(sleepCounter * 10); // lazy backoff function
		++sleepCounter; // inc after, this will make it run one more time before sleeping
		return;
	}

	sleepCounter = 0;
	
	// reusable object representing a person
	Person person;

	++runCount;

    const auto mapInfo = globals::mapper->partitionMap.getState(tablePartitioned->partition, globals::running->nodeId);

	if (mapInfo != openset::mapping::NodeState_e::active_owner &&
		mapInfo != openset::mapping::NodeState_e::active_clone)
	{
		// if we are not in owner or clone state we are just going to backlog
		// the inserts until our state changes, then we will perform inserts
		Logger::get().info("skipping partition " + to_string(tablePartitioned->partition) + " not active or clone.");
		this->scheduleFuture(1000);
		return;
	}	

	// map a table, partition and entire schema to the Person object
	person.mapTable(tablePartitioned->table, loop->partition);

	// we are going to convert the events into JSON, and in the process
	// we are going to group the events by their user_ids. 
	// We will then insert all the events for a given person in one
	// pass. This can greatly reduce redundant calls to Mount and Commit
	// which can be expensive as they both call LZ4 (which is fast, but still
	// has it's overhead)
	std::unordered_map < std::string, std::vector<cjson>> evtByPerson;

	// now insert without locks
	for (auto count = 0; 
        queueIter != localQueue.end() && count < (inBypass() ? 15 : 50); 
        ++queueIter, ++count, --tablePartitioned->insertBacklog)
	{
		cjson row(*queueIter, cjson::Mode_e::string);		
		cjson::releaseStringifyPtr(*queueIter);

		// we will take profile or table to specify the table name
		auto uuidString = row.xPathString("/person", "");
		toLower(uuidString);

	    const auto attr = row.xPath("/_");

		// do we have what we need to insert?
		if (attr && uuidString.length())
			evtByPerson[uuidString].emplace_back(std::move(row));
	}

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
}
