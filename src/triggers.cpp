#include "threads/locks.h"

#include "triggers.h"
#include "trigger.h"
#include "config.h"
#include "table.h"
#include "tablepartitioned.h"
#include "oloop_revent.h"

using namespace openset::revent;

/*
 * Triggers - is a per partition container of individual Trigger objects.
 *
 * Config is read from the table object.
 * 
 * Each partition has it's own independent copy of this object.
 *
 * Config must be maintained by using version variables in both
 * the Table object and the individual Triggers object. 
 *
 * This allows async reconfiguration within the worker threads.
 */

ReventManager::ReventManager(openset::db::TablePartitioned* parts) :
    parts(parts),
    columns(parts->table->getColumns()),
    loadVersion(parts->table->getLoadVersion())
{
    start();
}

ReventManager::~ReventManager()
{
    for (auto &r: revents)
    {
        delete r.second;
        r.second = nullptr;
    }
}

void ReventManager::start() 
{

    // this method gets the shared_ptr version we need to create 
    // an OpenLoopRetrigger object
    auto table = globals::database->getTable(parts->table->getName());

	{ // scope for lock
		csLock lock(globals::running->cs);

		// set the config version for this load
		loadVersion = table->getLoadVersion();

		// record the IDs we have before config syncing
		std::unordered_set<std::string> beforeNames;
		for (auto &t : revents)
			beforeNames.insert(t.second->getName());

		// get triggers from tables object
	    const auto triggerList = table->getTriggerConf();

		for (auto &t : *triggerList)
		{
			if (!t.first.size()) // bad name?
				continue;

			if (!revents.count(t.second->id))
			{
			    const auto trigger = new Revent(t.second, parts);
				revents[t.second->id] = trigger;
			}
		}

		// compare trigger list to before Ids, see if any 
		// were deleted
		for (const auto &t : beforeNames)
			if (triggerList->count(t) == 0)
			{
				// DELETE
				auto id = MakeHash(t);
				delete revents[id]; // delete the object
				revents.erase(id); // erase if form the hash

				cout << "this happened on " << parts->partition << endl;
				// TODO - delete this from the Attributes Object as well
			}
	}

	// create the re-trigger job (it will continue to remake itself after)
	async::OpenLoop* newCell = new async::OpenLoopRevent(table);
    // first run in 5 seconds
	newCell->scheduleFuture(5000); 
	// add it to the async loop for this partition
	parts->asyncLoop->queueCell(newCell);
}

void ReventManager::dispatchMessages() const
{
	csLock lock(globals::running->cs);

	auto triggers = parts->triggers->getTriggerMap();

	for (auto &t : triggers)
		parts->table->getMessages()->push(t.second->getName(), t.second->triggerQueue);	
}

void ReventManager::checkForConfigChange()
{
	if (loadVersion != parts->table->getLoadVersion())
		start();
}
