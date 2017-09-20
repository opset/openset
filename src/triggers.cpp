#include "threads/locks.h"

#include "triggers.h"
#include "trigger.h"
#include "config.h"
#include "table.h"
#include "tablepartitioned.h"
#include "oloop_retrigger.h"

using namespace openset::trigger;

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

Triggers::Triggers(openset::db::TablePartitioned* parts):
	table(parts->table),
	parts(parts),
	columns(table->getColumns()),
	loadVersion(table->getLoadVersion())
{
	load();
}

Triggers::~Triggers() 
{}


void Triggers::load() 
{
	{
		csLock lock(globals::running->cs);

		// set the config version for this load
		loadVersion = table->getLoadVersion();

		// record the IDs we have before config syncing
		std::unordered_set<std::string> beforeNames;
		for (auto &t : triggers)
			beforeNames.insert(t.second->getName());

		// get triggers from tables object
		auto triggerList = table->getTriggerConf();

		for (auto &t : *triggerList)
		{
			if (!t.first.size()) // bad name?
				continue;

			if (!triggers.count(t.second->id))
			{
				auto trigger = new Trigger(t.second, parts);
				triggers[t.second->id] = trigger;
			}
		}

		// compare trigger list to before Ids, see if any 
		// were deleted
		for (auto t : beforeNames)
		{
			if (triggerList->count(t) == 0)
			{
				// DELETE
				auto id = MakeHash(t);
				delete triggers[id]; // delete the object
				triggers.erase(id); // erase if form the hash

				cout << "this happened on " << parts->partition << endl;
				// TODO - delete this from the Attributes Object as well
			}
		}
	}

	// schedule the re-trigger job
	async::OpenLoop* newCell = new async::OpenLoopRetrigger(table);
	newCell->scheduleFuture(5000); // run this in 15 seconds

	// add it to the async loop for this partition
	parts->asyncLoop->queueCell(newCell);
}

void Triggers::dispatchMessages() const
{
	csLock lock(globals::running->cs);

	auto triggers = parts->triggers->getTriggerMap();

	for (auto &t : triggers)
		table->getMessages()->push(t.second->getName(), t.second->triggerQueue);
	
}

void Triggers::checkForConfigChange()
{
	if (loadVersion != table->getLoadVersion())
		load();
}
