#include "oloop_revent.h"

#include "people.h"
#include "person.h"
#include "database.h"
#include "table.h"
#include "tablepartitioned.h"

using namespace std;
using namespace openset::async;
using namespace openset::db;

OpenLoopRetrigger::OpenLoopRetrigger(openset::db::Table* table) :
	OpenLoop(),
	table(table),
	linearId(0)
{}

OpenLoopRetrigger::~OpenLoopRetrigger() 
{}

void OpenLoopRetrigger::prepare()
{
	linearId = 0;
	person.mapTable(table, loop->partition);
	lowestStamp = Now() + 15000;
}

void OpenLoopRetrigger::run()
{
	
	auto parts = table->getPartitionObjects(loop->partition);

    parts->triggers->checkForConfigChange();

    const auto maxLinearId = parts->people.peopleCount();
	
	openset::db::PersonData_s* personData;

    const auto now = Now();

	while (true)
	{
		if (sliceComplete())
			return; // let some other open loops run

		if (linearId > maxLinearId)
		{
			auto messages = table->getMessages();

			parts->triggers->dispatchMessages();
						
			messages->run(); // do message queue maintenance 

			OpenLoop* newCell = new OpenLoopRetrigger(table);

		    const auto diff = lowestStamp - Now();

			newCell->scheduleFuture(diff > 500 ? 500 : (diff < 100) ? 100 : diff); // run again in 15 seconds

			spawn(newCell);
			suicide();
			return;
		}

		if ((personData = parts->people.getPersonByLIN(linearId)) != nullptr
			&& personData->flagRecords)		
		{
			auto flagIter = personData->getFlags();

			for (auto i = 0; i < personData->flagRecords; ++i)
			{
				if (flagIter->flagType == flagType_e::future_trigger &&
					flagIter->value < now) // this is expired!
				{
					// get the corresponding trigger object (reference contains trigger Id)
					auto trigger = parts->triggers->getTrigger(flagIter->reference);

					if (trigger)
					{
						person.mount(personData);
						person.prepare();

						trigger->mount(&person);
						trigger->runFunction(flagIter->context); // context contains function Id
					}
					else
					{
						// TODO - missing trigger is an error that should be logged
					}

					// remove this flag from the person object
				    const auto replacementRecord = person.getGrid()->clearFlag(
						flagType_e::future_trigger,
						flagIter->reference,
						flagIter->context);

					parts->people.replacePersonRecord(replacementRecord);

				}
				else if (flagIter->flagType == flagType_e::future_trigger &&
					flagIter->value < lowestStamp)
					lowestStamp = flagIter->value;

				++flagIter;
			}
		}
		++linearId;						
	}
}
