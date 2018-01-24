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
	lowestStamp = Now() + 90000;

    parts = table->getPartitionObjects(loop->partition);
    parts->triggers->checkForConfigChange();

}

void OpenLoopRetrigger::run()
{
    const auto maxLinearId = parts->people.peopleCount();
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

			newCell->scheduleFuture(diff > 15000 ? 15000 : 5000); 

			spawn(newCell);
			suicide();
			return;
		}

        if (auto personData = parts->people.getPersonByLIN(linearId);
             personData && personData->flagRecords)		
		{
			auto flagIter = personData->getFlags();

            for (auto i = 0; i < personData->flagRecords; ++i)
			{
				if (flagIter->flagType == flagType_e::future_trigger &&
					flagIter->value < now) // this is expired!
				{
					// get the corresponding trigger object (reference contains trigger Id)
					auto trigger = parts->triggers->getRevent(flagIter->reference);

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

                    // personData has moved
                    const auto tp = person.getGrid()->getMeta();
                    if (personData != tp)
                    {
                        personData = tp;
                        flagIter = personData->getFlags() + i;
                    }
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
