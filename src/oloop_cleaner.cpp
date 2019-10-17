#include "oloop_cleaner.h"

#include "customers.h"
#include "customer.h"
#include "database.h"
#include "table.h"
#include "tablepartitioned.h"

using namespace std;
using namespace openset::async;
using namespace openset::db;

OpenLoopCleaner::OpenLoopCleaner(const openset::db::Database::TablePtr table) :
    OpenLoop(table->getName()),
    table(table),
    linearId(0)
{}

void OpenLoopCleaner::prepare()
{
    linearId = 0;
    person.mapTable(table.get(), loop->partition);

    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    //parts->triggers->checkForConfigChange();
}

void OpenLoopCleaner::respawn()
{
    OpenLoop* newCell = new OpenLoopCleaner(table);

    // schedule and add some random shuffle to stagger start-times accross workers
    newCell->scheduleFuture(table->maintInterval);

    spawn(newCell); // add replacement to scheduler
    suicide(); // kill this cell.
}

bool OpenLoopCleaner::run()
{
    const auto maxLinearId = parts->people.customerCount();

    auto dirty = false;

    Logger::get().info("+ cleaner running for " + table->getName() + ".");

    while (true)
    {
        if (sliceComplete())
        {
            if (dirty)
                parts->attributes.clearDirty();
            return true; // let some other open loops run
        }

        if (linearId > maxLinearId)
        {
            if (dirty)
                parts->attributes.clearDirty();
            respawn();
            return false;
        }

        if (const auto personData = parts->people.getCustomerByLIN(linearId); personData)
        {
            person.mount(personData);
            person.prepare();
            if (person.getGrid()->cull())
            {
                if (person.getGrid()->getRows()->empty())
                {
                    parts->people.drop(personData->id);
                }
                else
                {
                    person.commit();
                }
                dirty = true;
            }
        }

        ++linearId;
    }
}
