#include "oloop_query.h"
#include "indexbits.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopQuery::OpenLoopQuery(
    ShuttleLambda<CellQueryResult_s>* shuttle,
    Database::TablePtr table,
    Macro_s macros,
    openset::result::ResultSet* result,
    int instance)
    : OpenLoop(table->getName(), oloopPriority_e::realtime),
      // queries are high priority and will preempt other running cells
      macros(std::move(macros)),
      shuttle(shuttle),
      table(table),
      parts(nullptr),
      maxLinearId(0),
      currentLinId(-1),
      interpreter(nullptr),
      instance(instance),
      runCount(0),
      startTime(0),
      population(0),
      index(nullptr),
      result(result)
{}

OpenLoopQuery::~OpenLoopQuery()
{
    if (interpreter)
    {
        // free up any segment bits we may have made
        //for (auto bits : interpreter->segmentIndexes)
          //  delete bits;

        delete interpreter;
    }
}

void OpenLoopQuery::prepare()
{
    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    maxLinearId = parts->people.peopleCount();

    // generate the index for this query
    indexing.mount(table.get(), macros, loop->partition, maxLinearId);
    bool countable;
    index      = indexing.getIndex("_", countable);
    population = index->population(maxLinearId);

    interpreter = new Interpreter(macros);
    interpreter->setResultObject(result);

    // if we are in segment compare mode:
    if (macros.segments.size())
    {
        std::vector<IndexBits*> segments;

        for (const auto& segmentName : macros.segments)
        {
            if (segmentName == "*"s)
            {
                auto tBits = new IndexBits();
                tBits->makeBits(maxLinearId, 1);
                segments.push_back(tBits);
            }
            else
            {
                if (!parts->segments.count(segmentName))
                {
                    shuttle->reply(
                        0,
                        result::CellQueryResult_s{
                            instance,
                        {},
                        openset::errors::Error{
                            openset::errors::errorClass_e::run_time,
                            openset::errors::errorCode_e::item_not_found,
                            "missing segment '" + segmentName + "'"
                        }
                        }
                    );
                    suicide();
                    return;
                }

                segments.push_back(parts->segments[segmentName].bits);

            }
        }

        interpreter->setCompareSegments(index, segments);
    }

    // map table, partition and select schema properties to the Person object
    auto mappedColumns = interpreter->getReferencedColumns();
    if (!person.mapTable(table.get(), loop->partition, mappedColumns))
    {
        partitionRemoved();
        suicide();
        return;
    }

    person.setSessionTime(macros.sessionTime);

    startTime = Now();
}

bool OpenLoopQuery::run()
{
    while (true)
    {
        if (sliceComplete())
            return true;

        // are we done? This will return the index of the
        // next set bit until there are no more, or maxLinId is met
        if (interpreter->error.inError() || !index->linearIter(currentLinId, maxLinearId))
        {
            result->setAccTypesFromMacros(macros);

            shuttle->reply(
                0,
                CellQueryResult_s {
                    instance,
                    {},
                    interpreter->error,
                });

            parts->attributes.clearDirty();

            suicide();
            return false;
        }

        if (const auto personData = parts->people.getPersonByLIN(currentLinId); personData != nullptr)
        {
            ++runCount;
            person.mount(personData);
            person.prepare();
            interpreter->mount(&person);
            interpreter->exec(); // run the script on this person - do some magic
        }
    }
}

void OpenLoopQuery::partitionRemoved()
{
    shuttle->reply(
        0,
        CellQueryResult_s {
            instance,
            {},
            openset::errors::Error {
                openset::errors::errorClass_e::run_time,
                openset::errors::errorCode_e::partition_migrated,
                "please retry query"
            }
        });
}
