#include "oloop_customer_list.h"
#include "indexbits.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopCustomerList::OpenLoopCustomerList(
    ShuttleLambda<CellQueryResult_s>* shuttle,
    Database::TablePtr table,
    Macro_s macros,
    openset::result::ResultSet* result,
    const std::vector<int> &sortOrderProperties,
    const std::vector<int64_t> &cursor,
    const bool descending,
    const int limit,
    int instance) :
        OpenLoop(table->getName(), oloopPriority_e::realtime),
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
        result(result),
        cursor(cursor),
        sortOrderProperties(sortOrderProperties),
        descending(descending),
        limit(limit)
{}

OpenLoopCustomerList::~OpenLoopCustomerList()
{
    if (interpreter)
        delete interpreter;
}

void OpenLoopCustomerList::prepare()
{
    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    maxLinearId = parts->people.customerCount();

    // generate the index for this query
    indexing.mount(table.get(), macros, loop->partition, maxLinearId);
    bool countable;
    index      = indexing.getIndex("_", countable);
    population = index->population(maxLinearId);

    interpreter = new Interpreter(macros);
    interpreter->setResultObject(result);

    IndexBits testIndex;

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

                segments.push_back(parts->segments[segmentName].getBits(parts->attributes));

            }
        }

        //interpreter->setCompareSegments(index, segments);
        testIndex.opCopy(*index);
        testIndex.opAnd(*segments[0]);
    }
    else
    {
        testIndex.opCopy(*index);
    }

    // map table, partition and select schema properties to the Customer object
    auto mappedColumns = interpreter->getReferencedColumns();
    if (!person.mapTable(table.get(), loop->partition, mappedColumns))
    {
        partitionRemoved();
        suicide();
        return;
    }

    person.setSessionTime(macros.sessionTime);

    const auto filterAscending = [&](SortKeyOneProp_s* key, int* value) -> bool {
        if (!testIndex.bitState(*value))
            return false;
        if (key->value == cursor[0] && key->customerId == cursor[1])
            return false;
        if (key->value < cursor[0])
            return false;
        if (key->value > cursor[0] || key->customerId >= cursor[1])
            return true;
        return false;
    };

    const auto filterDescending = [&](SortKeyOneProp_s* key, int* value) -> bool {
        if (!testIndex.bitState(*value))
            return false;
        if (key->value == cursor[0] && key->customerId == cursor[1])
            return false;
        if (key->value > cursor[0])
            return false;
        if (key->value < cursor[0] || key->customerId <= cursor[1])
            return true;
        return false;
    };

    const auto propIndex = macros.vars.columnVars[sortOrderProperties[0]].schemaColumn;

    if (descending)
        indexedList = parts->attributes.customerIndexing.getList(
            propIndex,
            true,
            limit,
            filterDescending
        );
    else
        indexedList = parts->attributes.customerIndexing.getList(
            propIndex,
            false,
            limit,
            filterAscending
        );

    iter = indexedList.begin();

    startTime = Now();
}

bool OpenLoopCustomerList::run()
{
    while (true)
    {
        if (sliceComplete())
            return true;

        // are we done? This will return the index of the
        // next set bit until there are no more, or maxLinId is met
        if (interpreter->error.inError() || iter == indexedList.end())
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

        if (const auto personData = parts->people.getCustomerByLIN(iter->second); personData != nullptr)
        {
            ++runCount;
            person.mount(personData);
            person.prepare();
            interpreter->mount(&person);
            interpreter->exec(); // run the script on this customer - do some magic
        }

        ++iter;
    }
}

void OpenLoopCustomerList::partitionRemoved()
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
