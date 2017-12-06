#include "oloop_histogram.h"
#include "indexbits.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopHistogram::OpenLoopHistogram(
	ShuttleLambda<CellQueryResult_s>* shuttle,
	Table* table, 
	Macro_s macros, 
    std::string groupName,
    const int64_t bucket,
	openset::result::ResultSet* result,
    const int instance) :

	OpenLoop(oloopPriority_e::realtime), // queries are high priority and will preempt other running cells
	macros(std::move(macros)),
	shuttle(shuttle),
    groupName(std::move(groupName)),
    bucket(bucket),
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

OpenLoopHistogram::~OpenLoopHistogram()
{
	if (interpreter)
	{
		// free up any segment bits we may have made
		for (auto bits : interpreter->segmentIndexes)
			delete bits;

		delete interpreter;
	}
}

void OpenLoopHistogram::prepare()
{
	parts = table->getPartitionObjects(loop->partition);
	maxLinearId = parts->people.peopleCount();

	// generate the index for this query	
	indexing.mount(table, macros, loop->partition, maxLinearId);
	bool countable;
	index = indexing.getIndex("_", countable);
	population = index->population(maxLinearId);

	interpreter = new Interpreter(macros);
	interpreter->setResultObject(result);

	// if we are in segment compare mode:
	if (macros.segments.size())
	{
		std::vector<IndexBits*> segments;

		for (const auto segmentName : macros.segments)
		{
            if (segmentName == "*")
            {
                auto tBits = new IndexBits();
                tBits->makeBits(maxLinearId, 1);
                segments.push_back(tBits);
            }
            else
            {
                auto attr = parts->attributes.get(COL_SEGMENT, MakeHash(segmentName));
                if (attr)
                    segments.push_back(attr->getBits());
                else
                    segments.push_back(new IndexBits());
            }
		}

		interpreter->setCompareSegments(index, segments);
	}


	auto mappedColumns = interpreter->getReferencedColumns();

	// map table, partition and select schema columns to the Person object
	person.mapTable(table, loop->partition, mappedColumns);
	person.setSessionTime(macros.sessionTime);

    rowKey.clear();
    rowKey.key[0] = MakeHash(groupName);
    result->addLocalText(rowKey.key[0], groupName);

    rowKey.types[0] = ResultTypes_e::Text;
    rowKey.types[1] = ResultTypes_e::Double;
    	
	startTime = Now();
}

void OpenLoopHistogram::run()
{
	auto count = 0;
	openset::db::PersonData_s* personData;
	while (true)
	{
		if (sliceComplete())
			break;

    	// are we done? This will return the index of the 
		// next set bit until there are no more, or maxLinId is met
		if (interpreter->error.inError() || !index->linearIter(currentLinId, maxLinearId))
		{
			shuttle->reply(
				0, 
				CellQueryResult_s{				
					instance,
                    {},
					interpreter->error,
				});
		
			suicide();
			return;
		}

		if ((personData = parts->people.getPersonByLIN(currentLinId)) != nullptr)
		{
			++runCount;
			person.mount(personData);
			person.prepare();
			interpreter->mount(&person);
			interpreter->exec(); // run the script on this person - do some magic
            auto returns = interpreter->getLastReturn();

            auto idx = -1;
            for (auto r : returns)
            {
                ++idx;

                if (r == NONE)
                    continue;

                auto value = static_cast<int64_t>(r.getDouble() * 10000.0);

                // bucket the key if it's non-zero
                if (bucket)
                    value = (value / bucket) * bucket;


                rowKey.key[1] = NONE;

                auto tPair = result->results.get(rowKey);
                if (!tPair)
                {
                    const auto t = new (result->mem.newPtr(sizeof(openset::result::Accumulator))) openset::result::Accumulator();
                    tPair = result->results.set(rowKey, t);
                }

                if (tPair->second->columns[idx].value == NONE)
                    tPair->second->columns[idx].value = 1;
                else
                    ++tPair->second->columns[idx].value;

                // set the key
                rowKey.key[1] = value;
                
                tPair = result->results.get(rowKey);
                if (!tPair)
                {
                    const auto t = new (result->mem.newPtr(sizeof(openset::result::Accumulator))) openset::result::Accumulator();
                    tPair = result->results.set(rowKey, t);
                }

                if (tPair->second->columns[idx].value == NONE)
                    tPair->second->columns[idx].value = 1;
                else
                    ++tPair->second->columns[idx].value;
            }
		}

		++count;
	}
}

void OpenLoopHistogram::partitionRemoved()
{
	shuttle->reply(
		0,
		CellQueryResult_s{
		instance,
        {},
		openset::errors::Error{
			openset::errors::errorClass_e::run_time,
			openset::errors::errorCode_e::partition_migrated,
			"please retry query"
		}
	});
}
