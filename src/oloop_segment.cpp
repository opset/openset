#include "oloop_segment.h"
#include "indexbits.h"
#include "columns.h"
#include "tablepartitioned.h"
#include "queryparser.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopSegment::OpenLoopSegment(
	ShuttleLambda<CellQueryResult_s>* shuttle,
	openset::db::Database::TablePtr table,
    const QueryPairs macros,
	openset::result::ResultSet* result,
    const int instance) :

	OpenLoop(table->getName(), oloopPriority_e::realtime),
	macrosList(macros),
	shuttle(shuttle),
	table(table),
	parts(nullptr),
	maxLinearId(0),
	currentLinId(-1),
	interpreter(nullptr),
	instance(instance),
	runCount(0), 
	startTime(0),
	index(nullptr),
	result(result),
	macroIter(macrosList.begin())
{}

OpenLoopSegment::~OpenLoopSegment()
{
    if (parts)
    {
        if (prepared)
            --parts->segmentUsageCount;
        parts->storeAllChangedSegments();
    }

    cout << "openloopsegment deleted" << endl;
}

void OpenLoopSegment::storeResult(std::string& name, int64_t count) const
{
	const auto nameHash = MakeHash(name);

	const auto set_cb = [count](openset::result::Accumulator* resultColumns)
	{
		if (resultColumns->columns[0].value == NONE)
			resultColumns->columns[0].value = count;
		else
			resultColumns->columns[0].value += count;
		//resultColumns->columns[0].distinctId = 0;
	};

	RowKey rowKey;
	rowKey.clear();
	rowKey.key[0] = nameHash;
    rowKey.types[0] = ResultTypes_e::Text;
	result->addLocalText(nameHash, name);

    auto aggs = result->getMakeAccumulator(rowKey);
    set_cb(aggs);
}

void OpenLoopSegment::storeSegments()
{
	/*  resultBits will contain fresh IndexBits objects.
	 *
	 *  We will iterate the macrosList, check for a TTL, and
	 *  if present get the bits from resultBits and store them in
	 *  the index. This can happen without locking as it happens
	 *  on a partition within an async worker thread, and indexes
	 *  are local to the partition	 
	 */

    // store any changes we've made to the segments
    parts->storeAllChangedSegments();

    for (auto& macro : macrosList)
    {
		const auto &segmentName = macro.first;

		if (macro.second.segmentRefresh != -1)
			parts->setSegmentRefresh(segmentName, macro.second.segmentRefresh);

        if (macro.second.segmentTTL != -1)
            parts->setSegmentTTL(segmentName, macro.second.segmentTTL);       
    }

    /*
	for (auto& macro : macrosList)
	{
		const auto &segmentName = macro.first;

		if (macro.second.segmentRefresh != -1)
			parts->setSegmentRefresh(segmentName, macro.second.segmentRefresh);

		// are we storing this, and was this a cached copy (not fresh)
		// if its a fresh query then store it, otherwise let it age.
		if (macro.second.segmentTTL != -1 && 
			segmentWasCached.count(segmentName) == 0)
		{
			const auto bits = resultBits[segmentName];

			if (!bits) continue;

			auto pop = bits->population(maxLinearId);

			// make sure it exists in the index, we don't care about the return value
			parts->attributes.getMake(COL_SEGMENT, segmentName);

			// swap our new or existing index entry with some new IndexBits, compress, and store them
			parts->attributes.swap(COL_SEGMENT, MakeHash(segmentName), bits);
			delete bits; // we are done with the bits

			resultBits[segmentName] = nullptr; // remove it from the map so it doesn't get deleted in ~OpenLoopSegment
			
			parts->setSegmentTTL(segmentName, macro.second.segmentTTL);
			parts->setSegmentRefresh(segmentName, macro.second.segmentRefresh);
		}
	}
    */
}

bool OpenLoopSegment::nextMacro()
{
	// loop until we find an segment index that requires
	// querying, otherwise, if an index is "countable"
	// we will just use it's population and move to
	// the next segment.
	while (true)
	{

		if (macroIter == macrosList.end())
        {
			storeSegments();

            if (macrosList.size())
                result->setAccTypesFromMacros(macrosList.begin()->second);

		    openset::errors::Error error;

			shuttle->reply(
				0,
				CellQueryResult_s{
					instance,
                    {},
					error
				}
			);

		    suicide();

			return false;
        }

		// set the resultName variable, this will be the branch
		// in the result set we use to store the value for this index count
		resultName = macroIter->first;
        segmentInfo = &parts->segments[resultName];

        auto& macros = macroIter->second;

		// generate the index for this query	
		indexing.mount(table.get(), macros, loop->partition, maxLinearId);
		bool countable;
		index = indexing.getIndex("_", countable);

		// get the bits for this segment
        auto bits = parts->getBits(resultName);
        
		// should we return these bits, as a cached copy?
		if (macros.useCached && !parts->isSegmentExpiredTTL(resultName))
		{
		    if (bits)
			{
				storeResult(resultName, bits->population(maxLinearId));
				++macroIter;
				continue; // try another index
			}
			// cached copy not found... carry on!
		}

		// is this something we can calculate using purely
		// indexes? (nifty)
		if (countable && !macros.isSegmentMath)
        {
            // index contains result
            bits->opCopy(*index);

		    // add to resultBits upon query completion
			storeResult(resultName, index->population(maxLinearId));

			++macroIter;
			continue; // try another index
		}

		interpreter = parts->getInterpreter(resultName, maxLinearId);
        auto getSegmentCB = parts->getSegmentCallback();
		interpreter->setGetSegmentCB(getSegmentCB);

		auto mappedColumns = interpreter->getReferencedColumns();

		// clean the person object
		person.reinit();
		// map table, partition and select schema columns to the Person object
		if (!person.mapTable(table.get(), loop->partition, mappedColumns))
	    {
            partitionRemoved();
	        suicide();
            return false;
	    }

		// is this calculated using other segments (i.e. the functions
		// population, intersection, union, difference and compliment)
		// meaning we do not have to iterate user records
		if (macros.isSegmentMath)
		{
			interpreter->interpretMode = InterpretMode_e::count;

			interpreter->mount(&person);
			interpreter->exec();

			// add to resultBits upon query completion
			storeResult(resultName, bits->population(maxLinearId));

			++macroIter;
			continue;
		};

		// reset the linear iterator current index
		currentLinId = -1;

		++macroIter;

		// we have to execute actual code that iterates people
		return true;
	}
}

void OpenLoopSegment::prepare()
{
	auto prepStart = Now();
    
	parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    parts->checkForSegmentChanges();   
    ++parts->segmentUsageCount;

	maxLinearId = parts->people.peopleCount();

	startTime = Now();

	// Note - OpenLoopSegment can return in the prepare if none of the queries
	// require iterating user records (as in were cached, segmentMath, or indexed).
	nextMacro();		
}

bool OpenLoopSegment::run()
{

	openset::db::PersonData_s* personData;
	while (true)
	{
		if (sliceComplete())
			return true; // let some other cells run

		if (!interpreter) 
			return false;

		// if there was an error, exit
		if (interpreter->error.inError())
		{
			const auto time = Now() - startTime;

			shuttle->reply(
				0, 
				CellQueryResult_s{				
					instance,
                    {},
					interpreter->error
				}
			);

			suicide();
			return false;
		}

		// are we out of bits to analyze?
		if (!index->linearIter(currentLinId, maxLinearId))
		{
			// add to resultBits upon query completion
			storeResult(resultName, interpreter->bits->population(maxLinearId));
			
			// is there another query to run? If not we are done
			if (!nextMacro())
				return false;

			// we have more macros, loop to the top and try again
			//continue;
			return true;
		}
		
		if (currentLinId < maxLinearId && 
			(personData = parts->people.getPersonByLIN(currentLinId)) != nullptr)
		{
			++runCount;
			person.mount(personData);
			person.prepare();
			interpreter->mount(&person);
			interpreter->exec();            	    

		    if (interpreter->error.inError())
            {
                openset::errors::Error error;
			    error = interpreter->error;
                interpreter = nullptr;

		        storeSegments();

                if (macrosList.size())
                    result->setAccTypesFromMacros(macrosList.begin()->second);

		        shuttle->reply(
			        0,
			        CellQueryResult_s{
			            instance,
                        {},
			            error
		            }
		        );
                suicide();
                return false;
            }

            // get return values from script
            auto returns = interpreter->getLastReturn();

            // any returns, are they true?
            segmentInfo->setBit(currentLinId, returns.size() && returns[0].getBool() == true);              
		}
	}
}

void OpenLoopSegment::partitionRemoved()
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
