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
	population(0),
	popEvaluated(0),
	index(nullptr),
	result(result),
	macroIter(macrosList.begin())
{}

OpenLoopSegment::~OpenLoopSegment()
{
	if (interpreter)
		delete interpreter;

	for (auto &rb : resultBits)
		if (rb.second)
			delete rb.second;
}

void OpenLoopSegment::storeResult(std::string name, int64_t count) const
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

	for (auto& macro : macrosList)
	{
		const auto &segmentName = macro.first;

		if (macro.second.segmentRefresh != -1)
			parts->setSegmentRefresh(segmentName, macro.second.segmentRefresh);

		// are we storing this, and was this a cached copy (not fresh)
		// if so, store it, otherwise let it age.
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
}

bool OpenLoopSegment::nextMacro()
{

	// lambda callback used by interpretor when executing segment
	// queries to get segments not caculated in the current query 
	// i.e. get save segments (those created with TTLs)
	const auto getSegmentCB = [&](std::string segmentName, bool &deleteAfterUsing) -> IndexBits*
	{
		// try for local bits (made during this query) first as they 
		// may be fresher
		deleteAfterUsing = false;
		if (resultBits.count(segmentName))
			return resultBits[segmentName];

		// if there are no bits with this name created in this query
		// then look in the index
		auto attr = parts->attributes.get(COL_SEGMENT, segmentName);
		
		if (!attr)
			return nullptr;

		deleteAfterUsing = true;
		return attr->getBits();
	};

	// loop until we find an segment index that requires
	// querying, otherwise, if an index is "countable"
	// we will just use it's population and move to
	// the next segment.
	while (true)
	{

		if (macroIter == macrosList.end())
			return false;

		// set the resultName variable, this will be the branch
		// in the result set we use to store the value for this index count
		resultName = macroIter->first;
		macros = macroIter->second;

		// generate the index for this query	
		indexing.mount(table.get(), macros, loop->partition, maxLinearId);
		bool countable;
		index = indexing.getIndex("_", countable);
		population = index->population(maxLinearId);
		popEvaluated += population;

		// create our bits, and zero them out
		auto bits = new IndexBits();
		bits->makeBits(maxLinearId, 0);


		// we may have a cached copy
		if (macros.useCached && !parts->isSegmentExpiredTTL(resultName))
		{
			auto deleteAfterUsing = false;
			const auto cachedBits = getSegmentCB(resultName, deleteAfterUsing);
			if (cachedBits)
			{
				bits->opCopy(*cachedBits);
				resultBits[resultName] = bits;
				storeResult(resultName, bits->population(maxLinearId));

				segmentWasCached.insert(resultName);

				if (deleteAfterUsing)
					delete cachedBits;

				++macroIter;
				continue; // try another index
			}
			// cached copy not found... carry on!
		}

		// is this something we can calculate using purely
		// indexes? (nifty)
		if (countable && !macros.isSegmentMath)
		{
			bits->opCopy(*index);

			// add to resultBits upon query completion
			resultBits[resultName] = bits;
			storeResult(resultName, population);

			++macroIter;
			continue; // try another index
		}

		// we need a new interpreter object
		if (interpreter) 
			delete interpreter;

		interpreter = new Interpreter(macros, openset::query::InterpretMode_e::count);
		interpreter->setGetSegmentCB(getSegmentCB);
		interpreter->setBits(bits, maxLinearId);

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
			resultBits[resultName] = interpreter->bits;
			storeResult(resultName, interpreter->bits->population(maxLinearId));

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

	maxLinearId = parts->people.peopleCount();

	startTime = Now();

	// Note - OpenLoopSegment can return in the prepare if none of the queries
	// require iterating user records (as in were cached, segmentMath, or indexed).
	if (!nextMacro())
	{
		const auto time = Now() - startTime;

		openset::errors::Error error;

		if (interpreter)
			error = interpreter->error;

		storeSegments();

        result->setAccTypesFromMacros(macros);

		shuttle->reply(
			0,
			CellQueryResult_s{
			instance,
            {},
			error
		}
		);

		this->suicide();
		return;
	}
		
}

bool OpenLoopSegment::run()
{
	openset::db::PersonData_s* personData;
	while (true)
	{
		if (sliceComplete())
			return true; // let some other cells run

		if (!interpreter && !nextMacro())
			return false;

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
			resultBits[resultName] = interpreter->bits;
			storeResult(resultName, interpreter->bits->population(maxLinearId));
			
			// is there another query to run? If not we are done
			if (!nextMacro())
			{
				const auto time = Now() - startTime;

				openset::errors::Error error;

				if (interpreter)
					error = interpreter->error;

				storeSegments();

                result->setAccTypesFromMacros(macros);

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
    			delete interpreter;
                interpreter = nullptr;

		        storeSegments();

                result->setAccTypesFromMacros(macros);

		        shuttle->reply(
			        0,
			        CellQueryResult_s{
			            instance,
                        {},
			            error
		            }
		        );
                this->suicide();
                return false;
            }

            // get return values from script
            auto returns = interpreter->getLastReturn();

            // any returns, are they true?
            const auto inSegment = (returns.size() && returns[0].getBool() == true);

            // if we have bits (we should always have bits)
            if (interpreter->bits)
            {                
                if (inSegment)
                    interpreter->bits->bitSet(currentLinId); // add to segment
                else
                    interpreter->bits->bitClear(currentLinId); // remove from segment
            }

            //loopState = LoopState_e::in_exit;
            //*stackPtr = 0;
            //++stackPtr;
            //return true;

                
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
