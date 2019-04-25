#include "oloop_seg_refresh.h"
#include "indexbits.h"
#include "columns.h"
#include "tablepartitioned.h"
#include "queryparser.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopSegmentRefresh::OpenLoopSegmentRefresh(openset::db::Database::TablePtr table) :
	OpenLoop(table->getName()),
	parts(nullptr),
	table(table),
	maxLinearId(0),
    currentLinId(-1),
	interpreter(nullptr),
	instance(0),
	runCount(0),
	index(nullptr)
{
}

OpenLoopSegmentRefresh::~OpenLoopSegmentRefresh()
{
    if (parts && prepared)
       --parts->segmentUsageCount;        
}

void OpenLoopSegmentRefresh::storeSegment() const
{
    // store any changes we've made to the segments
    parts->storeAllChangedSegments();

    auto delta = bits->population(maxLinearId) - startPopulation;

    // update the segment refresh
	parts->setSegmentRefresh(segmentName, macros.segmentRefresh);
	parts->setSegmentTTL(segmentName, macros.segmentTTL);

    if (delta != 0)
	    Logger::get().info("segment refresh on " + table->getName() + "/" + segmentName + ". (delta " + to_string(delta) + ")");
}

bool OpenLoopSegmentRefresh::nextExpired()
{
    // this loop is a bit expensive and may make sense to break down into
    // a more async process at some future point.
	while (true)
	{

		if (segmentsIter == parts->segments.end())
        {
            respawn();
			return false;
        }

		segmentName = segmentsIter->first;

        if (!parts->isRefreshDue(segmentName))
        {
            ++segmentsIter;
            continue;
        }
        
		macros = segmentsIter->second.macros;
        segmentInfo = &parts->segments[segmentName];
        	    
		// generate the index for this query	
		indexing.mount(table.get(), macros, loop->partition, maxLinearId);
		bool countable;
		index = indexing.getIndex("_", countable);

	    // get bits for this segment
		bits = parts->getBits(segmentName);        
        startPopulation = bits->population(maxLinearId);

        auto getSegmentCB = parts->getSegmentCallback();

		// is this something we can calculate using purely
		// indexes? (query logic shows we can simply use binary operators to calculate the segment)
		if (countable && !macros.isSegmentMath)
		{
            // index contains result
            bits->opCopy(*index);

            // index is the result when binary index math can be used.
            // copy the index
			storeSegment();
            ++segmentsIter;
			continue;
		}

        // this script needs to be executed
		interpreter = parts->getInterpreter(segmentName, maxLinearId);
		interpreter->setGetSegmentCB(getSegmentCB);

		auto mappedColumns = interpreter->getReferencedColumns();

		// clean the person object
		person.reinit();
		// map table, partition and select schema columns to the Person object
		if (!person.mapTable(table.get(), loop->partition, mappedColumns))
	    {
	        suicide();
            return false;
	    }

		// is this calculated using other segments (i.e. the functions
		// population, intersection, union, difference and compliment)
		// meaning we do not have to iterate user records
		if (macros.isSegmentMath)
		{
			interpreter->interpretMode = InterpretMode_e::count;

            // mount empty person record (required but not used in segment math queries).
			interpreter->mount(&person);
			interpreter->exec();

			storeSegment();

            ++segmentsIter;
			continue;
		};

		// reset the linear iterator current index
		currentLinId = -1;

		++segmentsIter;

		// we have to execute actual code that iterates people
		return true;
	}
}

void OpenLoopSegmentRefresh::prepare()
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
    
    segmentsIter = parts->segments.begin();
	maxLinearId = parts->people.peopleCount();
	
	nextExpired();
}

void OpenLoopSegmentRefresh::respawn()
{
    OpenLoop* newCell = new OpenLoopSegmentRefresh(table);
    
    newCell->scheduleFuture(table->segmentInterval); // check again in (60 second default)   
    spawn(newCell); // add replacement to scheduler

    suicide(); // kill this cell.
}

bool OpenLoopSegmentRefresh::run()
{

	if (!interpreter)
    {
        respawn();
		return false;
    }

	openset::db::PersonData_s* personData;

	while (true)
	{
		if (sliceComplete())
			break; // let some other cells run
		
		// are we out of bits to analyze?
        // lets move to the next expired segment
		if (interpreter->error.inError() || 
			!index->linearIter(currentLinId, maxLinearId))
		{

			// TODO - log error
			storeSegment();

			// add to resultBits upon query completion
			if (interpreter->error.inError())
	            Logger::get().error("attempted refresh on " + table->getName() + "/" + segmentName + ". " + interpreter->error.getErrorJSON());		    
			
			// all done?
			if (!nextExpired())
                return false;

            // keep working
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

            // if we have bits (we should always have bits)
            if (interpreter->bits)
            {                
                // get return values from script
                auto returns = interpreter->getLastReturn();

                // any returns, are they true?
                segmentInfo->setBit(currentLinId, returns.size() && returns[0].getBool() == true);
            }
		}
	}
    return true;
}
