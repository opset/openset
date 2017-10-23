#include "oloop_seg_refresh.h"
#include "indexbits.h"
#include "columns.h"
#include "tablepartitioned.h"
#include "queryparser.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopSegmentRefresh::OpenLoopSegmentRefresh(TablePartitioned* parts) :
	OpenLoop(),
	parts(parts),
	table(parts->table),
	maxLinearId(0),
	currentLinId(-1),
	interpreter(nullptr),
	instance(instance),
	runCount(0),
	index(nullptr)
{}

OpenLoopSegmentRefresh::~OpenLoopSegmentRefresh()
{
	if (interpreter)
	{
		if (interpreter->bits)
			delete interpreter->bits;
		delete interpreter;
	}
}

void OpenLoopSegmentRefresh::storeSegment(IndexBits* bits) const
{
	// make sure it exists in the index, we don't care about the return value
	parts->attributes.getMake(COL_SEGMENT, segmentName);

	// swap our new or existing index entry with some new IndexBits, compress, and store them
	parts->attributes.swap(COL_SEGMENT, MakeHash(segmentName), bits);
	//delete bits; // we are done with the bits

	parts->setSegmentRefresh(segmentName, macros.segmentRefresh);
	parts->setSegmentTTL(segmentName, macros.segmentTTL);

	Logger::get().info("did refresh on " + table->getName() + "/" + segmentName + ".");
}

bool OpenLoopSegmentRefresh::nextExpired()
{
	// retreive any indexes this script depends on
	auto getSegmentCB = [&](std::string segmentName, bool &deleteAfterUsing) -> IndexBits*
	{
		// if there are no bits with this name created in this query
		// then look in the index
		auto attr = parts->attributes.get(COL_SEGMENT, segmentName);
		
		if (!attr)
			return nullptr;

		deleteAfterUsing = true;
		return attr->getBits();
	};

	auto deleteInterpreter = [&]()
	{
		if (interpreter)
		{
			if (interpreter->bits)
				delete interpreter->bits;
			delete interpreter;
			interpreter = nullptr;
		}
	};

	while (true)
	{

		deleteInterpreter();

		// clean up old objects

		// reset the linear iterator current index
		currentLinId = -1;

		int64_t expiredBy = 0;
		std::vector<std::string> cleanup;

		{ // scoped lock
			csLock lock(*parts->table->getSegmentLock());

			// sync the refresh map in the table object with our local refresh timer
			// first, lets grab the master list
			auto refreshList = parts->table->getSegmentRefresh();

			// add new segments to our refreshList (a map)
			for (auto seg : *refreshList)
				if (!parts->segmentRefresh.count(seg.first))
					parts->setSegmentRefresh(seg.first, seg.second.getRefresh());

			auto now = Now();

			// find expired and clean up
			for (auto seg : parts->segmentRefresh)
			{
				// if we didn't see it in the master list
				// lets destroy it in our list
				if (!refreshList->count(seg.first))
				{
					cleanup.push_back(seg.first);
					continue;
				}

				if (seg.second < now)
				{
					segmentName = seg.first;
					expiredBy = now - seg.second;
					break;
				}
			}

			// no expired segment found, lets reschedule
			if (expiredBy && segmentName.length())
			{
				// copy those macros
				macros = parts->table->getSegmentRefresh()->at(segmentName).macros;
			}
		}

		// do some cleanup
		for (auto seg : cleanup)
			parts->segmentRefresh.erase(seg);

		// if we didn't get an expired segment lets exit
		if (!expiredBy)
		{
			this->scheduleFuture(5000); // try again in 15 seconds
			return false;
		}
		
		// generate the index for this query	
		indexing.mount(table, macros, loop->partition, maxLinearId);
		bool countable;
		index = indexing.getIndex("_", countable);

		// create our bits, and zero them out
		maxLinearId = parts->people.peopleCount();
		auto bits = new IndexBits();
		bits->makeBits(maxLinearId, 0);

		// reset max linear
		maxLinearId = parts->people.peopleCount();

		// is this something we can calculate using purely
		// indexes? (nifty)
		if (countable && !macros.isSegmentMath)
		{
			bits->opCopy(*index);
			storeSegment(bits);
			delete bits;
			continue;
		}

		interpreter = new Interpreter(macros, openset::query::InterpretMode_e::count);
		interpreter->setGetSegmentCB(getSegmentCB);
		interpreter->setBits(bits, maxLinearId);

		auto mappedColumns = interpreter->getReferencedColumns();

		// clean the person object
		person.reinit();
		// map table, partition and select schema columns to the Person object
		person.mapTable(table, loop->partition, mappedColumns);

		// is this calculated using other segments (i.e. the functions
		// population, intersection, union, difference and compliment)
		// meaning we do not have to iterate user records
		if (macros.isSegmentMath)
		{
			interpreter->interpretMode = InterpretMode_e::count;

			interpreter->mount(&person);
			interpreter->exec();

			storeSegment(bits);
			continue;
		};

		// we have to execute actual code that iterates people
		return true;
	}
}

void OpenLoopSegmentRefresh::prepare()
{
	auto prepStart = Now();

	parts = table->getPartitionObjects(loop->partition);
	
	nextExpired();	
}

void OpenLoopSegmentRefresh::run()
{
	if (!interpreter && 
		!nextExpired())
	{
		this->scheduleFuture(5000);
		return;
	}

	if (!interpreter)
		return;

	openset::db::personData_s* personData;
	while (true)
	{
		if (sliceComplete())
			break; // let some other cells run
		
		// are we out of bits to analyze?
		if (interpreter->error.inError() || 
			!index->linearIter(currentLinId, maxLinearId))
		{

			// TODO - log error

			// add to resultBits upon query completion
			if (!interpreter->error.inError())
				storeSegment(interpreter->bits);
			
			// is there another query to run? If not we are done
			if (!nextExpired())
			{			
				this->scheduleFuture(5000);
				return;
			}

			// we have more macros, loop to the top and try again
			//continue;
			return;
		}
		
		if ((personData = parts->people.getPersonByLIN(currentLinId)) != nullptr)
		{
			++runCount;
			person.mount(personData);
			person.prepare();
			interpreter->mount(&person);
			interpreter->exec();
		}

		//++count;
	}
}
