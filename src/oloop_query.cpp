#include "oloop_query.h"
#include "indexbits.h"
#include "columns.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopQuery::OpenLoopQuery(
	ShuttleLambda<CellQueryResult_s>* shuttle,
	Table* table, 
	macro_s macros, 
	openset::result::ResultSet* result,
	int instance) :

	OpenLoop(oloopPriority_e::realtime), // queries are high priority and will preempt other running cells
	macros(macros),
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
		for (auto bits : interpreter->segmentIndexes)
			delete bits;

		delete interpreter;
	}
}

void OpenLoopQuery::prepare()
{
	auto prepStart = Now();	

	parts = table->getPartitionObjects(loop->partition);
	maxLinearId = parts->people.peopleCount();

	// generate the index for this query	
	indexing.mount(table, macros, loop->partition, maxLinearId);
	bool countable;
	index = indexing.getIndex("_", countable);
	population = index->population(maxLinearId);

	//Logger::get().info(' ', to_string(loop->partition) + " has " + to_string(population) + " stop @ " + to_string(maxLinearId));

	interpreter = new Interpreter(macros);
	interpreter->setResultObject(result);

	// if we are in segment compare mode:
	if (macros.segments.size())
	{
		std::vector<IndexBits*> segments;

		for (const auto segmentName : macros.segments)
		{
			auto attr = parts->attributes.get(COL_SEGMENT, MakeHash(segmentName));
			if (attr)
				segments.push_back(attr->getBits());
			else
				segments.push_back(new IndexBits());
		}

		interpreter->setCompareSegments(index, segments);
	}


	auto mappedColumns = interpreter->getReferencedColumns();

	// map table, partition and select schema columns to the Person object
	person.mapTable(table, loop->partition, mappedColumns);
	
	startTime = Now();
}

void OpenLoopQuery::run()
{
	auto count = 0;
	openset::db::personData_s* personData;
	while (true)
	{
		if (sliceComplete())
			break;

		/*		
		// is the cluster broken?
		if (!OpenSet::globals::mapper->partitionMap.isClusterComplete(OpenSet::globals::running->partitionMax, { OpenSet::mapping::NodeState_e::active_owner }))
		{
			// the cluster is broken, so return an error
			partitionRemoved();
			suicide();
			return; 
		}
		*/

		// are we done? This will return the index of the 
		// next set bit until there are no more, or maxLinId is met
		if (interpreter->error.inError() || !index->linearIter(currentLinId, maxLinearId))
		{
			auto time = Now() - startTime;

			shuttle->reply(
				0, 
				CellQueryResult_s{				
					//res,
					time,
					runCount,
					population,
					maxLinearId,
					instance,
					interpreter->error,
					parts
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
		}

		++count;
	}
}

void OpenLoopQuery::partitionRemoved()
{
	shuttle->reply(
		0,
		CellQueryResult_s{
		//res,
		0,
		0,
		0,
		0,
		instance,
		openset::errors::Error{
			openset::errors::errorClass_e::run_time,
			openset::errors::errorCode_e::partition_migrated,
			"please retry query"
		},
		parts
	});
}
