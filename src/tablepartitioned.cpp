#include "tablepartitioned.h"
#include "asyncpool.h"
#include "oloop_insert.h"
#include "oloop_seg_refresh.h"
#include "oloop_cleaner.h"

using namespace openset::db;

TablePartitioned::TablePartitioned(
	Table* table,
	const int partition, 
	AttributeBlob* attributeBlob, 
	Columns* schema) :
		table(table),
		partition(partition),
		attributes(partition, attributeBlob, schema),
		attributeBlob(attributeBlob),
		people(partition),
		asyncLoop(openset::globals::async->getPartition(partition)),
		triggers(new openset::revent::ReventManager(this)),
		insertBacklog(0)
{	
    const auto sharedTablePtr = table->getSharedPtr();

	async::OpenLoop* insertCell = new async::OpenLoopInsert(sharedTablePtr);
	insertCell->scheduleFuture(1000); // run this in 1 second
	asyncLoop->queueCell(insertCell);

	async::OpenLoop* segmentRefreshCell = new async::OpenLoopSegmentRefresh(sharedTablePtr);
	segmentRefreshCell->scheduleFuture(30'000 + randomRange(10'000, -10'000)); // run this in 15 seconds add shuffle
	asyncLoop->queueCell(segmentRefreshCell);
        
	async::OpenLoop* cleanerCell = new async::OpenLoopCleaner(sharedTablePtr);
	cleanerCell->scheduleFuture(3'600'000 + randomRange(300'000, -300'000)); // start this in 90 seconds add shuffle
	asyncLoop->queueCell(cleanerCell);

}

TablePartitioned::~TablePartitioned()
{
    if (triggers)
        delete triggers;

    csLock lock(insertCS);

    for (auto item : insertQueue)
        PoolMem::getPool().freePtr(item);

    insertQueue.clear();
}
