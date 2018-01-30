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
	async::OpenLoop* insertCell = new async::OpenLoopInsert(this);
	insertCell->scheduleFuture(1000); // run this in 1 second
	asyncLoop->queueCell(insertCell);

	async::OpenLoop* segmentRefreshCell = new async::OpenLoopSegmentRefresh(this);
	segmentRefreshCell->scheduleFuture(15000); // run this in 15 seconds
	asyncLoop->queueCell(segmentRefreshCell);

	async::OpenLoop* cleanerCell = new async::OpenLoopCleaner(table);
	cleanerCell->scheduleFuture(30000); // start this in 30 seconds
	asyncLoop->queueCell(cleanerCell);

}

TablePartitioned::~TablePartitioned()
{
    csLock lock(insertCS);

    for (auto item : insertQueue)
        delete [] item;

    insertQueue.clear();
}
