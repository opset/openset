#include "tablepartitioned.h"
#include "asyncpool.h"
#include "oloop_insert.h"
#include "oloop_seg_refresh.h"
#include "oloop_cleaner.h"
#include "sidelog.h"

using namespace openset::db;

TablePartitioned::TablePartitioned(
	Table* table,
	const int partition, 
	AttributeBlob* attributeBlob, 
	Columns* schema) :
		table(table),
		partition(partition),
		attributes(partition, table, attributeBlob, schema),
		attributeBlob(attributeBlob),
		people(partition),
		asyncLoop(openset::globals::async->getPartition(partition)),
		triggers(new openset::revent::ReventManager(this)),
		insertBacklog(0)
{	
    // this will stop any translog purging until the insertCell (below) 
    // gets to work.
    SideLog::getSideLog().resetReadHead(table, partition);

    const auto sharedTablePtr = table->getSharedPtr();

	async::OpenLoop* insertCell = new async::OpenLoopInsert(sharedTablePtr);
	insertCell->scheduleFuture(1000); // run this in 1 second
	asyncLoop->queueCell(insertCell);
        
	async::OpenLoop* segmentRefreshCell = new async::OpenLoopSegmentRefresh(sharedTablePtr);
	segmentRefreshCell->scheduleFuture(table->segmentInterval); 
	asyncLoop->queueCell(segmentRefreshCell);
        
	async::OpenLoop* cleanerCell = new async::OpenLoopCleaner(sharedTablePtr);
	cleanerCell->scheduleFuture(table->maintInterval); 
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

