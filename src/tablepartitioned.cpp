#include "tablepartitioned.h"
#include "asyncpool.h"
#include "oloop_insert.h"
#include "oloop_seg_refresh.h"

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
		insertBacklog(0),
		triggers(new openset::revent::ReventManager(this))
{	
	async::OpenLoop* insertCell = new async::OpenLoopInsert(this);
	insertCell->scheduleFuture(1000); // run this in 1 second
	asyncLoop->queueCell(insertCell);

	async::OpenLoop* segmentRefreshCell = new async::OpenLoopSegmentRefresh(this);
	segmentRefreshCell->scheduleFuture(15000); // run this in 15 seconds
	asyncLoop->queueCell(segmentRefreshCell);
}
