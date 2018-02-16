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
		attributes(partition, table, attributeBlob, schema),
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

void openset::db::TablePartitioned::serializeInsertBacklog(HeapStack * mem)
{
    csLock lock(insertCS);

    const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
	(*sectionLength) = static_cast<int64_t>(insertQueue.size());

    for (auto item : insertQueue)
    {
        const auto itemLength = recast<int32_t*>(mem->newPtr(sizeof(int32_t)));
	    (*itemLength) = static_cast<int32_t>(strlen(item));  
        const auto itemPtr = mem->newPtr(*itemLength);
        memcpy(itemPtr, item, *itemLength);
    }

    cout << ("serialized " + to_string(*sectionLength)) << endl;
}

int64_t openset::db::TablePartitioned::deserializeInsertBacklog(char * mem)
{
    csLock lock(insertCS);

	auto read = mem;
    
    const auto sectionLength = *recast<int64_t*>(read);
    read += sizeof(int64_t);

    cout << ("deserialized " + to_string(sectionLength)) << endl;

    for (auto i = 0; i < sectionLength; ++i)
    {
        const auto itemLength = *recast<int32_t*>(read);
        read += sizeof(int32_t);

        const auto itemPtr = static_cast<char*>(PoolMem::getPool().getPtr(itemLength + 1));
        memcpy(itemPtr, read, itemLength);
        itemPtr[itemLength] = 0;

        insertQueue.push_back(itemPtr);

        read += itemLength;        
    }

    return read - mem;
}


