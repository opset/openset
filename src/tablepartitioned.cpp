#include "tablepartitioned.h"
#include "asyncpool.h"
#include "oloop_insert.h"
#include "oloop_seg_refresh.h"
#include "oloop_cleaner.h"
#include "sidelog.h"
#include "queryinterpreter.h"

using namespace openset::db;

SegmentPartitioned_s::~SegmentPartitioned_s()
{
    if (bits)
        delete bits;

    if (interpreter)
        delete interpreter;
}

openset::db::IndexBits* openset::db::SegmentPartitioned_s::prepare(Attributes& attributes)
{
    if (bits)
        return bits;

    changeCount = 0;
    const auto attr = attributes.getMake(COL_SEGMENT, segmentName);
    bits = new IndexBits();
    bits->mount(attr->index, attr->ints, attr->ofs, attr->len, attr->linId);

    return bits;
}

void openset::db::SegmentPartitioned_s::commit(Attributes& attributes)
{
    if (changeCount)
        attributes.swap(COL_SEGMENT, MakeHash(segmentName), bits);
    changeCount = 0;
}

openset::db::SegmentPartitioned_s::SegmentChange_e openset::db::SegmentPartitioned_s::setBit(int64_t linearId, bool state)
{
    const auto currentState = bits->bitState(linearId);
    if (state && !currentState)
    {
        ++changeCount;
        bits->bitSet(linearId);
        return SegmentChange_e::enter;
    }

    if (!state && currentState)
    {
        ++changeCount;
        bits->bitClear(linearId);
        return SegmentChange_e::exit;
    }

    return SegmentChange_e::noChange;
}

openset::query::Interpreter * openset::db::SegmentPartitioned_s::getInterpreter(int64_t maxLinearId)
{
    if (!interpreter)
        interpreter = new openset::query::Interpreter(macros, openset::query::InterpretMode_e::count);

    if (!bits)
        throw std::runtime_error("call prepare before calling getInterpreter");

    interpreter->setBits(bits, maxLinearId);

    return interpreter;
}

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
        //triggers(new openset::revent::ReventManager(this)),
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
    //if (triggers)
      //  delete triggers;

    csLock lock(insertCS);

    for (auto item : insertQueue)
        PoolMem::getPool().freePtr(item);

    insertQueue.clear();
}

openset::query::Interpreter* TablePartitioned::getInterpreter(const std::string& segmentName, int64_t maxLinearId)
{
     if (!segments.count(segmentName))
         return nullptr;

    return segments[segmentName].getInterpreter(maxLinearId);
}

void TablePartitioned::checkForSegmentChanges()
{
    // if segment calculations are taking place in an open-loop
    // we will not change or invalidate any segment records
    //
    // OpenLoopSegment and OpenLoopSegmentRefresh will increment on
    // prepare, and decrement on exit
    if (segmentUsageCount)
        return;

    storeAllChangedSegments();

    std::vector<std::string> orphanedSegments;
    InterpreterList onInsertList;

    { // scope a lock
        csLock lock(*table->getSegmentLock());

        // sync the refresh map in the table object with our local refresh timer
        // first, lets grab the master list
        const auto masterRefreshList = table->getSegmentRefresh();

        bool changed = false;

        // add new or changed segments from master to partition
        for (auto& seg : *masterRefreshList)
        {
            // add or replace segments that are new or different
            if (!segments.count(seg.first) || seg.second.lastModified != segments[seg.first].lastModified)
            {
                const auto segmentName = seg.first;

                auto newSegment = SegmentPartitioned_s(
                    seg.second.segmentName,
                    seg.second.macros,
                    seg.second.lastModified,
                    seg.second.zIndex,
                    seg.second.onInsert);

                newSegment.lastModified = seg.second.lastModified;

                segments.erase(segmentName);
                segments.emplace(segmentName, newSegment);

                segmentTTL[segmentName] = table->getSegmentTTL()->count(segmentName) ?
                    (*table->getSegmentTTL())[segmentName].TTL : 0;

                // force immediate refresh
                segmentRefresh.erase(segmentName);

                changed = true;
            }
        }

        for (auto& seg: segments)
        {
            // do we have a partition segment not in the master (deleted?)
            if (!masterRefreshList->count(seg.first))
                orphanedSegments.push_back(seg.first);
            else if (seg.second.onInsert)
                onInsertList.push_back(&seg.second);
        }
    }

    // delete any segments in the cleanup list
    for (auto &segName : orphanedSegments)
        segments.erase(segName);

    std::sort(
        onInsertList.begin(),
        onInsertList.end(),
        [](const SegmentPartitioned_s* left, const SegmentPartitioned_s* right ) -> bool
        {
            return left->zIndex > right->zIndex;
        });

    onInsertSegments = std::move(onInsertList);
}

std::function<openset::db::IndexBits*(const string&, bool&)> TablePartitioned::getSegmentCallback()
{

    return [this](const std::string& segmentName, bool &deleteAfterUsing) -> IndexBits*
    {
        if (this->segments.count(segmentName))
        {
            deleteAfterUsing = false;
            return this->segments[segmentName].prepare(this->attributes);
        }

        // if there are no bits with this name created in this query
        // then look in the index
        auto attr = this->attributes.get(COL_SEGMENT, segmentName);

        if (!attr)
            return nullptr;

        deleteAfterUsing = true;
        return attr->getBits();
    };

}

void TablePartitioned::storeAllChangedSegments()
{
    for (auto& seg: segments)
        seg.second.commit(attributes);
}

openset::db::IndexBits* TablePartitioned::getBits(std::string& segmentName)
{
    if (this->segments.count(segmentName))
        return this->segments[segmentName].prepare(attributes);

    return nullptr;
}

void TablePartitioned::pushMessage(const int64_t segmentHash, const SegmentPartitioned_s::SegmentChange_e state, std::string uuid)
{
    //if (!messages.count(segmentHash))
    messages[segmentHash].emplace_back(
       revent::TriggerMessage_s {
            segmentHash,
            state == SegmentPartitioned_s::SegmentChange_e::enter
                ? openset::revent::TriggerMessage_s::State_e::entered
                : openset::revent::TriggerMessage_s::State_e::exited,
            uuid
        }
    );

    //cout << "hash: " << segmentHash << "  state: " << (state == SegmentPartitioned_s::SegmentChange_e::enter ? "in" : "out") << "  uuid: " << uuid << endl;

/*        messages.emplace(
            segmentHash,
            revent::TriggerMessage_s {
                segmentHash,
                entered ? revent::TriggerMessage_s::State_e::entered : revent::TriggerMessage_s::State_e::exited,
                uuid
            });*/
}

void TablePartitioned::flushMessageMessages()
{
    if (!messages.size())
        return;

    csLock lock(globals::running->cs);

    for (auto &&t : messages)
    {
        // push our local message cache to the main cache so it can dispatched
        table->getMessages()->push(t.first, t.second);

        // empty our local cache now that it has been pusehd to the main caches
        t.second.clear();
    }
}
