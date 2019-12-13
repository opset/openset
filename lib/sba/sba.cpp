#include "sba.h"
#include <cmath>

using namespace std;

PoolMem::PoolMem()
{
    // set indexes in bucket objects
    auto idx = 0;
    for (auto &b : breakPoints)
    {
        b.index = idx;
        bucketLookup.push_back(b.maxSize);
        ++idx;
    }
}

void* PoolMem::getPtr(const int64_t size)
{
    int64_t bucket = 0;

    // will iterate through buckets of matching sqrt until one fits or we hit the end.
    // this will iterate once or twice
    while (bucket < 33 && size > bucketLookup[bucket])
        ++bucket;

    // bucket index beyond lookup, so this is a non-pooled allocation
    if (bucket >= 33)
    {
        // this is a big allocation (outside our bucket sizes), so grab it from heap
        const auto alloc = reinterpret_cast<alloc_s*>(new char[size + MemConstants::PoolMemHeaderSize]);
        alloc->poolIndex = -1; // -1 = non-pooled
        return alloc->data;
    }

    auto &mem = breakPoints[bucket];

    csLock lock(mem.memLock);

    if (!mem.freed.empty())
    {
        const auto alloc = mem.freed.back();
        mem.freed.pop_back();
        alloc->poolIndex = mem.index;
        return alloc->data;
    }

    const auto alloc = reinterpret_cast<alloc_s*>(mem.heap->newPtr(mem.maxSize + MemConstants::PoolMemHeaderSize));
    //const auto alloc = reinterpret_cast<alloc_s*>(new char[mem.maxSize + MemConstants::PoolMemHeaderSize]);
    alloc->poolIndex = mem.index;
    return alloc->data;
}

int PoolMem::getSize(void* ptr)
{
    const auto alloc = reinterpret_cast<alloc_s*>(static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);

    if (alloc->poolIndex == -2) // already freed
        return -2;

    // -1 means this was non-pooled so just delete it
    if (alloc->poolIndex == -1)
        return -1;

    return breakPoints[alloc->poolIndex].maxSize;
}

void PoolMem::freePtr(void* ptr)
{
    const auto alloc = reinterpret_cast<alloc_s*>(static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);

    if (alloc->poolIndex == -2) // already freed
    {
        return; // nice place for a breakpoint in debug
    }

    // -1 means this was non-pooled so just delete it
    if (alloc->poolIndex == -1)
    {
        delete[](static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);
        return;
    }

    auto& mem = breakPoints[alloc->poolIndex];

    csLock lock(mem.memLock);

    alloc->poolIndex = -2;
    mem.freed.push_back(alloc);
}

