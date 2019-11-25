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
        ++idx;
    }

    // build the reverse lookup - once
    auto bits = 0;
    while (true)
    {
        const auto size = pow(bits, 2);
        auto bucket = -1;
        for (auto &b : breakPoints)
            if (b.maxSize >= size)
            {
                bucket = b.index;
                break;
            }
        bucketLookup.push_back(bucket == 0 ? 1 : bucket);
        ++bits;

        if (size >= breakPoints.back().maxSize)
            break;
    }
}

void* PoolMem::getPtr(int64_t size)
{        
    // give us the starting bucket for iteration
    int64_t bucket = std::sqrt(size);

    // will iterate through bucekts of matching sqrt until one fits or we hit the end.
    // this will iteratate once or twice
    while (bucket < bucketLookup.size() && size > breakPoints[bucketLookup[bucket]].maxSize)
        ++bucket;

    // bucket index beyond lookup, so this is a non-pooled allocation
    if (bucket >= bucketLookup.size())
    {
        // this is a big allocation (outside our bucket sizes), so grab it from heap
        const auto alloc = reinterpret_cast<alloc_s*>(new char[size + MemConstants::PoolMemHeaderSize]);
        alloc->poolIndex = -1; // -1 = non-pooled
        return alloc->data;
    }

    // figure out which bucket size (if any) this allocation will fit
    auto &mem = breakPoints[bucketLookup[bucket]];

    csLock lock(mem.memLock);

    if (!mem.freed.empty())
    {
        const auto alloc = mem.freed.back();
        mem.freed.pop_back();
        alloc->poolIndex = mem.index;
        return alloc->data;
    }

    //reinterpret_cast<alloc_s*>(mem.heap.newPtr(mem.maxSize + MemConstants::PoolMemHeaderSize));
    const auto alloc = reinterpret_cast<alloc_s*>(new char[mem.maxSize + MemConstants::PoolMemHeaderSize]);
    alloc->poolIndex = mem.index;
    return alloc->data;
}

void PoolMem::freePtr(void* ptr)
{
    const auto alloc = reinterpret_cast<alloc_s*>(static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);

    if (alloc->poolIndex == -2) // already freed 
        return; // nice place for a breakpoint in debug

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

    // if a pool gets to large, trim it back
    if (mem.freed.size() > MemConstants::CullSize)
    {
        const auto cullTo = MemConstants::CullSize / 5;
        while (mem.freed.size() > cullTo)
        {
            delete [] reinterpret_cast<char*>(mem.freed.back());
            mem.freed.pop_back();
        }
    }
}

