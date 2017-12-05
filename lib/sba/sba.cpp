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
		auto size = pow(bits, 2);
		auto bucket = -1;
		for (auto &b : breakPoints)
			if (b.maxSize >= size)
			{
				bucket = b.index;
				break;
			}
		bucketLookup.push_back(bucket == 0 ? 1 : bucket);
		++bits;

		if (size >= 16384)
			break;
	}

}

PoolMem::~PoolMem()
{}

void* PoolMem::getPtr(int64_t size)
{
	// give us the starting bucket for iteration
	int64_t bucket = std::sqrt(size);

	// will iterate through bucekts of matching sqrt until one fits or we hit the end
	const auto lookupSize = bucketLookup.size();
	while (bucket < lookupSize && size > breakPoints[bucketLookup[bucket]].maxSize)
		++bucket;

	// bucket index beyond lookup, so this is made on the heap
	if (bucket >= bucketLookup.size())
	{
		// this is a big allocation (outside our bucket sizes), so grab it from heap
		auto alloc = reinterpret_cast<alloc_s*>(new char[size + MemConstants::PoolMemHeaderSize]);
		alloc->poolIndex = -1;
		return alloc->data;
	}

	// figure out which bucket size (if any) this allocation will fit
	auto &mem = breakPoints[bucketLookup[bucket]];

	csLock lock(mem.memLock);

	++mem.allocated;

	if (mem.freed.size())
	{
		auto alloc = mem.freed.back();
		mem.freed.pop_back();
		alloc->poolIndex = mem.index;
		return alloc->data;
	}

	auto alloc = reinterpret_cast<alloc_s*>(mem.heap.newPtr(mem.maxSize + MemConstants::PoolMemHeaderSize));
	alloc->poolIndex = mem.index;
	return alloc->data;
}

void PoolMem::freePtr(void* ptr)
{
	auto alloc = reinterpret_cast<alloc_s*>(static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);

    if (alloc->poolIndex == -2) // already freed
        return;

    // -1 is a big block that did not fit in a pool
	if (alloc->poolIndex == -1)
	{
		delete[](static_cast<char*>(ptr) - MemConstants::PoolMemHeaderSize);
		return;
	}

	auto& mem = breakPoints[alloc->poolIndex];

	csLock lock(mem.memLock);

	--mem.allocated;
	
	alloc->poolIndex = -2;
	mem.freed.push_back(alloc);

	if (!mem.allocated)
	{
		mem.freed.clear();
		mem.heap.reset();
	}
}

//PoolMem* POOL = new PoolMem();

