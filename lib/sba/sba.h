#pragma once
#include <vector>
#include <mutex>
#include "threads/locks.h"

namespace MemConstants
{
	const int64_t PoolMemHeaderSize = 4;
	const int PoolBuckets = 257;
	const int PoolBucketOffset = 4;
	const int PoolBucketAlign = 8;
    const int CullSize = 10;
}

class PoolMem
{
private:

#pragma pack(push,1)
	struct alloc_s
	{
		int32_t poolIndex;
		char data[1];
	};
#pragma pack(pop)

	struct memory_s
	{
		CriticalSection memLock;
		int32_t index{ 0 };
		const int64_t maxSize;
		std::vector<alloc_s*> freed;

		memory_s(const int64_t maxSize) :
			maxSize(maxSize)
		{}
	};

	std::vector<memory_s> breakPoints = {
		{ 16 },
		{ 20 },
		{ 24 },
		{ 28 },
		{ 36 },
		{ 52 },
		{ 64 },
		{ 100 },
		{ 144 },
		{ 256 },
		{ 400 },
		{ 576 },
		{ 784 },
		{ 1024 },
		{ 1296 },
		{ 1600 },
		{ 1936 },
		{ 2304 },
		{ 2704 },
		{ 3136 },
		{ 3600 },
		{ 4096 },
		{ 4624 },
		{ 5184 },
		{ 5776 },
		{ 6400 },
		{ 7056 },
		{ 7744 },
		{ 9216 },
		{ 10816 },
		{ 12544 },
		{ 14400 },
		{ 16384 },
/*		{ 18496 },
		{ 20736 },
		{ 23104 },
		{ 25600 },
		{ 28224 },
		{ 30976 },
		{ 33856 },
		{ 36864 },
		{ 40000 },
		{ 43264 },
		{ 46656 },
		{ 50176 },
		{ 53824 },
		{ 57600 },
		{ 61504 },
		{ 65536 }, */
	};

	std::vector<int> bucketLookup;

	PoolMem();
	~PoolMem() = default; // we never clean anything up, this is forever.

public:

	// singleton 
	static PoolMem& getPool()
	{
		static PoolMem pool;
		return pool;
	}

	void* getPtr(int64_t size);
	void freePtr(void* ptr);
};

//extern PoolMem* POOL;
