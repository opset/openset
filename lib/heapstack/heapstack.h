#pragma once
/*

Heap Stack - a convenient way to allocation millions of small structures
             quickly and make them go away just as fast.

The MIT License (MIT)

Copyright (c) 2015 Seth A. Hamilton

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

#include <vector>
#include <mutex>
#include <iostream>
#include "threads/locks.h"

using namespace std;

// constants used by HeapStack and PoolMem
namespace MemConstants
{
	const int64_t HeapStackBlockSize = 256LL * 1024LL;
}

class HeapStackBlockPool
{
private:
	std::vector<void*> pool;
	CriticalSection poolLock;

    HeapStackBlockPool() = default;

public:

	// singlton
	static HeapStackBlockPool& getPool()
	{
        static HeapStackBlockPool globalPool{};
		return globalPool;
	}

	inline void* Get()
	{
		{ // scope the lock
			csLock lock(poolLock);

			if (!pool.empty())
			{
				auto block = pool.back();
				pool.pop_back();
				return block;
			}
		}
		return new char[MemConstants::HeapStackBlockSize];
	}

	inline void Put(void* item)
	{
		csLock lock(poolLock);
		pool.push_back(item);
	}


	int32_t blockCount() const
	{
		return static_cast<int>(pool.size());
	}

};


class HeapStack
{
private:

	// this is the block structure, blocks of heap memory cast to this type will ultimately
	// become our stack(s). 
	// Note: alignment forced
#pragma pack(push,1)
	struct block_s
	{
		block_s* nextBlock{ nullptr };
		int64_t endOffset{ 0 };
		bool nonpooled{ false };
		char data[1]; // fake size, we will be casting this over a buffer
	};
#pragma pack(pop)

	const int64_t headerSize{ sizeof(block_s) - 1LL }; // size of block header, minus the 1 byte 'data' array
	const int64_t blockSize{ MemConstants::HeapStackBlockSize };
	const int64_t dataSize{ MemConstants::HeapStackBlockSize - headerSize };

	int64_t blocks{ 0 };
	int64_t bytes{ 0 };

	block_s* head{ nullptr };
	block_s* tail{ nullptr };

public:

	// constructor, default allocates 4 meg blocks. 
	HeapStack();
	~HeapStack();

private:
	void Release();

public:
	// newPtr - returns a pointer to a block of memory of "size"
	inline char* newPtr(int64_t size)
	{
		if (size >= dataSize)
			newNonpooledBlock(size);
		else if (!tail || tail->endOffset + size >= dataSize)
			newBlock();

		char* insertPtr = tail->data + tail->endOffset;
		tail->endOffset += size;
		bytes += size;
		return insertPtr;
	}

	void reset();

	// currentData - returns a pointer to current memory block
	char* currentData() const;

	char* getHeadPtr() const;

	block_s* firstBlock() const;

	// getSizeBytes - returns how many bytes are being used by DATA in the block stack.
	int64_t getBytes() const;

	// getAllocated - returns how many bytes are used by the raw blocks in the block stack
	int64_t getAllocated() const;

	// getBlocks - returns how many blocks are within the block stack
	int64_t getBlocks() const;

	// flatten - returns a contiguous block of memory containing the data within all the blocks.
	//
	// returns pointer made with pooled mem, must be deleted with pooled mem
	char* flatten() const;

	// flatten - same as basic flatten but returns length via reference param
	char* flatten(int64_t& length) const;

	// release a flattened pointer here
	static void releaseFlatPtr(char* flatPtr);

private:
	// newBlock - adds a new block to the list of blocks, updates the block links.
	void newBlock();
	void newNonpooledBlock(int64_t size);
};
