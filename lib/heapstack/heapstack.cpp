#include "heapstack.h"
#include <cstring>

#include "../sba/sba.h"

using namespace std;

HeapStack::~HeapStack()
{
	Release();

	if (head) // lets not leak the last block left by release
    {
		if (head->nonpooled)
			delete[] reinterpret_cast<char*>(head);
		else
			HeapStackBlockPool::getPool().Put(head);
    }
}

void HeapStack::Release()
{
    // if the stack is empty we will just reset the class members
    // for good measure, if it was full we will remove all linked 
    // pages except the first, which will reset to initial state
	if (head)
	{
		auto block = head->nextBlock;

		while (block)
		{
			--blocks;
			const auto t = block->nextBlock;
			if (block->nonpooled)
				delete[] reinterpret_cast<char*>(block);
			else
				HeapStackBlockPool::getPool().Put(block);
			block = t;
		}
	}

	blocks = (head) ? 1 : 0;
	bytes = 0;

	if (!head)
	{
		head = nullptr;
		tail = nullptr;
	}
	else
	{
		head->endOffset = 0;
		head->nextBlock = nullptr;
		tail = head;
	}
}

void HeapStack::reset()
{
	Release();
}

char* HeapStack::currentData() const
{
	return tail->data;
}

char* HeapStack::getHeadPtr() const
{
	if (!head)
		return nullptr;

	return head->data;
}

HeapStack::block_s* HeapStack::firstBlock() const
{
	return head;
}

int64_t HeapStack::getBytes() const
{
	return bytes;
}

int64_t HeapStack::getAllocated() const
{
	return blocks * blockSize;
}

int64_t HeapStack::getBlocks() const
{
	return blocks;
}

char* HeapStack::flatten() const
{
	if (!head || !bytes)
		return nullptr;

	const auto buff = static_cast<char*>(PoolMem::getPool().getPtr(bytes));
	auto write = buff;

	auto block = head;

	while (block)
	{
		memcpy(write, block->data, block->endOffset);
		write += block->endOffset;

		block = block->nextBlock;
	}

	return buff;
}

char* HeapStack::flatten(int64_t& length) const
{
	length = bytes;
	return flatten();
}

void HeapStack::releaseFlatPtr(char* flatPtr)
{
	PoolMem::getPool().freePtr(flatPtr);
}

void HeapStack::newBlock()
{
	const auto block = reinterpret_cast<block_s*>(HeapStackBlockPool::getPool().Get());

	block->nextBlock = nullptr;
	block->endOffset = 0;
	block->nonpooled = false;

	++blocks;

	// link up the blocks, assign the head if needed, move the tail along
	if (tail)
		tail->nextBlock = block;

	if (!head)
		head = block;

	tail = block;
}

void HeapStack::newNonpooledBlock(int64_t size)
{
	const auto block = reinterpret_cast<block_s*>(new char[size + headerSize]);

	block->nextBlock = nullptr;
	block->endOffset = 0;
	block->nonpooled = true;

	++blocks;

	// link up the blocks, assign the head if needed, move the tail along
	if (tail)
		tail->nextBlock = block;

	if (!head)
		head = block;

	tail = block;
}
