#include "sortedhashtree.h"

TreeMemory* TreePool;

TreeMemory::TreeMemory()
	: TreeCS()
{
	TreeCS.lock();

	for (int i = 0; i < 2048; i++)
		ReturnBlock(new char[ SHT_BLOCKSIZE ], false);

	TreeCS.unlock();
}

TreeMemory::~TreeMemory()
{
	char* block;

	while (!this->BlockList.empty())
	{
		block = this->BlockList.top();
		this->BlockList.pop();
		delete [] block;
	}
}

char* TreeMemory::GetBlock()
{
	char* block;

	TreeCS.lock();

	if (!BlockList.empty())
	{
		block = BlockList.top();
		BlockList.pop();
		TreeCS.unlock();

		return block;
	}
	else
	{
		block = new char[ SHT_BLOCKSIZE ];
		TreeCS.unlock();

		return block;
	}
}

void TreeMemory::ReturnBlock(char* block, bool lock)
{
	if (lock)
		TreeCS.lock();

	if (BlockList.size() > 2048)
		delete [] block;
	else
		BlockList.push(block);

	if (lock)
		TreeCS.unlock();
}

void InitializeSortedHashTree()
{
	TreePool = new TreeMemory();
};
