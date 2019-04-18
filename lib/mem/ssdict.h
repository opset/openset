#pragma once

#include "../include/libcommon.h"
#include "../heapstack/heapstack.h"


const int ssPageLength = (1 << 4) - 1;

class sspool
{
public:

	HeapStack mem;
	std::vector<std::stack<uint8_t*>> freePool;

	sspool() :
		mem(),
		freePool(15, std::stack<uint8_t*>()) // allocate a block stack using defaults
	{ };

	~sspool()
	{ };

	void debug()
	{
		printf("Free Pool\r\n");
		for (auto i = 0; i < 15; i++)
		{
			printf("%i = %zi\r\n", i, freePool[i].size());
		}

		std::cout << mem.getBytes() << "\r\n";
	}

	uint8_t* newPtr(int bits, int size)
	{
		uint8_t* tPtr;

		if (freePool[bits].empty())
			return reinterpret_cast<uint8_t*>(mem.newPtr(size));

		tPtr = freePool[bits].top();
		freePool[bits].pop();

		return tPtr;
	}

	void freePtr(int bits, void* block)
	{
		freePool[bits].push(static_cast<uint8_t*>(block));
	}
};



template <typename K, typename V>
struct ssDictPage
{
public:

#pragma pack(push,1)
	struct dictPage_s
	{
		K key;
		V value;
		ssDictPage* rangeNext;
	};
	using Page = dictPage_s;
	
	ssDictPage* next;
	Page*      page;
	int8_t     bits;
	int8_t     used;


#pragma pack(pop)

	ssDictPage(sspool* mem) :
		used(0),
		bits(1),
		next(nullptr)
	{
		page = (dictPage_s*)mem->newPtr(1, sizeof(dictPage_s) * ((1 << bits) - 1));
	}

	void resize(sspool* mem)
	{
		auto currentSize = (1 << bits)-1;

		if (used < currentSize)
			return;

		++bits;

		auto newPage = (dictPage_s*)mem->newPtr(bits, sizeof(dictPage_s) * ((1 << bits) - 1));
		memcpy(newPage, page, currentSize * sizeof(dictPage_s));
		mem->freePtr(bits - 1, page);
		page = newPage;		
	}

	inline int getIndex(K key)
	{
		int first = 0, last = used - 1, mid = last >> 1;
/*
		cout << "----" << endl;
		for (auto i = 0; i < used; i++)
			cout << page[i].key << endl;
*/
		while (first <= last)
		{
			if (key > page[mid].key)
				first = mid + 1;
			else if (key < page[mid].key)
				last = mid - 1;
			else 
				return mid; // exact match
			
			mid = (first + last) >> 1;
		}

		if (first == ssPageLength)
			return -2000;

		if (used < ssPageLength)
			return -(first + 1);
		else
			return first;

		return used;

		/*
		for (register auto i = 0; i < used; ++i)
			if ((page+i)->key >= key)
				return (used < ssPageLength) ? -(i + 1) : i;

		return (used == ssPageLength) ? -2000 : used;
*		 */
	}

	inline void slide(int index)
	{
		memmove(page + index + 1, page + index, sizeof(dictPage_s) * ((1 << bits) - 1 - index - 1));
	}

};

template <typename K, typename V>
class ssDict
{
public:

	using Page = ssDictPage<K, V>;
	using Row = typename ssDictPage<K, V>::dictPage_s;
	sspool mem;

	Page* root;
	int branches;

	ssDict() :
		branches(0)
	{
		void* d = mem.mem.newPtr(sizeof(Page));
		root = new (d) Page(&mem);
	}

	void set(K key, V value)
	{
		Page* current = root;
		int ptr;

		while (true)
		{
			ptr = current->getIndex(key);

			if (ptr == -2000)
			{
				if (!current->next)
				{
					void* d = mem.mem.newPtr(sizeof(Page));
					current->next = new (d) Page(&mem);
					++branches;
				}
				current = current->next;
			}
			else if (ptr < 0)
			{
				ptr = -ptr - 1;
				current->resize(&mem);
				current->slide(ptr);
				current->page[ptr].key = key;
				current->page[ptr].value = value;
				current->page[ptr].rangeNext = nullptr;
				++current->used;
				return;
			}
			else
			{
				if (ptr == current->used)
				{
					current->resize(&mem);
					current->page[ptr].key = key;
					current->page[ptr].value = value;
					current->page[ptr].rangeNext = nullptr;
					++current->used;
					return;
				}
				else if (current->page[ptr].key != key)
				{
					if (!current->page[ptr].rangeNext)
					{
						void* d = mem.mem.newPtr(sizeof(Page));
						current->page[ptr].rangeNext = new (d) Page(&mem);
						++branches;
					}
					current = current->page[ptr].rangeNext;
				}
				else
				{
					current->page[ptr].key = key;
					current->page[ptr].value = value;
					return;
				}

			}
		}
		
	}

	bool get(K key, V &value)
	{
		Page* current = root;
		int ptr;

		while (true)
		{
			ptr = current->getIndex(key);

			if (ptr == -2000)
			{
				if (current->next)
					current = current->next;
				else
					return false;
				continue;
			}

			if (ptr < 0)
				ptr = -ptr - 1;
			
			if (current->page[ptr].key == key)
			{
				value = current->page[ptr].value;
				return true;
			}
			else
			{
				current = current->page[ptr].rangeNext;
				if (!current)
					return false;
			}
		
		}

	}

};