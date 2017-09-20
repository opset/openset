#pragma once
/*

Binary List Hash - a hash for big data. If you have millions of 10's of millions
of things to fit into a hash and memory overhead is a concern, use this.

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

#include <cstdio>
#include <iostream>
#include <stack>
#include <vector>
#include <limits>
#include <algorithm>
#include "../heapstack/heapstack.h"

typedef uint8_t tBranch8;
const int32_t LINEARSCAN = 32;

// pool of sub-blocks
//
// BinListDict16 uses 17 different allocation sizes.
// as pages in the tree grow they need to be resized.
// the old page is returned here so it can be reused.
// in practice this is very efficient for both total
// memory size, as well as reducing fragmentation.
//

class ShortPtrPool8
{
public:

	HeapStack mem;
	std::vector<std::stack<uint8_t*>> freePool;

	ShortPtrPool8() :
		mem(),
		freePool(9, std::stack<uint8_t*>()) // allocate a block stack using defaults
	{ };

	~ShortPtrPool8()
	{ };

	void debug()
	{
		printf("Free Pool\r\n");
		for (auto i = 0; i < 9; i++)
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

template <typename tKey, typename tVal>
class BinListDict8
{
private:
#pragma pack(push,1)

	struct bl_element_s
	{
		tBranch8 valueWord;
		void* next;
	};

	struct bl_array_s
	{
		int8_t pageBits;
		int16_t used;
		bl_element_s nodes[256]; // all properties must be above this array
	};

	int sizeofArrayHeader = 3; // pageBits + used in 1 byte aligned mem
	/*
		overlay turns a key of size<tKey> into an array of ints (16 bit unsigned numbers)
		also... it will pad and fill the last word if the tKey is an odd length
	struct overlay
	{
		//uint16_t ints[(sizeof(tKey) / 2) + 1]; // array of ints same size as tKey + overflow word
		tBranch8 ints[sizeof(tKey)];
		int elements;

		overlay():
			elements(0)
		{}

		explicit overlay(tKey* value)
		{
			set(value);
		}

		void set(tKey* value)
		{
			elements = sizeof(tKey) / 2;
			memcpy(ints, value, sizeof(tKey));
		}

		tBranch8* set(tKey* value, tBranch8* & iter)
		{
			set(value);
			iter = ints + elements;
			return reinterpret_cast<tBranch8*>(&ints);
		}

		// returns end pointer to ints, returns the start iterator (+1 word, so decrement first) for loop as reference
		// keys are reference in reverse
		tBranch8* getListPtr(tBranch8* & iter)
		{
			iter = ints + elements;
			return static_cast<tBranch8*>(&ints);
		}
	};
*/

#pragma pack(pop)

	ShortPtrPool8 mem; // the list pool
	bl_array_s* root; // root node for hash tree

	// it runs faster when these are globals, less stack work
	// is my guess
	uint8_t *iter, *ints;
	bl_array_s *node, *lastNode, *newNode;
	int index, lastIndex;

	bool isSigned;

public:

	BinListDict8() :
		root(nullptr), 
		mem(),
		isSigned(numeric_limits<tVal>::is_signed)//, over()
	{
		root = createNode(8); // make a full sized root node
	}

	~BinListDict8()
	{}

	// debug - dumps usage data for the memory manager
	// 
	// shows how many cached/recycled lits are available
	//
	void debug()
	{
		mem.debug();
	}

	// set - set a key/value in the hash
	//
	// note: adding an existing key will overwrite with a
	//       provided value.
	//
	void set(tKey key, tVal value)
	{
		/*
		tBranch8* ints = (uint8_t*)&key;
		tBranch8* iter = ints + sizeof(tKey);
		bl_array_s* node = root, *lastNode = node, *newNode;
		int index, lastIndex = 0;
		*/

		ints = (uint8_t*)&key;
		iter = ints + sizeof(tKey);
		node = root;
		lastNode = node;
		lastIndex = 0;

		while (--iter)
		{
			if ((index = getIndex(node, *iter)) >= 0)
			{
				lastNode = node;
				lastIndex = index;

				if (iter == ints) // we are at the end
				{
					//tVal* tvalue = (tVal*)node->nodes[index].next;
					memcpy(&node->nodes[index].next, &value, sizeof(tVal));
					return;
				}

				node = static_cast<bl_array_s*>(node->nodes[index].next);
			}
			else
			{
				index = -index - 1;

				// make a space in current node
				node = makeGap(node, index, lastNode, lastIndex);

				if (iter == ints) // we are at the end
				{
					memcpy(&node->nodes[index].next, &value, sizeof(tVal));
					node->nodes[index].valueWord = *iter;
					return;
				}

				// make a new node
				newNode = createNode(0);

				// stick the new (empty) node in the space we just made
				node->nodes[index] = {*iter, newNode};

				// set the lastnode to current node
				lastNode = node;
				lastIndex = index;

				// set the current node to the new node
				node = newNode;
			}
		}
	};

	// get - get a key/value in the hash
	//
	// return is true/false for found
	// value is returned as a reference.
	//
	// super useful in an if statement
	//
	//  if (hash.get( somekey, somevalu ))
	//  {
	//     //do something with some val. 
	//  };
	//
	//  save a check then a second lookup to get
	//
	bool get(tKey key, tVal& value)
	{
		/*
		tBranch8* ints = (uint8_t*)&key;
		tBranch8* iter = ints + sizeof(tKey);

		bl_array_s *node = root, *lastNode = node, *newNode;
		int index, lastIndex;
		*/
		ints = (uint8_t*)&key;
		iter = ints + sizeof(tKey);
		node = root;
		lastNode = node;

		while (--iter)
		{

			if ((index = getIndex(node, *iter)) >= 0)
			{
				if (iter == ints) // we are at the end
				{
					memcpy(&value, &node->nodes[index].next, sizeof(tVal));
					return true;
				}
				node = (bl_array_s*)node->nodes[index].next;
			}
			else
			{
				return false;
			}
		}
	};

	// exists - is key in hash 
	//
	bool exists(tKey key)
	{
		tBranch8* words = (uint8_t*)&key;//over.set(&key, iter);
		tBranch8* iter = words + sizeof(tKey);

		bl_array_s *node = root, *lastNode = node, *newNode;
		int index, lastIndex;

		while (--iter)
		{			

			if ((index = getIndex(node, *iter)) >= 0)
			{
				if (iter == words) // we are at the end
					return true;

				node = (bl_array_s*)node->nodes[index].next;
			}
			else
			{
				return false;
			}
		}
	};

private:

	// this is a fairly common binary search. Google will find you serveral
	// that look similar
	int64_t getIndex(bl_array_s* node, uint32_t valWord)
	{
		// int64_t vs int32_t resulted in a 50% performance increase.
		int32_t first = 0, last = node->used - 1, mid;
		auto iterate = false;
	
		if (node->used == 0) // empty list so return -1 (meaning index 0)
			return -1;

		if (node->nodes[0].valueWord == valWord) // if instant match, return index 0
			return 0;

		if (node->nodes[0].valueWord > valWord) // check for head insert return -1 (0)
			return -1;

		// check to see if valword is a list append.
		// this increases the speed of mostly sequential keys immesely
		if (node->nodes[last].valueWord < valWord) // no point in looking the valWord is after the last item
			return -(last + 2);

		// the list is full, so, everything is 1:1 and valWord is the index
		if (node->used == 256)
			return valWord;

		// on a short list scanning sequentially is more efficient
		// because the data is processors cache lines. 
		// iterating the first dozen or is most efficient 
		// and is quicker than list sub-division on my i7 type processor.
		// Some of the newer server processors might benifit from a 
		// higher setting.
		//
		//  bl_element_s = 10 bytes
		//  cache line   = 64 bytes. 
		//  6 elements per cache line.
		//  
		//  testing showed a positive gain for on my processor
		//  at two cache lines worth of elements.

		first = 0;
		last = node->used - 1;
		iterate = false;


		if (node->used <= LINEARSCAN)
		{
			// if this is a short list, iterate
			iterate = true;
		}
		else
		{
			// A proportional first split!
			//
			// Let's assume the key has good distribution.
			// If so then the values should distributed across the
			// array be proportionally, lets set mid point to
			// it's estimated location in the distribution of the list
			//
			mid = last >> 1;
			//mid = (((double)valWord / 256.0) * (double)(last + 1));

			while (first <= last)
			{
				// if the list gets short, lets stop dividing
				// and iterate! I know, it sounds horrible,
				// but it is actually "much" faster.
				if (last - first <= LINEARSCAN)
				{
					iterate = true;
					break;
				}

				if (valWord > node->nodes[mid].valueWord)
					first = mid + 1; // search bottom of list
				else if (valWord < node->nodes[mid].valueWord)
					last = mid - 1; // search top of list
				else
					return mid; // found

				mid = (first + last) >> 1; // usually written like first + ((last - first) / 2)			
			}
		}

		if (iterate)
		{
			for (; first <= last; ++first)
			{
				// >= will pass either condition (the conditions of the inner and outer if),
				// however on test that fails often is faster than two tests that fail often 
				// thus reducing to a nested if on the match saved 15% on read time.
				if (node->nodes[first].valueWord >= valWord)
				{
					if (node->nodes[first].valueWord == valWord)
						return first;
					return -(first + 1);
				}
			}

			return -(last + 2);
		}
		else
		{
			return -(first + 1); // java sdk returns - number to show insertion point. To convert back to positive insertion -(first) - 1;
		}
	};

	bl_array_s* makeGap(bl_array_s* node, int32_t index, bl_array_s* parent, int32_t parentIndex)
	{
		int32_t length = 1 << static_cast<int>(node->pageBits);

		// this node is full, so we will make a new one, and copy 
		if (node->used == length)
		{
			bl_array_s* newNode = createNode(node->pageBits + 1);

			// copy first half to new node
			memcpy(newNode->nodes, node->nodes, sizeof(bl_element_s) * index);
			// copy second half to new node (1 element over for insertion)
			if (index < node->used)
				memcpy(&newNode->nodes[index + 1], &node->nodes[index], sizeof(bl_element_s) * (node->used - index));

			// copy elements used + 1 for the space we just made
			newNode->used = node->used + 1;

			// smart delete
			mem.freePtr(node->pageBits, node);
			//delete [](uint8_t*)node;

			//if (node != root)
			parent->nodes[parentIndex].next = newNode; // point the parent at this new node
			//else
				//root = newNode;

			// return the new node
			return newNode;
		}

		// memmove will copy overlapped. Just move point after index down
		if (index < node->used)
			memmove(node->nodes + index + 1, node->nodes + index, sizeof(bl_element_s) * (node->used - index));

		++node->used;

		return node;
	}

	inline bl_array_s* createNode(int pageBits) // length in bits
	{
		auto length = 1 << pageBits;
		bl_array_s* node = reinterpret_cast<bl_array_s*>(mem.newPtr(pageBits, (length * sizeof(bl_element_s)) + sizeofArrayHeader));
		node->pageBits = pageBits;
		node->used = 0;
		return node;
	}

	/** Proxy for [] operator, on assignment the left hand side does
	 *  not have a position to reference. The Proxy class provides 
	 *  code to manage assignment and create the value.
	 */
	class Proxy
	{
	public:
		BinListDict8* dict;
		tKey key;
		tVal val;
		bool found;

		Proxy(BinListDict8* dict, tKey key) :
			dict(dict),
			key(key)
		{
			found = dict->get(key, val);
		}

		~Proxy()
		{}

		void operator=(tVal rhs)
		{
			found = true;
			val = rhs;
			dict->set(key, val);
		}

		operator int() const
		{
			if (!found)
				throw "poo";
			return val;
		}
	};

	public:

	Proxy operator[](tKey key)
	{
		return Proxy(this, key);
	}

#pragma pack(push,1)

	union Overlay
	{
		tKey original;
		uint8_t bytes[sizeof(tKey)];
	};

	struct Item
	{
		bl_array_s* branch;
		int offset;
		Item():
			branch(nullptr),
			offset(0)
		{}
	};

	struct Cursor
	{
		Item stack[sizeof(tKey)];
		int depth;
		tVal value;
		Item row;
		Item lastRow;
		Overlay key;
		bool valid;
		bl_array_s* signedRoot;
		Cursor(): 
			depth(0), 
			valid(true),
			signedRoot(nullptr)
		{
			signedRoot = new bl_array_s;
		}
		Cursor(Cursor& source)
		{
			memcpy(this->stack, source.stack, sizeof(Item) * sizeof(tKey));
			this->depth = source.depth;
			this->value = source.value;
			this->row = source.row;
			this->lastRow = source.lastRow;
			this->key = source.key;
			this->valid = source.valid;
			this->signedRoot = source.signedRoot;
			source.signedRoot = nullptr;
		}
		~Cursor()
		{
			if (signedRoot)
				delete signedRoot;
		}
	};
#pragma pack(pop)


	Cursor IterateStart()
	{
		Cursor cursor;
		cursor.depth = 0;

		Item item;
		Item lastItem;

		
		// copy the root node
		memcpy(
			cursor.signedRoot, 
			root, 
			sizeofArrayHeader +
			(sizeof(bl_element_s) * root->used));

		/**
		 * If the type is a signed type we can return
		 * an ordered list by sorting the root node by the 
		 * unsigned int8. The first bit always contains sign.
		 */
		if (isSigned)
		{
			sort(
				cursor.signedRoot->nodes,
				cursor.signedRoot->nodes + cursor.signedRoot->used,
				[](const bl_element_s &a, const bl_element_s &b) -> bool
				{
					return (int8_t)a.valueWord < (int8_t)b.valueWord;
				}	
			);
		}		
		item.branch = cursor.signedRoot;
		
		//item.branch = root;
		item.offset = 0;

		cursor.stack[cursor.depth] = item;
		++cursor.depth;

		lastItem = item;

		while (cursor.depth < sizeof(tKey))
		{
			// empty branch
			if (lastItem.branch && !lastItem.branch->used)
			{
				cursor.valid = false;
				return cursor;
			}

			item.branch = (bl_array_s*)item.branch->nodes[0].next;
			item.offset = 0;

			cursor.stack[cursor.depth] = item;
			++cursor.depth;

			// TODO - move down? cast last node (pointer type) to value type
			cursor.value = (tVal)item.branch->nodes[item.offset].next;
			lastItem = item;
		}

		auto count = 0;

		for (int i = sizeof(tKey) - 1; i >=0; --i)
		{
			cursor.key.bytes[count] =
				cursor.stack[i].branch->nodes[cursor.stack[i].offset].valueWord;
			++count;
		}

		--cursor.stack[cursor.depth - 1].offset;

		return cursor;
	}

	bool iterate(Cursor& cursor)
	{
		if (cursor.depth <= 0)
			return false;

		cursor.value = 0;

		Item* stackPtr = cursor.stack + cursor.depth - 1;

		++stackPtr->offset;

		cursor.row.offset = stackPtr->offset;
		cursor.row.branch = stackPtr->branch;

		cursor.key.bytes[(sizeof(tKey) - (cursor.depth))] = 
			stackPtr->branch->nodes[stackPtr->offset].valueWord;

		while (cursor.row.offset >= cursor.row.branch->used)
		{
			--cursor.depth;

			// out of tree, done, no more data
			if (cursor.depth <= 0)
			{
				cursor.depth = 0;
				cursor.valid = false;
				return false;
			}

			stackPtr = cursor.stack + cursor.depth - 1;

			++stackPtr->offset;

			cursor.row.offset = stackPtr->offset;
			cursor.row.branch = stackPtr->branch;

			cursor.key.bytes[sizeof(tKey) - (cursor.depth)] = 
				stackPtr->branch->nodes[stackPtr->offset].valueWord;
		}

		//Cursor->LastRow = Cursor->Row;
		cursor.lastRow.offset = cursor.row.offset;
		cursor.lastRow.branch = cursor.row.branch;

		while (cursor.depth < sizeof(tKey))
		{
			if (!cursor.lastRow.branch->used)
			{
				cursor.depth = 0;
				return false;
			}

			cursor.row.branch = 
				(bl_array_s*)cursor.lastRow.branch->nodes[cursor.lastRow.offset].next;
			cursor.row.offset = 0;

			cursor.stack[cursor.depth].offset = cursor.row.offset;
			cursor.stack[cursor.depth].branch = cursor.row.branch;
			++cursor.depth;

			cursor.key.bytes[sizeof(tKey) - (cursor.depth)] = 
				cursor.stack[cursor.depth - 1].branch->nodes[cursor.stack[cursor.depth - 1].offset].valueWord;

			//Cursor->LastRow = Cursor->Row;
			cursor.lastRow.offset = cursor.row.offset;
			cursor.lastRow.branch = cursor.row.branch;
		}

		// cast last node (pointer type) to value type
		cursor.value = 
			(tVal)cursor.row.branch->nodes[cursor.row.offset].next;

		return true;
	};



/*
	class iterator
	{
	private:
		BinListDict8* dict;
	public:
		typedef iterator self_type;
		typedef tVal value_type;
		typedef tVal& reference;
		typedef tVal* pointer;
		typedef std::forward_iterator_tag iterator_category;
		typedef int difference_type;

		iterator(BinListDict8* dict) :
			dict(dict)
			{};


		self_type operator++() { self_type i = *this; ptr_++; return i; }
		self_type operator++(int junk) { ptr_++; return *this; }
		reference operator*() { return *ptr_; }
		pointer operator->() { return ptr_; }
		bool operator==(const self_type& rhs) { return ptr_ == rhs.ptr_; }
		bool operator!=(const self_type& rhs) { return ptr_ != rhs.ptr_; }
	};
*/
};
