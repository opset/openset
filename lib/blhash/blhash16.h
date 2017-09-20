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
#include "../heapstack/heapstack.h"

typedef uint16_t tBranch;

// pool of sub-blocks
//
// BinListDict16 uses 17 different allocation sizes.
// as pages in the tree grow they need to be resized.
// the old page is returned here so it can be resused.
// in practice this is very efficient for both total
// memory size, as well as reducing fragmentation.
//

class ShortPtrPool16
{
public:

#pragma pack(push,1)

	struct memblock_s
	{
		uint8_t data[1];
	};

#pragma pack(pop)

	HeapStack mem;
	std::vector<std::stack<uint8_t*>> freePool;

	ShortPtrPool16() :
		mem(),
		freePool(17, std::stack<uint8_t*>()) // allocate a block stack using defaults
	{ };

	~ShortPtrPool16()
	{ };

	void debug()
	{
		printf("Free Pool\r\n");
		for (auto i = 0; i < 17; i++)
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
class BinListDict16
{
private:
#pragma pack(push,1)

	struct bl_element_s
	{
		tBranch valueWord;
		void* next;
	};

	struct bl_array_s
	{
		int16_t pageBits;
		int32_t used;
		bl_element_s nodes[65536]; // all properties must be above this array
	};

	int sizeofArrayHeader = 6; // pageBits + used in 1 byte aligned mem
	/*
		overlay turns a key of size<tKey> into an array of ints (16 bit unsigned numbers)
		also... it will pad and fill the last word if the tKey is an odd length
	*/
	struct overlay
	{
		//uint16_t ints[(sizeof(tKey) / 2) + 1]; // array of ints same size as tKey + overflow word
		tBranch words[sizeof(tKey)];
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
			if (sizeof(tKey) % 2 == 0) // even ints, so tKey fits completely
			{
				elements = sizeof(tKey) / 2;
			}
			else // odd, dangling byte, so we must fill last element (overflow) with 0
			{
				elements = (sizeof(tKey) / 2) + 1;
				words[elements - 1] = 0;
			}

			memcpy(words, value, sizeof(tKey));
		}

		tBranch* set(tKey* value, tBranch* & iter)
		{
			set(value);
			iter = words + elements;
			return reinterpret_cast<tBranch*>(&words);
		}

		// returns end pointer to ints, returns the start iterator (+1 word, so decrement first) for loop as reference
		// keys are reference in reverse
		tBranch* getListPtr(tBranch* & iter)
		{
			iter = words + elements;
			return static_cast<tBranch*>(&words);
		}
	};

#pragma pack(pop)

	bl_array_s* root; // root node for hash tree

	ShortPtrPool16 mem; // the list pool

	// naughty class-globals, these should be locals to the get and set functions,
	// however having them global is worth 2% as we avoid pushing the stack as deep on each 
	// call into get or set
	overlay over;
	tBranch *iter, *words;
	bl_array_s *node = root, *lastNode = node, *newNode;
	int64_t index, lastIndex;

public:

	BinListDict16() :
		root(nullptr), mem(), over()
	{
		root = createNode(9); // make a full sized root node
	}

	~BinListDict16()
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
		words = over.set(&key, iter);

		node = root;
		lastNode = node;

		//if (node->nodes[0].valueWord == 44758)

		while (1)
		{
			iter--;

//			if (*iter == 39230)
	//			cout << "here" << endl;


			if ((index = getIndex(node, *iter)) >= 0)
			{
				lastNode = node;
				lastIndex = index;

				if (iter == words) // we are at the end
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

				if (iter == words) // we are at the end
				{
					memcpy(static_cast<void*>(&node->nodes[index].next), &value, sizeof(tVal));
					node->nodes[index].valueWord = *iter;
					return;
				}

				// make a new node
				newNode = createNode(0);

				// stick the new (empty) node in the space we just made
				node->nodes[index].next = newNode;
				node->nodes[index].valueWord = *iter;

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
		words = over.set(&key, iter);
		node = root;

		while (1)
		{
			iter--;

			if ((index = getIndex(node, *iter)) >= 0)
			{
				if (iter == words) // we are at the end
				{
					memcpy(&value, &node->nodes[index].next, sizeof(tVal));
					return true;
				}

				node = static_cast<bl_array_s*>(node->nodes[index].next);
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
		words = over.set(&key, iter);
		node = root;

		int index;

		while (true)
		{
			iter--;

			if ((index = getIndex(node, *iter)) >= 0)
			{
				if (iter == words) // we are at the end
					return true;

				node = static_cast<bl_array_s*>(node->nodes[index].next);
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
	static int64_t getIndex(bl_array_s* node, uint16_t valWord)
	{
		// int64_t vs int32_t resulted in a 50% performance increase.
		int64_t first = 0, last = node->used - 1, mid;
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
		if (node->used == 65536)
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

		if (node->used <= 10)
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
			mid = (int64_t)(((double)valWord / 65536.0) * (double)(last + 1));

			while (first <= last)
			{
				// if the list gets short, lets stop dividing
				// and iterate! I know, it sounds horrible,
				// but it is actually "much" faster.
				if (last - first <= 10)
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

	bl_array_s* makeGap(bl_array_s* node, int64_t index, bl_array_s* parent, int64_t parentIndex)
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

			if (node != root)
				parent->nodes[parentIndex].next = newNode; // point the parent at this new node
			else
				root = newNode;

			// return the new node
			return newNode;
		}

		// memmove will copy overlapped. Just move point after index down
		if (index < node->used)
			memmove(&node->nodes[index + 1], &node->nodes[index], sizeof(bl_element_s) * (node->used - index));

		++node->used;

		return node;
	}

	bl_array_s* createNode(int pageBits) // length in bits
	{
		auto length = 1 << pageBits;
		bl_array_s* node = reinterpret_cast<bl_array_s*>(mem.newPtr(pageBits, (length * sizeof(bl_element_s)) + sizeofArrayHeader));
		node->pageBits = pageBits;
		node->used = 0;
		return node;
	}
};
