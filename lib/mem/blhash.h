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

#pragma once

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stack>
#include <functional>
#include "../heapstack/heapstack.h"

typedef uint16_t tBranch;

// pool of sub-blocks
//
// BinaryListHash uses 17 different allocation sizes.
// as pages in the tree grow they need to be resized.
// the old page is returned here so it can be resused.
// in practice this is very efficient for both total
// memory size, as well as reducing fragmentation.
//

class ShortPtrPool
{
public:

#pragma pack(push,1)

    struct MemBlock_s
    {
        uint8_t data[1];
    };

#pragma pack(pop)

    HeapStack mem;
    std::vector<std::vector<uint8_t*>> freePool;

    ShortPtrPool()
        : freePool(17, std::vector<uint8_t*>())
    { };

    ~ShortPtrPool() = default;

    void reset()
    {
        for (auto& item : freePool)
            item.clear();
        mem.reset();
    }

    void debug()
    {
        printf("Free Pool\r\n");
        for (int i = 0; i < 17; i++)
        {
            printf("%i = %zi\r\n", i, freePool[i].size());
        }

        std::cout << mem.getBytes() << "\r\n";
    }

    __forceinline uint8_t* newPtr(int bits, int size)
    {
        if (freePool[bits].empty())
            return reinterpret_cast<uint8_t*>(mem.newPtr(size));

        const auto tPtr = freePool[bits].back();
        freePool[bits].pop_back();

        return tPtr;
    }

    __forceinline void freePtr(int bits, void* block)
    {
        freePool[bits].push_back(static_cast<uint8_t*>(block));
    }
};

template <typename tKey, typename tVal>
class BinaryListHash
{
public:
    using FilterCB = std::function<bool(tKey*, tVal*)>;

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
        overlay turns a key of size<tKey> into an array of words (16 bit unsigned numbers)
        also... it will pad and fill the last word if the tKey is an odd length
    */
    struct overlay
    {
        tBranch words[sizeof(tKey)];
        int elements;

        overlay() = default;

        overlay(tKey* value)
        {
            set(value);
        }

        void set(tKey* value)
        {
            if (sizeof(tKey) % 2 == 0) // even words, so tKey fits completely
            {
                elements = sizeof(tKey) / 2;
            }
            else // odd, dangling byte, so we must fill last element (overflow) with 0
            {
                elements            = (sizeof(tKey) / 2) + 1;
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

        // returns end pointer to words, returns the start iterator (+1 word, so decrement first) for loop as reference
        // keys are reference in reverse
        tBranch* getListPtr(tBranch* & iter)
        {
            iter = words + elements;
            return static_cast<tBranch*>(&words);
        }

        tKey getKey()
        {
            tKey key;
            memcpy(&key, words, sizeof(tKey));
            return key;
        }

        tKey* getKeyPtr()
        {
            return reinterpret_cast<tKey*>(words);
        }
    };
#pragma pack(pop)

    bl_array_s* root; // root node for hash tree
    ShortPtrPool mem; // the list pool

    int32_t distinct {0};

    // serialize variables (passing them as params is just really slow)
    overlay serializeOver;
    int serializeLimit;
    FilterCB serializeCB;

public:

    using ResultItem = std::pair<tKey, tVal>;
    using HashVector = std::vector<ResultItem>;
    HashVector serializeList;

    BinaryListHash()
        : root(nullptr)
    {
        root = createNode(9); // make a full sized root node
    };

    ~BinaryListHash() = default;

    void clear()
    {
        mem.reset();
        root = createNode(9);
    }

    // debug - dumps usage data for the memory manager
    //
    // shows how many cached/recycled lists are available
    //
    void debug()
    {
        mem.debug();
    };

    // set - set a key/value in the hash
    //
    // note: adding an existing key will overwrite with a
    //       provided value.
    //
    void set(tKey key, tVal value)
    {
        tBranch *iter;
        int64_t index, lastIndex = 0;

        overlay over;
        tBranch* words = over.set(&key, iter);

        bl_array_s* node = root;
        bl_array_s* lastNode = node;

        while (true)
        {
            iter--;

            if ((index = getIndex(node, *iter)) >= 0)
            {
                lastNode  = node;
                lastIndex = index;

                if (iter == words) // we are at the end
                {
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
                    ++distinct;
                    return;
                }

                // make a new node
                bl_array_s* newNode = createNode(0);

                // stick the new (empty) node in the space we just made
                node->nodes[index].next      = newNode;
                node->nodes[index].valueWord = *iter;

                // set the last node to current node
                lastNode  = node;
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
    //  if (hash.get( someKey, someValue ))
    //  {
    //     //do something with some val.
    //  };
    //
    //  save a check then a second lookup to get
    //
    bool get(tKey key, tVal& value)
    {
        tBranch *iter;
        int64_t index;

        overlay over;
        tBranch* words = over.set(&key, iter);
        bl_array_s* node = root;

        while (true)
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

        tBranch *iter;
        overlay over;
        tBranch* words = over.set(&key, iter);
        bl_array_s* node  = root;

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


    HashVector& serialize(int limit, FilterCB filterCallBack)
    {
        tKey key;
        serializeOver.set(&key);

        serializeList.clear();
        serializeList.reserve(distinct);

        serializeLimit = limit;
        serializeCB = filterCallBack;

        serializeRecurse(root, 0);

        return serializeList;
    }

private:

    void serializeRecurse(bl_array_s* node, int depth)
    {
        for (auto idx = 0; idx < node->used; ++idx)
        {
            if (serializeLimit == -1)
                return;

            serializeOver.words[serializeOver.elements - 1 - depth] = node->nodes[idx].valueWord;

            if (depth == serializeOver.elements - 1)
            {
                if (//serializeOver > serializeStart &&
                    serializeCB(serializeOver.getKeyPtr(), reinterpret_cast<tVal*>(&node->nodes[idx].next)))
                {
                    serializeList.emplace_back(*serializeOver.getKeyPtr(), *reinterpret_cast<tVal*>(&node->nodes[idx].next));
                    if (serializeList.size() == serializeLimit)
                    {
                        serializeLimit = -1;
                        return;
                    }
                }
            }
            else
            {
                serializeRecurse(reinterpret_cast<bl_array_s*>(node->nodes[idx].next), depth + 1);
            }
        }

    }

    // this is a fairly common binary search. Google will find you serveral
    // that look similar
    static int32_t getIndex(bl_array_s* node, uint16_t valWord)
    {
        // int64_t vs int32_t resulted in a 50% performance increase.
        int32_t first = 0;
        int32_t last  = node->used - 1;

        if (node->used == 0) // empty list so return -1 (meaning index 0)
            return -1;

        if (node->nodes[0].valueWord == valWord) // first one is 0, shortcut for 1 item lists
            return 0;

        // check to see if valueWord is a list append.
        // this increases the speed of mostly sequential keys immensely
        if (node->nodes[last].valueWord < valWord) // no point in looking the valWord is after the last item
            return -(last + 2);

        // the list is full, so, everything is 1:1 and valWord is the index
        if (node->used == 65536)
            return valWord;

        // on a short list scanning sequentially is more efficient
        // because the data is fits in a cache line.
        // iterating the first dozen or is most efficient
        // and is quicker than list sub-division on my i7 type processor.
        // Some of the newer server processors might benefit from a
        // higher setting.
        //
        //  bl_element_s = 10 bytes
        //  cache line   = 64 bytes.
        //  6 elements per cache line.
        //
        //  testing showed a positive gain for on my processor
        //  at two cache lines worth of elements.

        if (node->used <= 8)
        {
            ++first; // we just checked index 0 above, so skip it
            for (; first <= last; ++first)
            {
                // nesting these conditions netted 15% speed improvement
                if (node->nodes[first].valueWord >= valWord)
                {
                    if (node->nodes[first].valueWord == valWord)
                        return first;
                    return -(first + 1);
                }
            }

            return -(last + 2);
        }

        // Proportional first split
        //
        // Let's assume the key has good distribution.
        // If so then the values should distributed across the
        // array be proportionally, lets set mid point to
        // it's estimated location in the distribution of the list
        //
        auto mid = static_cast<int32_t>((static_cast<double>(valWord) / 65536.0) * static_cast<double>(last + 1));

        while (first <= last)
        {
            if (valWord > node->nodes[mid].valueWord)
                first = mid + 1; // search bottom of list
            else if (valWord < node->nodes[mid].valueWord)
                last = mid - 1; // search top of list
            else
                return mid; // found

            mid = (first + last) >> 1; // usually written like first + ((last - first) / 2)
        }

        return -(first + 1);
        // java sdk returns - number to show insertion point. To convert back to positive insertion -(first) - 1;
    };

    bl_array_s* makeGap(bl_array_s* node, int64_t index, bl_array_s* parent, int64_t parentIndex)
    {
        auto length = 1 << static_cast<int>(node->pageBits);

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

        // mem move will copy overlapped.
        if (index < node->used)
            memmove(&node->nodes[index + 1], &node->nodes[index], sizeof(bl_element_s) * (node->used - index));

        ++node->used;

        return node;
    }

    bl_array_s* createNode(int pageBits) // length in bits
    {
        const int32_t length = 1 << pageBits;

        auto* node = reinterpret_cast<bl_array_s*>(mem.newPtr(
            pageBits,
            (length * sizeof(bl_element_s)) + sizeofArrayHeader));
        //new uint8_t[(length * sizeof(bl_element_s)) + sizeofArrayHeader];
        node->pageBits = pageBits;
        node->used     = 0;
        return node;
    }
};
