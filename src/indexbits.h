#pragma once

#include <list>

#include "common.h"
#include "sba/sba.h"
#include <cassert>

namespace openset
{
    namespace db
    {
        const int64_t BitArraySize = 510;

        struct IndexPageMemory_s
        {
            bool dirty { false };
            bool empty { true };
            // 4096 bytes
            int64_t bitArray[BitArraySize];
        };

        const int64_t IndexPageRecordSize = sizeof(IndexPageMemory_s);
        const int64_t IndexPageDataSize = sizeof(uint64_t) * BitArraySize;
        const int64_t IndexBitsPerPage = BitArraySize * 64;
        const int64_t Overflow = 64;

#pragma pack(push,1)
        struct CompPageMemory_s
        {
            int index { 0 };
            CompPageMemory_s* next { nullptr };
            char compressedData[IndexPageDataSize];
        };
#pragma pack(pop)

        const int64_t CompPageHeaderSize = sizeof(int) + sizeof(CompPageMemory_s*);

        class IndexMemory
        {
            using IndexPageList = std::vector<IndexPageMemory_s*>;
            using RawPageList = std::vector<CompPageMemory_s*>;

            IndexPageList indexPages;
            RawPageList rawPages;

            IndexPageMemory_s* lastIndex { nullptr };

        public:

            IndexMemory() = default;

            IndexMemory(IndexMemory&& source) noexcept
            {
                lastIndex = source.lastIndex;
                indexPages = std::move(source.indexPages);
                rawPages = std::move(source.rawPages);

                source.indexPages.clear();
                source.rawPages.clear();
            }

            IndexMemory(const IndexMemory& source)
            {
                // raw pages are not copied
                lastIndex = source.lastIndex;

                for (auto sourcePage : source.indexPages)
                {
                    const auto page = reinterpret_cast<IndexPageMemory_s*>(PoolMem::getPool().getPtr(IndexPageRecordSize));
                    memcpy(page, sourcePage, IndexPageRecordSize);
                    indexPages.push_back(page);
                }
            }

            IndexMemory(IndexMemory* source)
            {
                // raw pages are not copied
                lastIndex = source->lastIndex;

                for (auto sourcePage : source->indexPages)
                {
                    const auto page = reinterpret_cast<IndexPageMemory_s*>(PoolMem::getPool().getPtr(IndexPageRecordSize));
                    memcpy(page, sourcePage, IndexPageRecordSize);
                    indexPages.push_back(page);
                }
            }

            IndexMemory& operator=(IndexMemory&& source) noexcept
            {
                lastIndex = source.lastIndex;
                indexPages = std::move(source.indexPages);
                rawPages = std::move(source.rawPages);

                return *this;
            }

            IndexMemory& operator=(const IndexMemory& source)
            {
                // raw pages are not copied
                reset();
                lastIndex = source.lastIndex;

                for (auto sourcePage : source.indexPages)
                {
                    const auto page = reinterpret_cast<IndexPageMemory_s*>(PoolMem::getPool().getPtr(IndexPageRecordSize));
                    memcpy(page, sourcePage, IndexPageRecordSize);
                    indexPages.push_back(page);
                }

                return *this;
            }

            ~IndexMemory()
            {
                reset();
            }

            void reset()
            {
                for (auto page : indexPages)
                    PoolMem::getPool().freePtr(page);
                indexPages.clear();
                rawPages.clear();
                lastIndex = nullptr;
            }

            int64_t intCount() const
            {
                return BitArraySize * static_cast<int64_t>(indexPages.size());
            }

            int64_t* getBitInt(const int64_t bitIndex)
            {
                const auto page = getPage(bitIndex);
                lastIndex = page;
                const auto intIndex = (bitIndex / 64LL) % BitArraySize; // convert bit index into int64 index

                return page->bitArray + intIndex;
            }

            int64_t* getInt(const int64_t intIndex)
            {
                const auto page = getPage(intIndex * 64LL);
                lastIndex = page;
                const auto indexInPage = intIndex % BitArraySize;

                return page->bitArray + indexInPage;
            }

            void setDirty() const
            {
                if (lastIndex)
                    lastIndex->dirty = true;
            }

            void setDirtyAllPages()
            {
                for (const auto page : indexPages)
                    page->dirty = true;
            }

            IndexPageMemory_s* getPage(const int64_t bitIndex)
            {
                const auto pageIndex = bitIndex / IndexBitsPerPage; // convert bit index into page in dex

                while (pageIndex >= static_cast<int64_t>(indexPages.size()))
                {
                    const auto page = reinterpret_cast<IndexPageMemory_s*>(PoolMem::getPool().getPtr(IndexPageRecordSize));
                    memset(page->bitArray, 0, IndexPageDataSize);
                    indexPages.push_back(page);
                }

                return indexPages.at(pageIndex);
            }

            IndexPageMemory_s* getPageByPageIndex(const int64_t pageIndex, const bool clean = true)
            {
                while (pageIndex >= static_cast<int64_t>(indexPages.size()))
                {
                    const auto page = reinterpret_cast<IndexPageMemory_s*>(PoolMem::getPool().getPtr(IndexPageRecordSize));
                    if (clean)
                        memset(page->bitArray, 0, IndexPageDataSize);
                    indexPages.push_back(page);
                }

                return indexPages.at(pageIndex);
            }

            CompPageMemory_s* getRawPage(const int pageIndex)
            {
                for (auto page : rawPages)
                {
                    if (page->index > pageIndex)
                        break;
                    if (page->index == pageIndex)
                        return page;
                }

                return nullptr;
            }

            static int pagePopulation(IndexPageMemory_s* page)
            {
                auto source = static_cast<int64_t*>(page->bitArray);
                const auto end = source + BitArraySize;

                int64_t pop = 0;

                while (source < end)
                {
            #ifdef _MSC_VER
                    pop += __popcnt64(*source);
            #else
                    pop += __builtin_popcountll(*source);
            #endif
                    ++source;
                }

                return static_cast<int>(pop);
            }

            void decompress(char* compressedData);
            char* compress();
        };


        class IndexBits
        {
        public:
            IndexMemory data;
            bool placeHolder;

            IndexBits();
            IndexBits(IndexBits&& source) noexcept;
            IndexBits(const IndexBits& source);
            explicit IndexBits(IndexBits* source);
            ~IndexBits();

            IndexBits& operator=(IndexBits&& other) noexcept;
            IndexBits& operator=(const IndexBits& other);

            void reset();

            // make bits that are on or off
            // index is number of bits, state is 1 or 0
            void makeBits(int64_t index, int state);

            // takes buffer to compressed data and actual size as parameters
            // note: actual size is number of long longs (in64_t)
            void mount(char* compressedData);

            // returns a POOL buffer ptr, and the number of bytes
            char* store();

            void setSizeByBit(int64_t index);
            void bitSet(int64_t index);
            void bitClear(int64_t index);
            bool bitState(int64_t index);

            int64_t population(const int64_t stopBit);

            void opCopy(const IndexBits& source);
            void opCopyNot(IndexBits& source);
            void opAnd(IndexBits& source);
            void opOr(IndexBits& source);
            void opAndNot(IndexBits& source);
            void opNot();

            bool linearIter(int64_t& linId, int64_t stopBit);
        };

        class IndexLRU
        {
            using Key = std::pair<int, int64_t>;
            using Value = std::pair<IndexBits*, std::list<Key>::iterator>;

            std::list<Key> items;
            unordered_map <Key, Value> keyValuesMap;
            int cacheSize;

        public:
            IndexLRU(int cacheSize) :
                cacheSize(cacheSize)
            {}

            std::tuple<int, int64_t, IndexBits*> set(int propIndex, int64_t value, IndexBits* bits)
            {
                const Key key(propIndex, value);

                items.push_front(key);

                const Value listMap(bits, items.begin());
                keyValuesMap[key] = listMap;

                if (keyValuesMap.size() > cacheSize) {
                    const auto evictedKey = items.back();
                    const auto evicted = keyValuesMap[items.back()].first;
                    keyValuesMap.erase(items.back());
                    items.pop_back();
                    return {evictedKey.first, evictedKey.second, evicted};
                }

                return {0,0,0};
            }

            IndexBits* get(int propIndex, int64_t value)
            {

                const Key key(propIndex, value);

                if (auto iter = keyValuesMap.find(key); iter != keyValuesMap.end())
                {
                    items.erase(iter->second.second);
                    items.push_front(key);
                    iter->second.second = items.begin();
                    return iter->second.first;
                }
                return nullptr;
            }
        };
    };
};
