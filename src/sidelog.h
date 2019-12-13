#pragma once

#include <unordered_map>
#include <limits>
#include <atomic>

#include "sba/sba.h"
#include "threads/locks.h"
#include "heapstack/heapstack.h"

#include "common.h"
#include "table.h"

namespace openset::db
{
    struct SideLogCursor_s
    {
        int64_t stamp{ Now() };
        int64_t tableHash{ 0 };
        int32_t partition{ -1 };
        char* jsonData { nullptr };
        SideLogCursor_s* next { nullptr };

        SideLogCursor_s() = default;

        SideLogCursor_s(const int64_t tableHash, const int32_t partition, char* data) :
            tableHash(tableHash),
            partition(partition),
            jsonData(data)
        { }

        ~SideLogCursor_s() = default;

        void serialize(HeapStack* mem) const
        {
            const auto serializedStamp = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
            *serializedStamp = stamp;

            const auto serializedTableHash = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
            *serializedTableHash = tableHash;

            const auto serializedPartition = recast<int32_t*>(mem->newPtr(sizeof(int32_t)));
            *serializedPartition = partition;

            const auto serializedJsonLength = recast<int32_t*>(mem->newPtr(sizeof(int32_t)));
            *serializedJsonLength = strlen(jsonData);

            const auto serializedJson = mem->newPtr(*serializedJsonLength);
            memcpy(serializedJson, jsonData, *serializedJsonLength);
        }

        void deserialize(char* &mem)
        {
            stamp = *recast<int64_t*>(mem);
            mem += sizeof(int64_t);

            tableHash = *recast<int64_t*>(mem);
            mem += sizeof(int64_t);

            partition = *recast<int32_t*>(mem);
            mem += sizeof(int32_t);

            const auto jsonLength = *recast<int32_t*>(mem);
            mem += sizeof(int32_t);

            // buffer for the json text and +1 for the null terminator
            jsonData = static_cast<char*>(PoolMem::getPool().getPtr(jsonLength + 1));
            memcpy(jsonData, mem, jsonLength);
            jsonData[jsonLength] = 0x00;
            mem += jsonLength;
        }
    };

    class SideLog
    {
        //const int64_t LOG_MAX_AGE = 1'000;
        const int64_t MIN_LOG_SIZE = 1'000;

        atomic<int64_t> logSize{ 0 };
        int64_t lastLogSize{ 0 };

        SideLogCursor_s* head { nullptr };
        SideLogCursor_s* tail { nullptr };

        CriticalSection cs;

        using JsonList = std::vector<char*>;

        // pair is <tableHash, parition>
        using ReadMap = std::unordered_map<std::pair<int64_t, int32_t>, SideLogCursor_s*>;

        //LastMap writeHeads;
        ReadMap readHeads;

        int64_t lastTrim{ Now() };

        SideLog() = default;

        SideLogCursor_s* getLastRead(const int64_t tableHash, const int32_t partition)
        {
            const auto key = std::make_pair( tableHash, partition );

            if (const auto readEntry = readHeads.find(key); readEntry != readHeads.end())
                return readEntry->second;

            return nullptr;
        }

        void setLastRead(const int64_t tableHash, const int32_t partition, SideLogCursor_s* link)
        {
            const auto key = std::make_pair( tableHash, partition );

            if (const auto readEntry = readHeads.find(key); readEntry != readHeads.end())
                readEntry->second = link;
            else
                readHeads.insert({key, link});
        }

        unordered_set<SideLogCursor_s*> getReferencedEntries()
        {
            unordered_set<SideLogCursor_s*> links;

            if (readHeads.empty())
                return links;

            for (const auto &head : readHeads)
                links.insert(head.second);

            return links;
        }

        void trimSideLog()
        {
            if (lastTrim + 60'000 < Now() && lastLogSize != logSize)
            {
                lastTrim = Now();
                Logger::get().debug("transaction log at " + to_string(logSize) + " transactions");
                lastLogSize = logSize;
            }

            //const auto keepStamp = Now() - LOG_MAX_AGE;
            const auto referencedEntries = getReferencedEntries();

            if (referencedEntries.count(nullptr))
                return;

            auto cursor = head;

            auto count = 0;

            while (cursor &&
                   logSize > MIN_LOG_SIZE &&
                   //cursor->stamp < keepStamp &&
                   referencedEntries.count(cursor) == 0)
            {
                const auto nextEntry = cursor->next;

                // free json data
                PoolMem::getPool().freePtr(cursor->jsonData);
                // free struct - was created with placement new, destructor need not be called
                PoolMem::getPool().freePtr(cursor);

                --logSize;
                ++count;

                cursor = nextEntry;
            }

            head = cursor;

            if (!head)
                tail = nullptr;

        }

        void resetReadHeads()
        {
            for (auto &head : readHeads)
                head.second = nullptr;
        }


    public:

        // singleton
        static SideLog& getSideLog()
        {
            static SideLog log;
            return log;
        }

        void lock()
        {
            cs.lock();
        }

        void unlock()
        {
            cs.unlock();
        }

        int64_t getLogSize() const
        {
            return logSize;
        }

        // lock/unlock from caller using lock() and unlock() to accelerate inserts
        int add(const Table* table, const int32_t partition, char* json)
        {
            const auto tableHash = table->getTableHash();

            // create with placement new
            const auto newEntry =
                new (PoolMem::getPool().getPtr(sizeof(SideLogCursor_s)))
                    SideLogCursor_s(tableHash, partition, json);

            ++logSize;

            // link it onto the end of the list
            if (!head)
                head = newEntry;

            if (tail)
                tail->next = newEntry;

            tail = newEntry;

            return logSize;
        }

        JsonList read(const Table* table, const int32_t partition, const int limit, int64_t& readPosition)
        {
            readPosition = 0;

            JsonList resultList;
            resultList.reserve(limit);

            const auto tableHash = table->getTableHash();

            csLock lock(cs);

            auto lastCursor = getLastRead(tableHash, partition);
            auto cursor = lastCursor;

            if (!cursor)
                cursor = head;
            else
                cursor = cursor->next;

            if (!cursor)
            {
                readPosition = reinterpret_cast<int64_t>(lastCursor);
                trimSideLog();
                return resultList;
            }

            while (cursor)
            {
                lastCursor = cursor;

                if (cursor->tableHash == tableHash && cursor->partition == partition)
                {
                    resultList.push_back(cursor->jsonData);

                    if (static_cast<int>(resultList.size()) == limit)
                        break;
                }

                cursor = cursor->next;
            }

            readPosition = reinterpret_cast<int64_t>(lastCursor);

            trimSideLog();

            return resultList;
        }

        void updateReadHead(const Table* table, const int32_t partition, const int64_t handle)
        {
            csLock lock(cs);
            setLastRead(table->getTableHash(), partition, reinterpret_cast<SideLogCursor_s*>(handle));
        }

        void resetReadHead(const Table* table, const int32_t partition)
        {
            csLock lock(cs);
            const auto tableHash = table->getTableHash();
            setLastRead(tableHash, partition, nullptr);
        }

        void removeReadHeadsByPartition(const int32_t partition)
        {
            csLock lock(cs);

            for (auto iter = readHeads.begin(); iter != readHeads.end();)
            {
                if (iter->first.second == partition)
                    iter = readHeads.erase(iter);
                else
                    ++iter;
            }
        }

        void serialize(HeapStack* mem)
        {
            csLock lock(cs);

            // grab 8 bytes, this will contain the number of entries in the Log
            const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
            *sectionLength = 0;

            auto count = 0;
            auto cursor = head;

            while (cursor)
            {
                ++count;
                cursor->serialize(mem);
                cursor = cursor->next;
            }

            // update the count
            *sectionLength = count;
        }

        void deserialize(char* mem)
        {
            csLock lock(cs);

            auto read = mem;

            const auto sectionLength = *recast<int64_t*>(read);
            read += sizeof(int64_t);

            // reset log size
            logSize = 0;

            // save the old Head so we can append the
            // old sidelog to the this new one
            const auto oldHead = head;
            head = nullptr;
            tail = nullptr;

            for (auto i = 0; i < sectionLength; ++i)
            {
                // create with placement new
                const auto newEntry =
                    new (PoolMem::getPool().getPtr(sizeof(SideLogCursor_s)))
                        SideLogCursor_s();

                newEntry->deserialize(read);

                // link it onto the end of the list
                if (!head)
                    head = newEntry;

                if (tail)
                    tail->next = newEntry;

                tail = newEntry;

                ++logSize;

            }

            // here we append (and update the sequence numbers)
            // on any transactions that were already in the sideLog.
            // This will give us replay of transactions from a long
            // running node, and any new transcations that have been
            // forwarded here.
            if (oldHead)
            {
                const auto newStamp = Now();

                auto cursor = oldHead;

                while (cursor)
                {
                    ++logSize;
                    const auto next = cursor->next;
                    cursor->stamp = newStamp;
                    cursor->next = nullptr;

                    if (tail)
                        tail->next = cursor;

                    tail = cursor;
                    cursor = next;
                }

            }

            // reset the read-head so this entire new transaction log
            // will get replayed through the insert mechanism
            resetReadHeads();
        }
    };
}


