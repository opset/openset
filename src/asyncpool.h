#pragma once

#include "common.h"
#include "asyncloop.h"
#include "threads/locks.h"
#include <vector>
#include <mutex>
#include <atomic>
#include <functional>
#include <condition_variable>
#include "internodemapping.h"

namespace openset
{

    namespace async
    {
        class AsyncPool;
    }

    namespace globals
    {
        extern async::AsyncPool* async;
    }

    namespace async
    {
        class OpenLoop;

        const int32_t PARTITION_WORKERS = 256; // max number of workers - max cores + hyperthreads

        class AsyncPool
        {
        public:

            // we store data about a shard here
            struct partitionInfo_s
            {
                AsyncPool* asyncPool;
                AsyncLoop* ooLoop; // open-ended-AsyncLoop
                int instance;
                int worker;
                atomic<int32_t> realtimeCells;

                explicit partitionInfo_s(AsyncPool* asyncPool, const int instance, const int worker) :
                    asyncPool(asyncPool),
                    ooLoop(nullptr),
                    instance(instance),
                    worker(worker),
                    realtimeCells(0)
                {}

                ~partitionInfo_s()
                {
                    if (ooLoop)
                        delete ooLoop;
                }

                void init()
                {
                    ooLoop = new AsyncLoop(asyncPool, instance, worker);
                }

                bool isInitialized() const
                {
                    return (ooLoop) ? true : false;
                }
            };

            struct workerInfo_s
            {
                std::mutex lock;
                atomic_bool triggered {false};
                std::condition_variable conditional;
                vector<partitionInfo_s*> jobs;
                atomic<int> queued;
            };


            CriticalSection poolLock;

            int32_t partitionMax{ 0 };
            int32_t workerMax{ 0 };

            CriticalSection globalAsyncLock;
            atomic<bool> globalAsyncInitSuspend{ false }; // we want it to suspend
            atomic<int32_t> globalAsyncLockDepth{ 0 }; // suspend depth
            atomic<int32_t> globalAsyncSuspendedWorkerCount{ 0 };

            bool running;

            //OpenSet::mapping::PartitionMap partitionMap;

            workerInfo_s workerInfo[PARTITION_WORKERS];
            partitionInfo_s* partitions[PARTITION_MAX];

            atomic<int64_t> lastZombieStamp{0};
            std::vector<partitionInfo_s*> zombiePartitions;

            AsyncPool(int32_t ShardMax, int32_t WorkerMax) :
                partitionMax(ShardMax),
                workerMax(WorkerMax),
                running(false)
            {
                openset::globals::async = this;

                // all nulls
                memset(partitions, 0, sizeof(partitions));

                for (auto &wInfo : workerInfo)
                    wInfo.queued = 0;
            }

            ~AsyncPool() = default;

            int getLeastBusy() const;

            void mapPartitionsToAsyncWorkers();

            void suspendAsync();
            void resumeAsync();
            void waitForResume();
            void assertAsyncLock() const;

            AsyncLoop* initPartition(int32_t partition);

            void balancePartitions();

            void freePartition(int32_t partition);

            /* Add a cell to every the loop object in every partition
             * calls back to a factory function that builds the cell
             */
            void cellFactory(std::vector<int> partitionList, const function<OpenLoop*(AsyncLoop*)>& factory);
            void cellFactory(const function<OpenLoop*(AsyncLoop*)>& factory);

            void purgeByTable(const std::string& tableName);

            int32_t count();

            AsyncLoop* isPartition(int32_t shardNumber);
            AsyncLoop* getPartition(int32_t shardNumber);

            void realtimeInc(int32_t shardNumber);
            void realtimeDec(int32_t shardNumber);
            int32_t getRealtimeRunning(int32_t shardNumber) const;

            bool isRunning() const
            {
                return running;
            }

            int getPartitionMax() const
            {
                return partitionMax;
            }

            int getWorkerCount() const
            {
                return workerMax;
            }

            void setPartitionMax(int maxPartitions)
            {
                partitionMax = maxPartitions;
            }

            void runner(int32_t workerId) noexcept;

            void maint() noexcept;

            void startAsync();
        };
    };

};

