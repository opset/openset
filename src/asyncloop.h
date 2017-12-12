#pragma once

#include "common.h"
#include <vector>
#include <mutex>
#include <atomic>

#include "threads/locks.h"
#include "oloop.h"

namespace openset
{
	namespace async
	{
		class OpenLoop;
		class AsyncPool;

		class AsyncLoop
		{
		private:

			CriticalSection pendLock;
			// where new worker cells get added
			vector<OpenLoop*> queued;
			atomic<int32_t> queueSize;
			// the active worker live
			vector<OpenLoop*> active;
			

			int64_t loopCount;

		public:

			AsyncPool* asyncPool;
			int64_t runTime;
			int partition;
			int worker;

			AsyncLoop(AsyncPool* asyncPool, int paritionId, int workerId);

			~AsyncLoop();

			// releases all cells owned by this loop
			void release();

			// uses locks - may be called from other threads
			void queueCell(OpenLoop* work);

			// this will add any queued jobs to the
			// active Loop. This is particularly useful 
			// because a job Cell can spawn more job cells
			// and they will be ready queued on the next cycle
			void scheduleQueued();

			int64_t getWorkerId() const
			{
				return worker;
			}

			int getPartitionId() const
			{
				return partition;
			}

			// this runs one iteration of the main Loop
			// short, sweet and called frequently
			bool Run(int64_t &nextRun);
		};
	};
};
