#pragma once

#include "asyncloop.h"

namespace openset
{
	namespace async
	{
		class AsyncLoop;

		enum class oloopState_e
		{
			running,
			done,
			clear
		};

		enum class oloopPriority_e
		{
			background,
			realtime
		};

		class OpenLoop
		{
		public:
			oloopPriority_e priority;           
			oloopState_e state;
            std::string owningTable;
			int64_t runAt;
			int64_t runStart; // time or call to run
			bool prepared;
			AsyncLoop* loop;

			explicit OpenLoop(std::string owningTable, oloopPriority_e priority = oloopPriority_e::background);
			virtual ~OpenLoop();
			void assignLoop(AsyncLoop* loop);

			// if there are realtime priority cells in this
			// partition, bypass will be true
			bool inBypass() const;

			void scheduleFuture(uint64_t milliFromNow);
			void scheduleAt(uint64_t milliRunAt);

			void spawn(OpenLoop* newCell) const;
			void suicide();

			bool sliceComplete() const;
			virtual bool checkCondition();
			virtual bool checkTimer(const int64_t milliNow);

			// these must be overridden (preferrably final) in derived classes
			virtual void prepare() = 0;
			virtual void run() = 0;			
			virtual void partitionRemoved() = 0; // allow for error handling if a partition is removed
		};
	};
};
