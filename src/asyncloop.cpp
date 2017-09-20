#include "asyncloop.h"
#include "asyncpool.h"

using namespace openset::async;

AsyncLoop::AsyncLoop(AsyncPool* asyncPool, int partitionId, int workerId) :
	queueSize(0),
	loopCount(0),
	asyncPool(asyncPool),
	runTime(50),
	partition(partitionId),
	worker(workerId)
{}

AsyncLoop::~AsyncLoop()
{
	release();
}

void AsyncLoop::release()
{
	csLock lock(pendLock);

	while (queued.size())
	{
		queued.back()->partitionRemoved();
		delete queued.back();
		queued.pop_back();
	}

	while (active.size())
	{
		// we are force removing, this member can be over-ridden to 
		// allow for graceful error handling (i.e. incomplete shuttle calls)
		active.back()->partitionRemoved(); 
		delete active.back();
		active.pop_back();
	}
}

// uses locks - may be called from other threads
void AsyncLoop::queueCell(OpenLoop* work)
{
	{
		csLock lock(pendLock);
		// assign this loop to the cell
		work->assignLoop(this);
		queued.push_back(work);
		++queueSize;
	}

	// trigger and run immediately?
	asyncPool->workerInfo[worker].triggered = true;
	asyncPool->workerInfo[worker].conditional.notify_one();
}

// this will add any queued jobs to the
// active Loop. This is particularly useful 
// because a job Cell can spawn more job cells
// and they will be ready queued on the next cycle
//
// Note, this is also where prepare is called, at this
// point 'loop' as been assigned.
void AsyncLoop::scheduleQueued()
{
	// call prepare on the cell to set it's state
	// outside the constructor, this happens in the partition thread
	csLock lock(pendLock);

	queueSize -= queued.size();
	active.insert(active.end(), make_move_iterator(queued.begin()), make_move_iterator(queued.end()));
	queued.clear();	
}

// moves a Cell to the cleanup queue
void AsyncLoop::markForCleanup(OpenLoop* work)
{
	completed.push_back(work);
	work->state = oloopState_e::clear;
}

// this runs one iteration of the main Loop
bool AsyncLoop::Run(int64_t &nextRun)
{
	// actual number of worker cells that did anything
	auto runCount = 0;	

	// inject any queued work
	if (queueSize)
		scheduleQueued();

	// nothing to do
	if (!active.size())
		return false;

	vector<OpenLoop*> rerun;

	// this is the inside of our open ended Loop
	// it will call each job that is ready to run
	for (auto w : active)
	{
		auto now = Now();

		if (!w->prepared)
		{
			w->prepare();
			w->prepared = true;
		}

		if (w->checkCondition() &&
			w->checkTimer(now) &&
			w->state == oloopState_e::running) // check - some cells will complete in prepare
		{
			w->runStart = now;
			w->run();

			// look for next scheduled (future) run operation
			if (w->state == oloopState_e::running && 
				w->runAt > now && (nextRun == -1 || w->runAt < nextRun))
				nextRun = w->runAt;
			
			++runCount;
		}

		if (w->state == oloopState_e::done)
			markForCleanup(w);
		else
			rerun.push_back(w); // reschedule jobs that have more to do

		//this_thread::yield(); // cooperate
	}

	// swap rerun queue to active queue
	active = std::move(rerun);

	// cleanup objects every 10 runs - low tech garbage collection
	if (++loopCount % 10 == 0 && completed.size())
		cleanup();

	// nothing to do
	return (!runCount) ? false : true;
}

// clean up - run every 50 loops, but could be run 
// on a timer
void AsyncLoop::cleanup()
{
	for (auto w : completed)
		delete w;

	completed.clear();
}
