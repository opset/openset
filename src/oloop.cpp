#include "oloop.h"
#include "asyncpool.h"

using namespace openset::async;

int64_t totalRuns = 0;

OpenLoop::OpenLoop(std::string owningTable, oloopPriority_e priority) :
    priority(priority),
    state(oloopState_e::running),
    owningTable(std::move(owningTable)),
    runAt(0),
    runStart(0),
    prepared(false),
    loop(nullptr)
{}

OpenLoop::~OpenLoop()
{
    // calling suicide will set priority to background
    if (priority == oloopPriority_e::realtime)
        globals::async->realtimeDec(this->loop->worker);
}

void OpenLoop::assignLoop(AsyncLoop* loop)
{
    this->loop = loop;
    if (priority == oloopPriority_e::realtime)
        globals::async->realtimeInc(this->loop->worker);

}

bool OpenLoop::inBypass() const
{
    if (priority == oloopPriority_e::realtime)
        return false;

    return (globals::async->getRealtimeRunning(this->loop->worker) != 0);
}

void OpenLoop::scheduleFuture(uint64_t milliFromNow)
{
    runAt = Now() + milliFromNow;
}

void OpenLoop::scheduleAt(uint64_t milliRunAt)
{
    runAt = milliRunAt;
}

void OpenLoop::spawn(OpenLoop* newCell) const
{
    loop->queueCell(newCell);
}

void OpenLoop::suicide()
{
    if (priority == oloopPriority_e::realtime)
    {
        globals::async->realtimeDec(this->loop->worker);
        priority = oloopPriority_e::background;
    }
    state = oloopState_e::done;
}

bool OpenLoop::sliceComplete() const
{
    const auto sliceDivisor = inBypass() ? 3 : 1;
    return (Now() > runStart + (loop->runTime / sliceDivisor));
}

bool OpenLoop::checkCondition()
{
    return true; // always good
}

bool OpenLoop::checkTimer(const int64_t milliNow)
{
    return (milliNow > runAt);
}

void OpenLoop::partitionRemoved()
{}

