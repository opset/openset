#include "message_broker.h"

#include "time/epoch.h"
#include "table.h"
#include "tablepartitioned.h"
#include "http_cli.h"

void openset::revent::Broker_s::webHookThread(MessageBroker* broker)
{
    thread th([&, broker]()
    {

        const auto maxMessages = 500;
        const auto maxBackOff = 300 * 4; // 5 minutes (in 250ms sleeps)

        vector<TriggerMessage_s> messageList;

        auto backOff = 0;

        atomic<bool> done = false;
        atomic<bool> retry = false;

        const auto done_cb = [&](
            const http::StatusCode status, const bool error, char* data, const size_t size)
        {
            done = true;
            retry = error;
        };

        auto hostPort = host + ":" + to_string(port);
        auto rest = std::make_shared<openset::web::Rest>(0, hostPort);
        std::string lastHost = hostPort;

        while (true)
        {
            hostPort = host + ":" + to_string(port);

            { // scoped lock
                csLock lock(cs);
                if (lastHost != hostPort)
                {
                    rest = std::make_shared<openset::web::Rest>(0, hostPort);
                    lastHost = hostPort;
                }
                if (shutdownRequested)
                {
                    shutdownComplete = true;
                    return;
                }
            }

            if (!messageList.size())
            {
                messageList = broker->pop(segmentName, subscriberName, maxMessages);
            }

            if (messageList.size())
            {
                cjson payload;

                auto messageArray = payload.setArray("messages");

                for (auto& m : messageList)
                {
                    auto msg = messageArray->pushObject();
                    msg->set("stamp", m.stamp);
                    msg->set("stamp_iso", Epoch::EpochToISO8601(m.stamp));
                    msg->set("id", m.uuid);
                    msg->set("state", m.state == TriggerMessage_s::State_e::entered ? "entered" : "exited");
                }

                auto buffer = cjson::stringify(&payload);

                const auto backlog = broker->size(segmentName, subscriberName);

                done = false;
                retry = false;

                rest->request(
                    "POST",
                    path,
                    {
                        { "segment", segmentName },
                        { "subscriber", subscriberName },
                        { "count", to_string(messageList.size())},
                        { "remaining", to_string(backlog) }
                    },
                    &buffer[0],
                    buffer.length(),
                    done_cb);

                // wait for response from remote endpoint
                while (!done)
                    ThreadSleep(55); // TODO move to eventing method

                if (retry)
                {
                    ++backOff;
                }
                else
                {
                    messageList.clear(); // reset the messageList, we got an OK
                    backOff = backlog ? 0 : 1; // immediate, or wait 250ms second if empty
                }
            }
            else
            {
                // no data, check for data in 250ms.
                backOff = 1;
            }

            if (backOff > maxBackOff)
                backOff = maxBackOff;

            // loop on short sleeps for back off period. This way we won't
            // block for long if a thread shutdown request is encountered.
            for (auto sleeps = 0; sleeps < backOff; ++sleeps)
            {
                {
                    csLock lock(cs); // scoped lock
                    if (shutdownRequested)
                    {
                        shutdownComplete = true;
                        return;
                    }
                }
                ThreadSleep(250);
            }
        }
    });

    th.detach();
}

openset::revent::MessageBroker::~MessageBroker() = default;

std::vector<openset::revent::Queue*> openset::revent::MessageBroker::getAllQueues(const int64_t segmentId)
{
    std::vector<openset::revent::Queue*> queues;

    for (auto &q:queueMap)
    {
        if (q.first == segmentId)
        {
            for (auto &t:q.second)
                queues.push_back(&t.second);
        }
    }

    return queues;
}

void openset::revent::MessageBroker::registerSubscriber(
    const std::string& segmentName,
    const std::string& subscriberName,
    const std::string& host,
    const int port,
    const std::string& path,
    const int64_t hold)
{
    csLock lock(cs); // scoped lock

    const auto key = std::make_pair(segmentName, subscriberName);
    const auto sub = subscribers.find(key);

    if (sub != subscribers.end()) // found
    {
        // update config
        {
            csLock subLock(sub->second.cs); // scoped lock
            sub->second.hold = hold;
            sub->second.host = host;
            sub->second.port = port;
            sub->second.path = path;
        }
    }
    else // not found
    {
        auto newSub = subscribers.emplace(key, Broker_s{ segmentName, subscriberName, host, port, path, hold });

        // emplace returns a goofy pair of pairs, our pair is in .first
        auto &info = newSub.first->second;

        // if the queueMap is missing an entry for this trigger
        // add a subscribers object keyed to this triggerId
        if (!queueMap.count(info.triggerId))
            queueMap.emplace(info.triggerId, Subscriptions{});

        auto &queue = queueMap[info.triggerId]; // for some readability

        // if the queueMap->subscriptions object does not contain
        // a queue for this subscriberId, then add one
        if (!queue.count(info.subscriberId))
            queue.emplace(info.subscriberId, Queue{});

        // start a thread for this sub
        info.webHookThread(this);
    }
}

bool openset::revent::MessageBroker::removeSubscriber(
    const std::string& segmentName,
    const std::string& subscriberName)
{
    const auto key = std::make_pair(segmentName, subscriberName);
    const auto sub = subscribers.find(key);

    if (sub != subscribers.end()) // found
    {
        { // scoped lock
            csLock subLock(sub->second.cs);
            sub->second.shutdownRequested = true;
        }

        while (true) // wait for thread to confirm shutdown
        {
            { // scoped lock
                csLock subLock(sub->second.cs);
                if (sub->second.shutdownComplete)
                    break;
            }
            ThreadSleep(55);
        }

        // worker thread has been gracefully stopped
        csLock lock(cs); // scoped lock
        subscribers.erase(key);
        return true;
    }

    return false;
}

void openset::revent::MessageBroker::backClean()
{
    // Note - internal function lock from the caller
    for (auto &sub : subscribers)
    {

        auto t = queueMap.find(sub.second.triggerId);

        if (t == queueMap.end())
            continue;

        auto s = t->second.find(sub.second.subscriberId);

        if (s == t->second.end())
            continue;

        auto& q = s->second;

        // anything older than this is expired
        const auto expireLine = Now() - sub.second.hold;

        // pop anything expired from the queue
        while (!q.empty() && q.front().stamp < expireLine)
            q.pop();
    }
}

void openset::revent::MessageBroker::push(
    int64_t segmentId,
    std::vector<TriggerMessage_s>& messages)
{
    csLock lock(cs); // scoped lock

    // get list of all subscribers waiting on messages for this trigger
    auto subQueues = getAllQueues(segmentId);

    // each segment can have multiple subQueues (different subscribers)
    // to accomodate this we basically insert the message multiple times,
    // once for each subQueue
    for (auto &m : messages)
    {
        // insert message m in each subscribed queue for this trigger
        for (auto q : subQueues)
            q->push(m);

    }

    backClean();
}

std::vector<openset::revent::TriggerMessage_s> openset::revent::MessageBroker::pop(
    const std::string& segmentName,
    const std::string& subscriberName,
    const int64_t max)
{
    std::vector<openset::revent::TriggerMessage_s> result;

    csLock lock(cs); // scoped lock

    const auto key = std::make_pair(segmentName, subscriberName);
    const auto sub = subscribers.find(key);

    if (sub != subscribers.end()) // found
    {
        auto t = queueMap.find(sub->second.triggerId);

        if (t == queueMap.end())
            return result;

        auto s = t->second.find(sub->second.subscriberId);

        if (s == t->second.end())
            return result;

        auto count = 0;

        while (s->second.size() && count < max)
        {
            result.push_back(s->second.front());
            s->second.pop();
            ++count;
        }

        return result;
    }

    return result;

}

int64_t openset::revent::MessageBroker::size(const std::string& segmentName, const std::string& subscriberName)
{
    csLock lock(cs); // scoped lock

    const auto key = std::make_pair(segmentName, subscriberName);
    const auto sub = subscribers.find(key);

    if (sub != subscribers.end()) // found
    {
        if (auto t = queueMap.find(sub->second.triggerId); t == queueMap.end())
        {
            return 0;
        }
        else
        {
            if (const auto s = t->second.find(sub->second.subscriberId); s == t->second.end())
                return 0;
            else
                return s->second.size();
        }
    }

    return 0;
}

void openset::revent::MessageBroker::run()
{
    csLock lock(cs); // scoped lock
    backClean();
}
