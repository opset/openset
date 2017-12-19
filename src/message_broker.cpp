#include "message_broker.h"

#include "time/epoch.h"
#include "table.h"
#include "tablepartitioned.h"
#include "http_cli.h"

void openset::trigger::Broker_s::webhookThread(MessageBroker* broker)
{
    thread th([&, broker]()
    {
        vector<triggerMessage_s> messageList;

        int backOff = 0;

        atomic<bool> done = false;
        atomic<bool> retry = false;

        auto done_cb = [&, broker](
            const http::StatusCode status, const bool error, char* data, const size_t size)
        {
            done = true;
            retry = error;
        };

        auto rest = std::make_shared<openset::web::Rest>(host + ":" + to_string(port));
        
        while (true)
        {
            if (!messageList.size())
            {
                messageList = broker->pop(triggerName, subscriberName, 500);
            }

            if (messageList.size())
            {
                cjson payload;

                auto messageArray = payload.setArray("messages");

                for (auto m : messageList)
                {
                    auto msg = messageArray->pushObject();
                    msg->set("stamp", m.stamp);
                    msg->set("stamp_iso", Epoch::EpochToISO8601(m.stamp));
                    msg->set("uid", m.uuid);
                    msg->set("message", m.message);
                }

                payload.set("remaining", broker->size(triggerName, subscriberName));

                auto buffer = cjson::Stringify(&payload);

                done = false;
                retry = false;
                rest->request("POST", path, {}, &buffer[0], buffer.length(), done_cb);

                while (!done)
                    ThreadSleep(55); // move to event pump

                if (retry)
                {
                    if (backOff < 60) // greather than 60 seconds
                        ++backOff;
                }
                else
                {
                    messageList.clear(); // reset the messageList, we got an OK
                    backOff = 0;
                }
            }
            else
            {
                ThreadSleep(500 * backOff);
            }

        }
    });

    th.detach();
}

openset::trigger::MessageBroker::MessageBroker()
{}

openset::trigger::MessageBroker::~MessageBroker()
{}

std::vector<openset::trigger::Queue*> openset::trigger::MessageBroker::getAllQueues(const int64_t triggerId)
{
	std::vector<openset::trigger::Queue*> queues;

	for (auto &q:queueMap)
	{
		if (q.first == triggerId)
		{
			for (auto &t:q.second)
				queues.push_back(&t.second);
		}
	}

	return queues;
}

void openset::trigger::MessageBroker::registerSubscriber(
	const std::string triggerName,
	const std::string subscriberName,
    const std::string host,
    const int port,
    const std::string path,
	const int64_t hold)
{
	csLock lock(cs); // scoped lock
	
	const auto key = std::make_pair(triggerName, subscriberName);
	const auto sub = subscribers.find(key);

	if (sub != subscribers.end()) // found
	{
		sub->second.hold = hold;
        sub->second.host = host;
        sub->second.port = port;
        sub->second.path = path;
	} 
	else // not found
	{
		auto newSub = subscribers.emplace(key, Broker_s{ triggerName, subscriberName, host, port, path, hold });

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
        info.webhookThread(this);
	}
}

void openset::trigger::MessageBroker::backClean()
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
		{
			cout << q.front().uuid << " - " << q.front().message << " hold:" << sub.second.hold << " stamp:" << q.front().stamp << " line:" << expireLine <<  endl;
			q.pop();
		}
	}
}

void openset::trigger::MessageBroker::push(
    const std::string trigger, 
	std::vector<triggerMessage_s>& messages)
{
	csLock lock(cs); // scoped lock

	// make a triggerId
    const auto triggerId = MakeHash(trigger);

	// get list of all subscribers to messages for this trigger
	auto subQueues = getAllQueues(triggerId);

	for (auto &m : messages)
	{
		// insert message m in each subscribed queue for this trigger
		for (auto q : subQueues)
			q->push(m);

	}	

	messages.clear();
	backClean();
}

std::vector<openset::trigger::triggerMessage_s> openset::trigger::MessageBroker::pop(
	const std::string triggerName, 
	const std::string subscriberName, 
	const int64_t max)
{
	std::vector<openset::trigger::triggerMessage_s> result;

	csLock lock(cs); // scoped lock

    const auto key = std::make_pair(triggerName, subscriberName);
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

int64_t openset::trigger::MessageBroker::size(std::string triggerName, std::string subscriberName)
{
	csLock lock(cs); // scoped lock

    const auto key = std::make_pair(triggerName, subscriberName);
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

void openset::trigger::MessageBroker::run()
{
	csLock lock(cs); // scoped lock
	backClean();
}
