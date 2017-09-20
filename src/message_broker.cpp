#include "message_broker.h"

#include "table.h"
#include "tablepartitioned.h"

openset::trigger::MessageBroker::MessageBroker()
{
	
}

openset::trigger::MessageBroker::~MessageBroker()
{
	
}

std::vector<openset::trigger::Queue*> openset::trigger::MessageBroker::getAllQueues(int64_t triggerId)
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
	std::string triggerName,
	std::string subscriberName,
	int64_t hold)
{
	csLock lock(cs); // scoped lock
	
	auto key = std::make_pair(triggerName, subscriberName);
	auto sub = subscribers.find(key);

	if (sub != subscribers.end()) // found
	{
		sub->second.hold = hold;
	} 
	else // not found
	{
		auto newSub = subscribers.emplace(key, broker_s{ triggerName, subscriberName, hold });

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
		auto expireLine = Now() - sub.second.hold;

		// pop anything expired from the queue
		while (!q.empty() && q.front().stamp < expireLine)
		{
			cout << q.front().uuid << " - " << q.front().message << " hold:" << sub.second.hold << " stamp:" << q.front().stamp << " line:" << expireLine <<  endl;
			q.pop();
		}
	}
}

void openset::trigger::MessageBroker::push(
	std::string trigger, 
	std::vector<triggerMessage_s>& messages)
{
	csLock lock(cs); // scoped lock

	// make a triggerId
	auto triggerId = MakeHash(trigger);

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
	std::string triggerName, 
	std::string subscriberName, 
	int64_t max)
{
	std::vector<openset::trigger::triggerMessage_s> result;

	csLock lock(cs); // scoped lock

	auto key = std::make_pair(triggerName, subscriberName);
	auto sub = subscribers.find(key);

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

	auto key = std::make_pair(triggerName, subscriberName);
	auto sub = subscribers.find(key);

	if (sub != subscribers.end()) // found
	{
		auto t = queueMap.find(sub->second.triggerId);

		if (t == queueMap.end())
			return 0;

		auto s = t->second.find(sub->second.subscriberId);

		if (s == t->second.end())
			return 0;

		return s->second.size();
	}
	
	return 0;
}

void openset::trigger::MessageBroker::run()
{
	csLock lock(cs); // scoped lock
	backClean();
}
