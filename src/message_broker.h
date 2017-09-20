#pragma once
#include <queue>
#include <unordered_map>

#include "common.h"
#include "trigger.h"
#include "threads/locks.h"

namespace openset
{
	namespace db // forwards
	{
		class Table;
		class TablePartitioned;
	};

	namespace trigger
	{
		struct broker_s
		{
			std::string triggerName;
			std::string subscriberName;
			int64_t triggerId;
			int64_t subscriberId;
			int64_t hold;

			broker_s(
				std::string triggerName, 
				std::string subscriberName, 
				int64_t hold) :
				triggerName(triggerName),
				subscriberName(subscriberName),
				triggerId(MakeHash(triggerName)),
				subscriberId(MakeHash(subscriberName)),
				hold(hold)
			{}
		};
		
		// independent queue for each subscriber
		using Queue = std::queue<triggerMessage_s>;
		// map of subscriber ID to message queue
		using Subscriptions = unordered_map<int64_t, Queue>;
		// map of trigger ids, to subscribers
		using QueueMap = unordered_map<int64_t, Subscriptions>;

		// subscriber information note: std::pair<triggerName, subscriberName>
		using SubscriberMap = std::unordered_map<std::pair<std::string, std::string>, broker_s>;

		class MessageBroker 
		{
		public:
			CriticalSection cs;
			QueueMap queueMap;
			SubscriberMap subscribers;

			MessageBroker();
			~MessageBroker();

		private:
			// returns a list of all queues regardless of Subscriber
			std::vector<Queue*> getAllQueues(int64_t triggerId);
			void backClean();

		public:

			// register a subscriber for this queue. Without a subscriber 
			// messages emitted by trigger scripts are discarded. The holdTime
			// indicates how many milliseconds a message may remain in the queue
			// before it is discarded
			void registerSubscriber(
				std::string triggerName,
				std::string subscriberName,
				int64_t hold);

			void push(
				std::string trigger, 
				std::vector<triggerMessage_s>& messages);

			std::vector<triggerMessage_s> pop(
				std::string triggerName, 
				std::string subscriberName, 
				int64_t max);

			int64_t size(std::string triggerName, std::string subscriberName);

			// perform queue maintenance, expire old messages, etc.
			void run();
		};
	};
};