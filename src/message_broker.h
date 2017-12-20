#pragma once
#include <queue>
#include <unordered_map>

#include "common.h"
#include "trigger.h"
#include "threads/locks.h"
#include <queue>

namespace openset
{
	namespace db // forwards
	{
		class Table;
		class TablePartitioned;
	};

	namespace revent
	{
        class MessageBroker;

		struct Broker_s
		{
			std::string reventName;
			std::string subscriberName;
            std::string host;
            int port;
            std::string path;
			int64_t triggerId;
			int64_t subscriberId;
			int64_t hold;

			Broker_s(
				const std::string reventName, 
				const std::string subscriberName, 
                const std::string host,
                const int port,
                const std::string path,
				const int64_t hold) :
				reventName(reventName),
				subscriberName(subscriberName),
                host(host),
                port(port),
                path(path),
				triggerId(MakeHash(reventName)),
				subscriberId(MakeHash(subscriberName)),
				hold(hold)
			{}

            void webhookThread(MessageBroker* broker);
		};
		
		// independent queue for each subscriber
		using Queue = queue<triggerMessage_s>;
		// map of subscriber ID to message queue
		using Subscriptions = unordered_map<int64_t, Queue>;
		// map of trigger ids, to subscribers
		using QueueMap = unordered_map<int64_t, Subscriptions>;

		// subscriber information note: std::pair<triggerName, subscriberName>
		using SubscriberMap = std::unordered_map<std::pair<std::string, std::string>, Broker_s>;

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
			std::vector<Queue*> getAllQueues(const int64_t triggerId);
			void backClean();

		public:

			// register a subscriber for this queue. Without a subscriber 
			// messages emitted by trigger scripts are discarded. The holdTime
			// indicates how many milliseconds a message may remain in the queue
			// before it is discarded
			void registerSubscriber(
				const std::string reventName,
				const std::string subscriberName,
                const std::string host,
                const int port,
                const std::string path,
				const int64_t hold);

			void push(
				const std::string trigger, 
				std::vector<triggerMessage_s>& messages);

			std::vector<triggerMessage_s> pop(
				const std::string triggerName, 
				const std::string subscriberName, 
				const int64_t max);

			int64_t size(std::string triggerName, std::string subscriberName);

			// perform queue maintenance, expire old messages, etc.
			void run();
		};
	};
};