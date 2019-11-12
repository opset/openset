#pragma once
#include <queue>
#include <unordered_map>

#include "common.h"
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
        struct TriggerMessage_s
        {
            enum class State_e : int
            {
                entered,
                exited
            };

            int64_t stamp { 0 };
            int64_t segmentId { 0 };
            string uuid;
            State_e state { State_e::entered };

            TriggerMessage_s() = default;

            TriggerMessage_s(const int64_t segmentId, const State_e state, std::string &uuidStr) :
                stamp(Now()),
                segmentId(segmentId),
                uuid(uuidStr),
                state(state)
            {}

            TriggerMessage_s(const TriggerMessage_s &other):
                stamp(other.stamp),
                segmentId(other.segmentId),
                state(other.state)
            {
                uuid = other.uuid;
            }

            TriggerMessage_s(TriggerMessage_s&& other) noexcept :
                stamp(other.stamp),
                segmentId(other.segmentId),
                state(other.state)
            {
                uuid = std::move(other.uuid);
            }
        };

        class MessageBroker;

        struct Broker_s
        {
            std::string segmentName;
            std::string subscriberName;
            std::string host;
            int port;
            std::string path;
            int64_t triggerId;
            int64_t subscriberId;
            int64_t hold;
            bool shutdownRequested { false };
            bool shutdownComplete { false };
            CriticalSection cs;

            Broker_s(
                const std::string& segmentName,
                const std::string& subscriberName,
                const std::string& host,
                const int port,
                const std::string& path,
                const int64_t hold) :
                segmentName(segmentName),
                subscriberName(subscriberName),
                host(host),
                port(port),
                path(path),
                triggerId(MakeHash(segmentName)),
                subscriberId(MakeHash(subscriberName)),
                hold(hold)
            {}

            void webHookThread(MessageBroker* broker);
        };

        // independent queue for each subscriber
        using Queue = queue<TriggerMessage_s>;
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

            MessageBroker() = default;
            ~MessageBroker();

        private:
            // returns a list of all queues regardless of Subscriber
            std::vector<Queue*> getAllQueues(const int64_t segmentId);
            void backClean();

        public:

            // register a subscriber for this queue. Without a subscriber
            // messages emitted by trigger scripts are discarded. The holdTime
            // indicates how many milliseconds a message may remain in the queue
            // before it is discarded
            void registerSubscriber(
                const std::string& segmentName,
                const std::string& subscriberName,
                const std::string& host,
                const int port,
                const std::string& path,
                const int64_t hold);

            // delete a subscriber
            bool removeSubscriber(const std::string& segmentName, const std::string& subscriberName);

            // pushes a local cache of messages from a tablePartitioned object
            // into the various subscriber caches and clears the messages vector upon completion
            void push(
                int64_t segmentId,
                std::vector<TriggerMessage_s>& messages);

            // pop up to "max" items from a subscribers queue
            std::vector<TriggerMessage_s> pop(
                const std::string& segmentName,
                const std::string& subscriberName,
                const int64_t max);

            int64_t size(const std::string& segmentName, const std::string& subscriberName);

            // perform queue maintenance, expire old messages, etc.
            void run();
        };
    };
};