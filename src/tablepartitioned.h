#pragma once

#include <queue>

#include "threads/locks.h"
#include "table.h"
#include "people.h"
#include "attributes.h"
#include "message_broker.h"
#include "config.h"

namespace openset
{
    namespace async
    {
        class AsyncLoop;
    };

    namespace query
    {
        class Interpreter;
    }

    namespace db
    {

        class IndexBits;

        struct SegmentPartitioned_s
        {

            enum class SegmentChange_e : int
            {
                enter,
                exit,
                noChange
            };

            string segmentName;
            int64_t segmentHash { 0 };
            int64_t refreshTime{ 86400 };
            query::Macro_s macros;

            int zIndex {100};
            int64_t lastModified {0};
            bool    onInsert {false};
            query::Interpreter* interpreter { nullptr };
            IndexBits* bits { nullptr };

            int changeCount {0};

            SegmentPartitioned_s(
                    const std::string& segmentName,
                    const query::Macro_s& macros,
                    const int64_t refreshTime,
                    const int zIndex,
                    const bool onInsert) :
                segmentName(segmentName),
                segmentHash(MakeHash(segmentName)),
                refreshTime(refreshTime),
                macros(macros),
                zIndex(zIndex),
                onInsert(onInsert)
            {}

            SegmentPartitioned_s() = default;

            ~SegmentPartitioned_s();

            /*
             * These functions maintain an uncompressed cached copy of the index.
             *
             * prepare - called before updating/creating an index.
             * commit - called after a batch of updates is made
             *
             * setBit - flips a bit to the desired state and returns the state change that took place
             */
            IndexBits* prepare(Attributes& attributes); // mounts bits, if they are not already
            void commit(Attributes& attributes); // commits changed bits, if any
            SegmentChange_e setBit(int64_t linearId, bool state); // flip bits by persion linear id

            // returns a new or cached interpreter. Call prepare before calling get Interpreter
            query::Interpreter* getInterpreter(int64_t maxLinearId);

        };


        class TablePartitioned
        {
        public:
            Table* table;
            int partition;
            Attributes attributes;
            AttributeBlob* attributeBlob;
            People people;
            openset::async::AsyncLoop* asyncLoop;
            //openset::revent::ReventManager* triggers;

            // map of segment names to expire times
            std::unordered_map<std::string, int64_t> segmentRefresh;
            std::unordered_map<std::string, int64_t> segmentTTL;
            std::unordered_map<std::string, SegmentPartitioned_s> segments;

            using MailBox = std::vector<revent::TriggerMessage_s>;
            using MessageQueues = std::unordered_map<int64_t, MailBox>;
            MessageQueues messages;

            using InterpreterList = std::vector<SegmentPartitioned_s*>;
            InterpreterList onInsertSegments;

            CriticalSection insertCS;
            atomic<int32_t> insertBacklog;
            std::vector<char*> insertQueue;

            int64_t markedForDeleteStamp{ 0 };

            // when an open-loop is using segments it will increment this value
            // when it is done it will decrement this value.
            //
            // checkForSegmentChanges will not invalidate segments that have changed
            // if this is a non-zero value... instead they will be invalidated at the
            // next opportunity
            int segmentUsageCount {0};

            explicit TablePartitioned(
                Table* table,
                const int partition,
                AttributeBlob* attributeBlob,
                Properties* schema);

            TablePartitioned() = default;

            ~TablePartitioned();

            void markForDeletion()
            {
                markedForDeleteStamp = Now();
            }

            int64_t getMarkedForDeletionStamp() const
            {
                return markedForDeleteStamp;
            }

            void setSegmentTTL(const std::string& segmentName, int64_t TTL)
            {
                if (TTL < 0)
                    return;

                // TODO - this should probably be set to a date	in the next century
                if (TTL == 0)
                    TTL = 86400000LL * 365LL;

                segmentTTL[segmentName] = Now() + TTL;
            }

            void setSegmentRefresh(const std::string& segmentName, int64_t refresh)
            {
                //if (refresh > 0)
                    segmentRefresh[segmentName] = Now() + refresh;
            }

            bool isRefreshDue(const std::string& segmentName)
            {
                if (!segmentRefresh.count(segmentName))
                    return true;
                return segmentRefresh[segmentName] <= Now();
            }

            bool isSegmentExpiredTTL(const std::string& segmentName)
            {
                if (!segmentTTL.count(segmentName))
                    return true;
                return segmentTTL[segmentName] <= Now();
            }

            openset::query::Interpreter* getInterpreter(const std::string& segmentName, int64_t maxLinearId);

            void checkForSegmentChanges();

            InterpreterList& getOnInsertSegments()
            {
                return onInsertSegments;
            }

            // Segmentation helpers

            // delegate that returns a function with a closure containing access to the this class
            // will return a segment from the main index, or a cached copy from the "segments" map, and
            // set the "deleteAfterUsing" parameter appropriately.
            //
            // The Interpreter needs this callback to operate when performing segment math
            std::function<openset::db::IndexBits*(const string&, bool&)> getSegmentCallback();

            void storeAllChangedSegments();

            openset::db::IndexBits* getBits(std::string& segmentName);

            void pushMessage(const int64_t segmentHash, const SegmentPartitioned_s::SegmentChange_e state, std::string uuid);

            void flushMessageMessages();
        };
    };
};