#pragma once

#include "common.h"
#include "customers.h"
#include "database.h"
#include "threads/locks.h"
#include "properties.h"
#include "message_broker.h"
#include "querycommon.h"
#include "var/var.h"
#include "property_mapping.h"
#include "robin_hood.h"

using namespace std;

namespace openset
{
    namespace db
    {

        class Database;
        class PropertyMapping;
        class TablePartitioned;
        class AttributeBlob;

        using CustomerIndexProps = std::vector<int>;

        struct SegmentTtl_s
        {
            string segmentName;
            int64_t TTL {0};

            SegmentTtl_s(
                    const std::string& segmentName,
                    const int64_t TTL) :
                segmentName(segmentName),
                TTL(TTL)
            {}

            SegmentTtl_s() {};
        };

        struct SegmentRefresh_s
        {
            string segmentName;
            int64_t refreshTime{ 86400 };
            query::Macro_s macros;

            int zIndex {100};
            int64_t lastModified {0};
            int64_t lastHash {0};
            bool    onInsert {false};

            SegmentRefresh_s(
                    const std::string& segmentName,
                    const query::Macro_s& macros,
                    const int64_t refreshTime,
                    const int zIndex,
                    const bool onInsert) :
                segmentName(segmentName),
                refreshTime(refreshTime),
                macros(macros),
                zIndex(zIndex),
                onInsert(onInsert)
            {
                lastModified = Now();
                lastHash = MakeHash(macros.rawScript);
            }

            SegmentRefresh_s() = default;

            int64_t getRefresh() const
            {
                return refreshTime;
            }
        };

        class Table
        {
            // partition specific object container
            string name;
            CriticalSection cs;

            // global objects
            Database* database;

            // used when accessing the segmentTLL and
            // segmentRefresh maps
            CriticalSection segmentCS;
            // map of segments, their TTLs, last refresh times, etc
            using SegmentTtl = robin_hood::unordered_map<std::string, SegmentTtl_s, robin_hood::hash<string>>;
            using SegmentRefresh = robin_hood::unordered_map<std::string, SegmentRefresh_s, robin_hood::hash<string>>;

            SegmentTtl segmentTTL;
            // list of segments that auto update and the code to update them
            SegmentRefresh segmentRefresh;

            // customer list ordering indexes
            CustomerIndexProps indexedProps;

            // global variables
            CriticalSection globalVarCS;
            cvar globalVars;

            // shared objects
            Properties properties;
            PropertyMapping propertyMap;
            openset::revent::MessageBroker messages;

            using EventOrderMapStr = robin_hood::unordered_map<string, int, robin_hood::hash<string>>;
            using EventOrderMapHash = robin_hood::unordered_map<int64_t, int, robin_hood::hash<int>>;
            using PartitionMap = robin_hood::unordered_map<int, TablePartitioned*, robin_hood::hash<int>>;
            using ZombiePartitions = std::queue<TablePartitioned*>;

            EventOrderMapStr eventOrderStrings;
            EventOrderMapHash eventOrderInts;
            AttributeBlob attributeBlob;

            // partition specific objects
            PartitionMap partitions;
            ZombiePartitions zombies;
            int64_t loadVersion;

        public:

            using TablePtr = shared_ptr<Table>;

            bool deleted{ false };
            bool numericCustomerIds{ false };

            // Table Settings
            int eventMax{ 5000 }; // remove oldest rows if more than rowCull
            int64_t tzOffset{ 0 }; // UTC
            int64_t eventTtl{ 86'400'000LL * 365LL * 5LL }; // auto cull older than stampCull
            int64_t sessionTime{ 60'000LL * 30LL }; // 30 minutes
            int64_t maintInterval{ 86'400'000LL }; // trim, index, clean, etc (daily)
            int64_t segmentInterval{ 1'000 }; // update segments every second
            int indexCompression{ 5 }; // 1-20 - 1 is slower, but smaller, 20 is faster and bigger
            int personCompression{ 5 }; // 1-20 - 1 is slower, but smaller, 20 is faster and bigger

            int64_t tableHash;

            explicit Table(const string &name, const bool numericIds, openset::db::Database* database);
            ~Table();

            TablePtr getSharedPtr() const;

            void initialize();
            void createMissingPartitionObjects();

            TablePartitioned* getPartitionObjects(const int32_t partition, const bool create);
            void releasePartitionObjects(const int32_t partition);

            void propagateCustomerIndexes();

            int64_t getSessionTime() const
            {
                return sessionTime;
            }

            Properties* getProperties()
            {
                return &properties;
            }

            PropertyMapping* getPropertyMapper()
            {
                return &propertyMap;
            }

            EventOrderMapHash* getEventOrderHashes()
            {
                return &eventOrderInts;
            }

            CustomerIndexProps* getCustomerIndexProps()
            {
                return &indexedProps;
            }

            EventOrderMapStr* getEventOrderStrings()
            {
                return &eventOrderStrings;
            }

            CriticalSection* getLock()
            {
                return &cs;
            }

            CriticalSection* getGlobalsLock()
            {
                return &globalVarCS;
            }

            AttributeBlob* getAttributeBlob()
            {
                return &attributeBlob;
            }

            // returns a COPY of the global
            cvar getGlobals()
            {
                csLock lock(globalVarCS);
                return globalVars;
            }

            cvar* getGlobalsPtr()
            {
                return &globalVars;
            }

            CriticalSection* getSegmentLock()
            {
                return &segmentCS;
            }

            SegmentTtl* getSegmentTTL()
            {
                return &segmentTTL;
            }

            SegmentRefresh* getSegmentRefresh()
            {
                return &segmentRefresh;
            }

            const string& getName() const
            {
                return name;
            }

            const int64_t getTableHash() const
            {
                return tableHash;
            }

            openset::revent::MessageBroker* getMessages()
            {
                return &messages;
            }

            Database* getDatabase() const
            {
                return database;
            }

            int64_t getLoadVersion() const
            {
                return loadVersion;
            }

            void forceReload()
            {
                ++loadVersion;
            }

            /*std::unordered_map<string, openset::revent::reventSettings_s*>* getTriggerConf()
            {
                return &triggerConf;
            }*/

            void setSegmentRefresh(
                const std::string& segmentName,
                const query::Macro_s& macros,
                const int64_t refreshTime,
                const int zIndex,
                const bool onInsert);
            void removeSegmentRefresh(const std::string& segmentName);

            void setSegmentTtl(std::string segmentName, const int64_t TTL)
            {
                csLock lock(segmentCS);
                segmentTTL.emplace(segmentName, SegmentTtl_s{ segmentName, TTL });
            }

            void serializeTable(cjson* doc);
            void serializeTriggers(cjson* doc);
            void serializeSettings(cjson* doc) const;

            void deserializeTable(const cjson* doc);
            void deserializeTriggers(const cjson* doc);
            void deserializeSettings(const cjson* doc);

        private:

            void clearZombies();


        };
    };
};
