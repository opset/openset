#pragma once

#include "common.h"
#include "people.h"
#include "database.h"
#include "threads/locks.h"
#include "columns.h"
#include "message_broker.h"
#include "querycommon.h"
#include "var/var.h"
#include "columnmapping.h"

using namespace std;

namespace openset
{

	namespace trigger
	{
		struct reventSettings_s;
	};

	namespace db
	{

		class Database;
        class ColumnMapping;
		class TablePartitioned;

		struct SegmentTtl_s
		{
			string segmentName;
			int64_t TTL;

			SegmentTtl_s(const std::string segmentName, const int64_t TTL) :
				segmentName(segmentName),
				TTL(TTL)
			{}
		};

		struct SegmentRefresh_s
		{
			string segmentName;
			int64_t refreshTime;
			query::Macro_s macros;

			SegmentRefresh_s(
				const std::string segmentName,
				const query::Macro_s macros,
				const int64_t refreshTime) :
				segmentName(segmentName),
				refreshTime(refreshTime),
				macros(macros)
			{}

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
			std::unordered_map<std::string, SegmentTtl_s> segmentTTL;
			// list of segments that auto update and the code to update them
			std::unordered_map<std::string, SegmentRefresh_s> segmentRefresh;

			// global variables
			CriticalSection globalVarCS;
			cvar globalVars;

			// shared objects
			Columns columns;
            ColumnMapping columnMap;
			openset::revent::MessageBroker messages;

			using zMapStr = std::unordered_map<string, int>;
			using zMapHash = std::unordered_map<int64_t, int>;
            using PartitionMap = unordered_map<int, TablePartitioned*>;
            using ZombiePartitions = std::queue<TablePartitioned*>;

			zMapStr zOrderStrings;
			zMapHash zOrderInts;
			std::unordered_map<std::string, openset::revent::reventSettings_s*> triggerConf;
			AttributeBlob attributeBlob;
			// partition specific objects
			PartitionMap partitions;
            ZombiePartitions zombies;
			int64_t loadVersion;

		public:

            using TablePtr = shared_ptr<Table>;

            bool deleted{ false };

            // Table Settings
			int eventMax{ 5000 }; // remove oldest rows if more than rowCull
            int64_t tzOffset{ 0 }; // UTC
			int64_t eventTtl{ 86'400'000LL * 365LL * 5 }; // auto cull older than stampCull
			int64_t sessionTime{ 60'000LL * 30LL }; // 30 minutes
            int64_t maintInterval{ 86'400'000LL }; // trim, index, clean, etc (daily)
            int64_t reventInterval{ 60'000 }; // check for re-events every 60 seconds
            int64_t segmentInterval{ 60'000 }; // update segments every 60 seconds
            int indexCompression{ 5 }; // 1-20 - 1 is slower, but smaller, 20 is faster and bigger
            int personCompression{ 5 }; // 1-20 - 1 is slower, but smaller, 20 is faster and bigger

			explicit Table(const string &name, openset::db::Database* database);
			~Table();

            TablePtr getSharedPtr() const;

            void initialize();
			void createMissingPartitionObjects();

			TablePartitioned* getPartitionObjects(const int32_t partition, const bool create);
			void releasePartitionObjects(const int32_t partition);

			int64_t getSessionTime() const
			{
				return sessionTime;
			}

			inline Columns* getColumns()
			{
				return &columns;
			}

            inline ColumnMapping* getColumnMapper()
			{
			    return &columnMap;
			}

			inline zMapHash* getZOrderHashes() 
			{
				return &zOrderInts;
			}

			inline zMapStr* getZOrderStrings()
			{
				return &zOrderStrings;
			}

			inline CriticalSection* getLock() 
			{
				return &cs;
			}

			inline CriticalSection* getGlobalsLock()
			{
				return &globalVarCS;
			}

			inline AttributeBlob* getAttributeBlob()
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

			inline CriticalSection* getSegmentLock()
			{
				return &segmentCS;
			}

			inline std::unordered_map<std::string, SegmentTtl_s>* getSegmentTTL()
			{
				return &segmentTTL;
			}

			inline std::unordered_map<std::string, SegmentRefresh_s>* getSegmentRefresh()
			{
				return &segmentRefresh;
			}

			inline const string& getName() const
			{
				return name;
			}

			inline openset::revent::MessageBroker* getMessages()
			{
				return &messages;
			}

			inline Database* getDatabase() const
			{
				return database;
			}

			inline int64_t getLoadVersion() const
			{
				return loadVersion;
			}

			inline void forceReload()
			{
				++loadVersion;
			}

			std::unordered_map<string, openset::revent::reventSettings_s*>* getTriggerConf()
			{
				return &triggerConf;
			}

			void setSegmentRefresh(std::string& segmentName, const query::Macro_s& macros, const int64_t refreshTime)
			{
				csLock lock(segmentCS);
				segmentRefresh.emplace(segmentName, SegmentRefresh_s{ segmentName, macros, refreshTime });
			}

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
