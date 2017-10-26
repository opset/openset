#pragma once

#include "common.h"
#include "people.h"
#include "threads/locks.h"
#include "columns.h"
#include "message_broker.h"
#include "querycommon.h"
#include "var/var.h"

using namespace std;

namespace openset
{

	namespace trigger
	{
		struct triggerSettings_s;
	};

	namespace db
	{

		class Database;
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
			openset::trigger::MessageBroker messages;

			using zMapStr = std::unordered_map<string, int>;
			using zMapInt = std::unordered_map<int64_t, int>;

			zMapStr zOrderStrings;
			zMapInt zOrderInts;
			std::unordered_map<std::string, openset::trigger::triggerSettings_s*> triggerConf;
			AttributeBlob attributeBlob;
			// partition specific objects
			TablePartitioned* partitions[PARTITION_MAX];
			int64_t loadVersion;

		public:
			int rowCull{ 5000 }; // remove oldest rows if more than rowCull
			int64_t stampCull{ 86'400'000LL * 365LL }; // auto cull older than stampCull
			int64_t sessionTime{ 60'000LL * 30LL }; // 30 minutes

			explicit Table(string name, openset::db::Database* database);

			~Table();

			void createMissingPartitionObjects();

			TablePartitioned* getPartitionObjects(int32_t partition);
			void releasePartitionObjects(int32_t partition);

			int64_t getSessionTime() const
			{
				return sessionTime;
			}

			inline Columns* getColumns()
			{
				return &columns;
			}

			inline zMapInt* getZOrderInts() 
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

			inline openset::trigger::MessageBroker* getMessages() 
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

			std::unordered_map<string, openset::trigger::triggerSettings_s*>* getTriggerConf() 
			{
				return &triggerConf;
			}

			void setSegmentRefresh(std::string segmentName, const query::Macro_s macros, const int64_t refreshTime)
			{
				csLock lock(segmentCS);
				segmentRefresh.emplace(segmentName, SegmentRefresh_s{ segmentName, macros, refreshTime });
			}

			void setSegmentTTL(std::string segmentName, const int64_t TTL)
			{
				csLock lock(segmentCS);
				segmentTTL.emplace(segmentName, SegmentTtl_s{ segmentName, TTL });
			}

			// serialize table structure, pk, trigger names, into cjson branch
			void serializeTable(cjson* doc);
			// used by intra-node config to serialize the transfer of 
			void serializeTriggers(cjson* doc);

			void deserializeTable(cjson* doc);
			void deserializeTriggers(cjson* doc);

			void loadConfig();
			void saveConfig();	
		};
	};
};
