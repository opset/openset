#pragma once

#include <queue>

#include "threads/locks.h"
#include "table.h"
#include "people.h"
#include "attributes.h"
#include "triggers.h"

namespace openset
{
	namespace async
	{
		class AsyncLoop;
	};

	namespace db
	{
		class TablePartitioned
		{
		public:
			Table* table;
			int partition;
			Attributes attributes;
			AttributeBlob* attributeBlob;
			People people;
			openset::async::AsyncLoop* asyncLoop;
			openset::trigger::Triggers* triggers;

			// map of segment names to expire times
			std::unordered_map<std::string, int64_t> segmentRefresh;
			std::unordered_map<std::string, int64_t> segmentTTL;

			CriticalSection insertCS;
			atomic<int32_t> insertBacklog;
			std::vector<char*> insertQueue;
			
			explicit TablePartitioned(
				Table* table,
				int partition,
				AttributeBlob* attributeBlob,
				Columns* schema);

			TablePartitioned() = delete;

			void setSegmentTTL(std::string segmentName, int64_t TTL)
			{
				if (TTL < 0)
					return;

				// TODO - this should probably be set to a date	in the next century
				if (TTL == 0) 
					TTL = 86400000LL * 365LL;
				
				segmentTTL[segmentName] = Now() + TTL;
			}

			void setSegmentRefresh(std::string segmentName, int64_t Refresh)
			{
				if (Refresh > 0)
					segmentRefresh[segmentName] = Now() + Refresh;
			}

			bool isRefreshDue(std::string segmentName)
			{
				if (!segmentRefresh.count(segmentName))
					return true;
				return segmentRefresh[segmentName] < Now();
			}

			bool isSegmentExpiredTTL(std::string segmentName)
			{
				if (!segmentTTL.count(segmentName))
					return true;
				return segmentTTL[segmentName] < Now();
			}

		};
	};
};