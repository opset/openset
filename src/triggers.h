#pragma once

#include "common.h"

#include "cjson/cjson.h"
#include <unordered_map>
#include "trigger.h"

namespace openset
{
	namespace db // name spaced forwards
	{
		class Table;
		class TablePartitioned;
		class Columns;
	};
};

namespace openset
{
	namespace revent
	{

		class ReventManager
		{
			openset::db::Table* table;
			openset::db::TablePartitioned* parts;
			openset::db::Columns* columns;
			std::unordered_map<int64_t, Revent*> revents;
			int64_t loadVersion;

		public:
			explicit ReventManager(openset::db::TablePartitioned* parts);
			~ReventManager();

			void start();

			std::unordered_map<int64_t, Revent*>& getTriggerMap()
			{
				return revents;
			}

			Revent* getRevent(const int64_t triggerId)
			{
			    const auto iter = revents.find(triggerId);
				return (iter == revents.end()) ? nullptr : iter->second;
			}			

			void dispatchMessages() const;
			void checkForConfigChange();
		};
	};
};