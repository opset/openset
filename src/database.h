#pragma once

#include <unordered_map>

#include "common.h"
#include "table.h"
#include "threads/locks.h"

namespace openset
{
	namespace db
	{
		class Database
		{
		public:

			CriticalSection cs;

			unordered_map<std::string, Table*> tables;

			explicit Database();
			~Database() = default;

			openset::db::Table* getTable(const std::string& tableName);
			openset::db::Table* newTable(const std::string& tableName);
            void dropTable(const std::string& tableName);

			void serialize(cjson* doc);
		};
	};

	namespace globals
	{
		extern openset::db::Database* database;
	}
};
