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

            using TablePtr = shared_ptr<Table>;
            using TableMap = unordered_map<std::string, TablePtr>;

			TableMap tables;

			explicit Database();
			~Database() = default;

			TablePtr getTable(const std::string& tableName);
			TablePtr newTable(const std::string& tableName);
            void dropTable(const std::string& tableName);

            std::vector<std::string> getTableNames();

			void serialize(cjson* doc);
		};
	};

	namespace globals
	{
		extern openset::db::Database* database;
	}
};
