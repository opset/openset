#pragma once

#include "common.h"
#include "logger.h"
#include "table.h"

#include "threads/locks.h"

#include <unordered_map>

using namespace std;

namespace openset
{
	namespace db
	{
		class Database
		{
		public:

			CriticalSection cs;

			unordered_map<string, Table*> tables;

			explicit Database();
			~Database();

			openset::db::Table* getTable(string TableName);
			openset::db::Table* newTable(string TableName);

			void initializeTables();

			void serialize(cjson* doc);

			void loadConfig();
			void saveConfig();
		};
	};

	namespace globals
	{
		extern openset::db::Database* database;
	}
};
