#pragma once

#include "common.h"

#include <unordered_map>

#include "threads/locks.h"
#include "dbtypes.h"

using namespace std;

namespace openset
{
	namespace db
	{
		class Columns
		{
		public:

			struct columns_s
			{
				string name;
				int32_t idx{0};
				columnTypes_e type{ columnTypes_e::freeColumn };
				bool isProp{false};
				bool deleted{ false };
			};

			// shared lock (uses spin locks)
			CriticalSection lock;

			columns_s columns[MAXCOLUMNS];
			unordered_map<string, columns_s*> nameMap;
			int columnCount{ 0 };

			Columns();
			~Columns();

			// get a column record, this will always 
			// return something
			columns_s* getColumn(int column);

			// get a column by name, this will return a nullptr
			// if none match
			columns_s* getColumn(string name);

			void deleteColumn(columns_s* columnInfo);

			int getColumnCount() const;

			void setColumn(int index, string name, columnTypes_e type, bool isSet, bool deleted = false);

		};
	};
};
