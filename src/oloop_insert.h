#pragma once

#include "common.h"
#include "oloop.h"
#include "database.h"
#include <unordered_set>

namespace openset
{
	namespace db
	{
		class Database;
		class TablePartitioned;
	};
};

namespace openset
{
	namespace async
	{
		class AsyncLoop;


		/*
			InsertCell - inserts event(s) into user records and
				updates indexes.
		*/

		class OpenLoopInsert : public OpenLoop
		{
		private:

			int sleepCounter = 0;

            openset::db::Database::TablePtr table;
			openset::db::TablePartitioned* tablePartitioned;
			int runCount;

			std::vector<char*> localQueue;
			decltype(localQueue)::iterator queueIter;


		public:

			explicit OpenLoopInsert(openset::db::Database::TablePtr table);
			~OpenLoopInsert() final = default;

			void prepare() final;
			bool run() final;
			void partitionRemoved() final {};
		};
	};
};
