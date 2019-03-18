#pragma once

#include "oloop.h"
#include "person.h"
#include "database.h"

namespace openset
{
	namespace db
	{
		class Table;
		class TablePartitioned;
	};
};

namespace openset
{
	namespace async
	{

		class OpenLoopCleaner : public OpenLoop
		{
			openset::db::Database::TablePtr table;
			openset::db::Person person;
			int64_t linearId; // used as iterator 

		    db::TablePartitioned* parts { nullptr };

		public:
			explicit OpenLoopCleaner(const openset::db::Database::TablePtr table);
			~OpenLoopCleaner() final = default;

            void respawn();

			void prepare() final;
			bool run() final;
			void partitionRemoved() final {};
		};
	};
};