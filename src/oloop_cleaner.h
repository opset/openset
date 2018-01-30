#pragma once

#include "oloop.h"
#include "person.h"

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
			openset::db::Table* table;
			openset::db::Person person;
			int64_t linearId; // used as iterator 

		    db::TablePartitioned* parts { nullptr };

		public:
			explicit OpenLoopCleaner(openset::db::Table* table);
			~OpenLoopCleaner() final = default;

            void respawn();

			void prepare() final;
			void run() final;
			void partitionRemoved() final {};
		};
	};
};