#pragma once

#include "database.h"
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

		class OpenLoopRetrigger : public OpenLoop
		{
			openset::db::Database::TablePtr table;
			openset::db::Person person;
			int64_t linearId; // used as iterator 
			int64_t lowestStamp; // lowest non-expired stamp, for reschedule

		    db::TablePartitioned* parts { nullptr };

		public:
			explicit OpenLoopRetrigger(openset::db::Database::TablePtr table);
			~OpenLoopRetrigger() final = default;

			void prepare() final;
			void run() final;
			void partitionRemoved() final {};
		};
	};
};