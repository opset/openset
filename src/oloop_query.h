#pragma once
#include "common.h"
#include "oloop.h"
#include "shuttle.h"
#include "querycommon.h"
#include "queryindexing.h"
#include "queryinterpreter.h"
#include "result.h"

namespace openset
{
	namespace db
	{
		class Table;
		class TablePartitioned;
	};

	namespace async
	{
		class OpenLoopQuery : public OpenLoop
		{
		public:
			openset::query::Macro_s macros;
			ShuttleLambda<openset::result::CellQueryResult_s>* shuttle;
			openset::db::Table* table;
			openset::db::TablePartitioned* parts;
			int64_t maxLinearId;
			int32_t currentLinId;
			Person person;
			openset::query::Interpreter* interpreter;
			int instance;
			int runCount;
			int64_t startTime;
			int population;
			openset::query::Indexing indexing;
			openset::db::IndexBits* index;
			openset::result::ResultSet* result;

			explicit OpenLoopQuery(
				ShuttleLambda<openset::result::CellQueryResult_s>* shuttle,
				openset::db::Table* table,
				openset::query::Macro_s macros, 
				openset::result::ResultSet* result,
				int instance);

			~OpenLoopQuery() override;

			void prepare() override;
			void run() override;
			void partitionRemoved() override;
		};
	}
}
