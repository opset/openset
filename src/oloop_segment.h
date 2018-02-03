#pragma once

#include "common.h"
#include "oloop.h"
#include "shuttle.h"
#include "database.h"
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
		class OpenLoopSegment : public OpenLoop
		{
		public:
			query::QueryPairs macrosList;
			ShuttleLambda<openset::result::CellQueryResult_s>* shuttle;
			openset::db::Database::TablePtr table;
			openset::db::TablePartitioned* parts;
			int64_t maxLinearId;
			int64_t currentLinId;
			Person person;
			openset::query::Interpreter* interpreter;
			int instance;
			int runCount;
			int64_t startTime;
			int population;
			int popEvaluated;
			openset::query::Indexing indexing;
			openset::db::IndexBits* index;
			openset::result::ResultSet* result;

			std::unordered_set<std::string> segmentWasCached;

			query::QueryPairs::iterator macroIter;
			query::Macro_s macros;

			openset::query::BitMap resultBits;

			std::string resultName;

			explicit OpenLoopSegment(
				ShuttleLambda<openset::result::CellQueryResult_s>* shuttle,
				openset::db::Database::TablePtr table,
				const query::QueryPairs macros,
				openset::result::ResultSet* result,
				const int instance);

			~OpenLoopSegment() final;

			void storeResult(std::string name, int64_t count) const;

			// store segments that have a TTL
			void storeSegments();

			bool nextMacro();

			void prepare() final;
			void run() final;
			void partitionRemoved() final;
		};
	}
}
