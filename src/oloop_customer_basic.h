#pragma once
#include "common.h"
#include "database.h"
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
        class OpenLoopCustomerBasicList : public OpenLoop
        {
        public:
            openset::query::Macro_s macros;
            ShuttleLambda<openset::result::CellQueryResult_s>* shuttle;
            openset::db::Database::TablePtr table;
            openset::db::TablePartitioned* parts;
            int64_t maxLinearId;
            int64_t currentLinId;
            Customer person;
            openset::query::Interpreter* interpreter;
            int instance;
            int runCount;
            int64_t startTime;
            int population;
            openset::query::Indexing indexing;
            openset::db::IndexBits* index;
            openset::result::ResultSet* result;

            std::vector<int64_t> cursor;
            bool descending;
            int limit;

            using BasicCustomerList = std::vector<std::pair<int64_t,int>>;

            BasicCustomerList indexedList;
            BasicCustomerList::iterator iter;

            explicit OpenLoopCustomerBasicList(
                ShuttleLambda<openset::result::CellQueryResult_s>* shuttle,
                openset::db::Database::TablePtr table,
                openset::query::Macro_s macros,
                openset::result::ResultSet* result,
                const std::vector<int64_t>& cursor,
                const bool descending,
                const int limit,
                int instance);

            ~OpenLoopCustomerBasicList() final;

            void prepare() final;
            bool run() final;
            void partitionRemoved() final;
        };
    }
}
