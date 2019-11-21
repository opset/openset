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
        class OpenLoopCustomerList : public OpenLoop
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

            CustomerIndexList indexedList;
            CustomerIndexList::iterator iter;

            explicit OpenLoopCustomerList(
                ShuttleLambda<openset::result::CellQueryResult_s>* shuttle,
                openset::db::Database::TablePtr table,
                openset::query::Macro_s macros,
                openset::result::ResultSet* result,
                int instance);

            ~OpenLoopCustomerList() final;

            void prepare() final;
            bool run() final;
            void partitionRemoved() final;
        };
    }
}
