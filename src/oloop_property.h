#pragma once

#include <vector>
#include <regex>

#include "database.h"
#include "common.h"
#include "result.h"
#include "oloop.h"
#include "shuttle.h"
#include "var/var.h"
#include "dbtypes.h"

namespace openset
{
    namespace db
    {
        class Table;
        class TablePartitioned;
        class IndexBits;
    };

    namespace async
    {
        class OpenLoopProperty : public OpenLoop
        {
        public:
            enum class PropertyQueryMode_e : int
            {
                all,
                rx,
                sub,
                gt,
                gte,
                lt,
                lte,
                eq,
                between, // gte and lt
            };

            using Ids = vector<int64_t>;
            using GroupMap = unordered_map<int64_t,Ids>; // bucket -> IdList

            struct ColumnQueryConfig_s
            {
                std::string propName;
                db::PropertyTypes_e propType;
                int propIndex;

                PropertyQueryMode_e mode { PropertyQueryMode_e::all };

                std::vector<std::string> segments{ "*" }; // default to all

                // using cvars because I can put strings, bools, doubles and ints int them and
                // don't need to break out separate filter and bucket vars for
                // each in this structure.
                cvar bucket{ 0 }; // histogramming
                cvar filterLow{ 0 };
                cvar filterHigh{ 0 };

                std::regex rx;
            };

            using SegmentNames = std::vector<std::string>;

        private:

            ShuttleLambda<result::CellQueryResult_s>* shuttle;

            ColumnQueryConfig_s config;

            openset::db::Database::TablePtr table;
            db::TablePartitioned* parts;
            result::ResultSet* result;

            int64_t stopBit{ 0 };
            int64_t instance{ 0 };

            std::vector<db::IndexBits*> segments;

            // loop locals
            result::RowKey rowKey;

            GroupMap groups;
            GroupMap::iterator groupsIter;

        public:

            explicit OpenLoopProperty(
                ShuttleLambda<result::CellQueryResult_s>* shuttle,
                openset::db::Database::TablePtr table,
                ColumnQueryConfig_s config,
                openset::result::ResultSet* result,
                const int64_t instance);

            ~OpenLoopProperty() final;

            void prepare() final;
            bool run() final;
            void partitionRemoved() final;
        };

    }
}