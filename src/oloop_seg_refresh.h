#pragma once
#include "common.h"
#include "database.h"
#include "oloop.h"
#include "shuttle.h"
#include "querycommon.h"
#include "queryindexing.h"
#include "queryinterpreter.h"
#include "result.h"
#include "tablepartitioned.h"

namespace openset
{
    namespace db
    {
        class Table;
        class TablePartitioned;
    };

    namespace async
    {
        class OpenLoopSegmentRefresh : public OpenLoop
        {
        public:
            openset::db::TablePartitioned* parts;
            openset::db::Database::TablePtr table;

            int64_t maxLinearId;
            int64_t currentLinId;
            Customer person;
            openset::query::Interpreter* interpreter;
            int instance;
            int runCount;
            int64_t startPopulation {0};

            openset::query::Indexing indexing;
            openset::db::IndexBits* index {nullptr};
            openset::db::IndexBits* bits {nullptr};

            std::unordered_map<std::string, SegmentPartitioned_s>::iterator segmentsIter;

            SegmentPartitioned_s* segmentInfo {nullptr};

            std::string segmentName;
            int64_t segmentHash { 0 };
            query::Macro_s macros;

            explicit OpenLoopSegmentRefresh(openset::db::Database::TablePtr table);

            ~OpenLoopSegmentRefresh() final;

            // store segments that have a TTL
            void storeSegment() const;

            void emitSegmentDifferences(openset::db::IndexBits* before, openset::db::IndexBits* after) const;

            bool nextExpired();

            void prepare() final;
            void respawn();
            bool run() final;
            void partitionRemoved() final {};
        };
    }
}
