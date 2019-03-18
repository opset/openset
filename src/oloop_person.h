#pragma once

#include "common.h"
#include "oloop.h"
#include "shuttle.h"
#include "database.h"

namespace openset
{
    namespace db
    {
        class Table;
        class TablePartitioned;
    };

    namespace async
    {

        class OpenLoopPerson : public OpenLoop
        {

            Shuttle<int>* shuttle;
            openset::db::Database::TablePtr table;
            int64_t uuid;

        public:
            
            explicit OpenLoopPerson(
                Shuttle<int>* shuttle,
                const openset::db::Database::TablePtr table,
                const int64_t uuid);

            void prepare() final;
            bool run() final;
            void partitionRemoved() final;
        };
               
    }
}