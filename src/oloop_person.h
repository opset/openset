#pragma once

#include "common.h"
#include "oloop.h"
#include "shuttle.h"

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
            db::Table* table;
            int64_t uuid;

        public:
            
            explicit OpenLoopPerson(
                Shuttle<int>* shuttle,
                openset::db::Table* table,
                const int64_t uuid);

            void prepare() final;
            void run() final;
            void partitionRemoved() final;
        };
               
    }
}