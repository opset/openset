#pragma once

#include <array>
#include <unordered_map>

#include "common.h"
#include "threads/locks.h"

namespace openset
{
    namespace db
    {

        class Table;
        class Attributes;

        struct ColumnMap_s
        {
            int64_t hash{0};
            int64_t rowBytes{0};
            int32_t refCount{0};
            int32_t columnCount{0};
            int32_t uuidColumn{0};
            int32_t sessionColumn{0};
            std::array<int32_t, MAXCOLUMNS> columnMap{-1};
            std::array<int32_t, MAXCOLUMNS> reverseMap{-1};
            unordered_map<int64_t, int32_t> insertMap;
        };

        // column maps are bulky, ugly, and fortunately, very sharable!
        class ColumnMapping
        {
            CriticalSection cs;
            ColumnMap_s* allMapping{ nullptr };
            std::unordered_map<int64_t, ColumnMap_s*> map;

        public:

            ColumnMapping() = default;
            ~ColumnMapping() = default;

            ColumnMap_s* mapSchema(Table* table, Attributes* attributes, const vector<string>& columnNames);
            ColumnMap_s* mapSchema(Table* table, Attributes* attributes);

            void releaseMap(ColumnMap_s* cm);
        };
    };
};