#pragma once

#include <array>
#include <unordered_map>

#include "common.h"
#include "threads/locks.h"

/*
 * Map Objects are used to map Schema Column Indexes (which may not be sequential)
 * into sequential index based lookups.
 * 
 * The PyQL compiler converst column references into 0 based indexes. If a Table has
 * 1000 columns, but a query uses 3 of those, the PyQL compiler will enumerate those three
 * columns.
 * 
 * When rowsets are expanded, only the columns that are referenced will be extrated,
 * this results in a tightly packed (high cache affinity) result set.
 * 
 * To make all this possible these structures were made to translate from the referenced
 * index to the schema index and back.
 * 
 * Note: These structures are bulky, so they are also shared, this makes sense as the same
 *       query is often running accross multiple cores.
 */
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
            int32_t uuidColumn{-1};
            int32_t sessionColumn{-1};
            std::array<int32_t, MAX_COLUMNS> columnMap{-1};
            std::array<int32_t, MAX_COLUMNS> reverseMap{-1};
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