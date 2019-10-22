#pragma once

#include <array>
//#include <unordered_map>
#include "robin_hood.h"

#include "common.h"
#include "threads/locks.h"

/*
 * Map Objects are used to map Schema Column Indexes (which may not be sequential)
 * into sequential index based lookups.
 *
 * The PyQL compiler converts property references into 0 based indexes. If a Table has
 * 1000 properties, but a query uses 3 of those, the PyQL compiler will map only those three
 * properties.
 *
 * When row sets are expanded, only the properties that are referenced will be extracted,
 * this results in a tightly packed result set (with good affinity).
 *
 * To make all this possible these structures were made to translate from the referenced
 * index to the schema index and back.
 *
 * Note: These structures are bulky, so they are also shared, this makes sense as the same
 *       query is often running across multiple cores.
 */
namespace openset
{
    namespace db
    {
        class Table;
        class Attributes;

        struct PropertyMap_s
        {
            int64_t hash{0};
            int64_t rowBytes{0};
            int32_t refCount{0};
            int32_t propertyCount{0};
            int32_t uuidPropIndex{-1};
            int32_t sessionPropIndex{-1};
            std::array<int32_t, MAX_PROPERTIES> propertyMap{-1};
            std::array<int32_t, MAX_PROPERTIES> reverseMap{-1};
            robin_hood::unordered_map<int64_t, int32_t, robin_hood::hash<int64_t>> insertMap;
        };

        // property maps are bulky, ugly, and fortunately, very sharable!
        class PropertyMapping
        {
            CriticalSection cs;
            PropertyMap_s* allMapping{ nullptr };
            robin_hood::unordered_map<int64_t, PropertyMap_s*, robin_hood::hash<int64_t>> map;

        public:

            PropertyMapping() = default;
            ~PropertyMapping() = default;

            PropertyMap_s* mapSchema(Table* table, Attributes* attributes, const vector<string>& propertyNames);
            PropertyMap_s* mapSchema(Table* table, Attributes* attributes);

            void releaseMap(PropertyMap_s* cm);
        };
    };
};