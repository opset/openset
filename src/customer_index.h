#pragma once

#include "common.h"
#include "mem/blhash.h"
#include "robin_hood.h"

namespace openset
{
    namespace db
    {
        struct SortKeyOneProp_s
        {
            int64_t customerId;
            int64_t value;

            SortKeyOneProp_s() = default;

            SortKeyOneProp_s(const int64_t customerId, const int64_t value) :
                customerId(customerId),
                value(value)
            {}
        };

        using CustomerIndexList = std::vector<std::pair<SortKeyOneProp_s,int>>;

        class CustomerPropIndex
        {
            BinaryListHash<SortKeyOneProp_s, int> index;

        public:
            CustomerPropIndex() = default;
            ~CustomerPropIndex() = default;

            void insert(int64_t customerId, int linId, int64_t value)
            {
                index.set(SortKeyOneProp_s{ customerId, value}, linId);
            }

            void erase(int64_t customerId, int64_t value)
            {
                // delete from `index`
            }

            CustomerIndexList serialize(
                int limit,
                const std::function<bool(SortKeyOneProp_s*, int*)>& filterCallback);
        };

        class CustomerIndexing
        {
            robin_hood::unordered_map<int, CustomerPropIndex*, robin_hood::hash<int>> indexes;

        public:
            CustomerIndexing() = default;
            ~CustomerIndexing()
            {
                for (auto& index : indexes)
                    delete index.second;
            }

            void createIndex(int propIndex)
            {
                if (!indexes.count(propIndex))
                    indexes.emplace(propIndex, new CustomerPropIndex());
            }

            void insert(int propIndex, int64_t customerId, int linId, int64_t value)
            {
                if (value == NONE)
                    return;

                if (const auto& iter = indexes.find(propIndex); iter != indexes.end())
                    iter->second->insert(customerId, linId, value);
            }

            void erase(int propIndex, int64_t customerId, int64_t value)
            {
                if (const auto& iter = indexes.find(propIndex); iter != indexes.end())
                    iter->second->erase(customerId, value);
            }

            CustomerIndexList getListAscending(
                int propIndex,
                int limit,
                const std::function<bool(SortKeyOneProp_s*, int*)>& filterCallback)
            {
                if (const auto& iter = indexes.find(propIndex); iter != indexes.end())
                    return iter->second->serialize(limit, filterCallback);
                return {};
            }
        };
    };
};