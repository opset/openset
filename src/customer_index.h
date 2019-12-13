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

            void insert(const int64_t customerId, const int linId, const int64_t value)
            {
                index.set(SortKeyOneProp_s{ customerId, value}, linId);
            }

            void erase(int64_t customerId, int64_t value)
            {
                // delete from `index`
            }

            CustomerIndexList serialize(
                bool descending,
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

            CustomerIndexList getList(
                int propIndex,
                bool descending,
                int limit,
                const std::function<bool(SortKeyOneProp_s*, int*)>& filterCallback)
            {
                if (limit <= 0)
                    limit = 1;
                if (limit > 1000)
                    limit = 1000;
                if (const auto& iter = indexes.find(propIndex); iter != indexes.end())
                    return iter->second->serialize(descending, limit, filterCallback);
                return {};
            }
        };
    };
};