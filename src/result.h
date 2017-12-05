#pragma once

#include <vector>
#include <functional>

#include "common.h"
#include "cjson/cjson.h"
#include "mem/bigring.h"
#include "heapstack/heapstack.h"
#include "querycommon.h"
#include "table.h"
#include "errors.h"

namespace openset
{
	namespace result
	{
		const int keyDepth = 8;

        enum class ResultTypes_e : int
        {
            Int = 0,
            Double = 1,
            Bool = 2,
            Text = 3,
            None = 4
        };

        enum class ResultSortOrder_e : int
        {
            Asc,
            Desc
        };

        enum class ResultSortMode_e : int
        {
            key,
            column
        };

		struct RowKey
		{
			
			int64_t key[keyDepth];
            ResultTypes_e types[keyDepth];

			RowKey() 
			{}

			inline void clear()
			{
				key[0] = NONE;
				key[1] = NONE;
				key[2] = NONE;
				key[3] = NONE;
				key[4] = NONE;
				key[5] = NONE;
				key[6] = NONE;
				key[7] = NONE;
                types[0] = ResultTypes_e::Int;
                types[1] = ResultTypes_e::Int;
                types[2] = ResultTypes_e::Int;
                types[3] = ResultTypes_e::Int;
                types[4] = ResultTypes_e::Int;
                types[5] = ResultTypes_e::Int;
                types[6] = ResultTypes_e::Int;
                types[7] = ResultTypes_e::Int;
			}

			inline void clearFrom(const int index)
			{
				for (auto iter = key + index; iter < key + keyDepth; ++iter)
					*iter = NONE;
			}

			inline RowKey keyFrom(const int index) const
			{
				auto newKey{ *this };
				newKey.clearFrom(index);
				return newKey;
			}

			inline void keyFrom(const int index, RowKey& rowKey) const
			{
				rowKey = *this;
				rowKey.clearFrom(index);
			}

			int getDepth()
			{
				auto count = 0;
				for (auto iter = key; iter < key + keyDepth; ++iter, ++count)
					if (*iter == NONE)
						break;
				return count;
			}

			bool operator==(const RowKey &other) const
			{
				return (memcmp(key, other.key, sizeof(key)) == 0);
			}
			bool operator!=(const RowKey &other) const
			{
				return (memcmp(key, other.key, sizeof(key)) != 0);
			}

			bool operator<(const RowKey &other) const
			{
				for (auto i = 0; i < keyDepth; ++i)
				{
					if (key[i] > other.key[i])
						return false;
					if (key[i] < other.key[i])
						return true;
				}
				return false;
			}

			bool operator>(const RowKey &other) const
			{
				for (auto i = 0; i < keyDepth; ++i)
				{
					if (key[i] < other.key[i])
						return false;
					if (key[i] > other.key[i])
						return true;
				}
				return false;
			}

		};


	}
}

// back out of name space to put this in std
namespace std
{
	template<>
	struct hash<openset::result::RowKey>
	{
		size_t operator()(const openset::result::RowKey key) const
		{
			auto hash = key.key[0];
			auto count = 1;
			for (auto iter = key.key + 1; iter < key.key + openset::result::keyDepth; ++iter, ++count)
			{
				if (*iter == NONE)
					return hash;
				hash = (hash << count) + key.key[1];
			}
			return hash;
		}
	};
}


namespace openset
{
	namespace result
	{
		struct Accumulation_s
		{
			int64_t value;
			int32_t count;
		};

		const int ACCUMULATOR_DEPTH = 16;

		struct Accumulator
		{
			Accumulation_s columns[ACCUMULATOR_DEPTH];

			Accumulator()
			{
				clear();
			}

			void clear()
			{
				for (auto &i : columns)
				{
					//i.distinctId = NONE;
					i.value = NONE;
					i.count = 0;
				}
			}
		};


		class ResultSet
		{
		public:
			bigRing<RowKey, Accumulator*> results{ ringHint_e::lt_compact };
			using RowPair = pair<RowKey, Accumulator*>;
			using RowVector = vector<RowPair>;
			vector<RowPair> sortedResult;			
			HeapStack mem;

			CriticalSection cs;

			// premereged result sets are made when deserializing result sets
			// from internode queries... pointers are from some block of memory
			// so the `results` object will be empty, but the `sortedResult`
			// object will be populated
			bool isPremerged = false;

			bigRing<int64_t, char*> localText{ ringHint_e::lt_compact }; // text local to result set

            ResultTypes_e accTypes[ACCUMULATOR_DEPTH];
            query::Modifiers_e accModifiers[ACCUMULATOR_DEPTH];

			ResultSet();
			ResultSet(ResultSet&& other) noexcept;

			void makeSortedList();
			void setAtDepth(RowKey& key, const function<void(Accumulator*)> set_cb);

            void setAccTypesFromMacros(const openset::query::Macro_s macros);

			// this is a cache of text values local to our partition (thread), blob requires
			// a lock, whereas this does not, we will merge them after.
			void addLocalText(const int64_t hashId, cvar &value)
			{
				const auto textPair = localText.get(hashId);

				if (!textPair)
				{
					const auto textPtr = mem.newPtr(value.getString().length() + 1);
					strcpy(textPtr, value.getString().c_str());
					localText.set(hashId, textPtr);
				}
			}

			void addLocalText(const int64_t hashId, const std::string &value)
			{
				const auto textPair = localText.get(hashId);

				if (!textPair)
				{
					const auto textPtr = mem.newPtr(value.length() + 1);
					strcpy(textPtr, value.c_str());
					localText.set(hashId, textPtr);
				}
			}

			void addLocalText(const int64_t hashId, char* value, const int32_t length)
			{
				const auto textPair = localText.get(hashId);

				if (!textPair)
				{
					const auto textPtr = mem.newPtr(length + 1);
					memcpy(textPtr, value, length);
					textPtr[length] = 0;
					localText.set(hashId, textPtr);
				}
			}

		};


		struct CellQueryResult_s
		{
            int32_t instance{ 0 };
            std::unordered_map<string, int64_t> stats;
			openset::errors::Error error;
			
			CellQueryResult_s(
                const int64_t instanceId,
                std::unordered_map<string, int64_t> stats,
				const openset::errors::Error error) :
                    instance(instanceId),
                    stats(std::move(stats)),
					error(error)
			{}
			
			CellQueryResult_s(CellQueryResult_s&& other) noexcept
			{
				instance = other.instance;
				error = std::move(other.error);
                stats = std::move(other.stats);
			}

			~CellQueryResult_s()
			{}

			CellQueryResult_s& operator=(CellQueryResult_s&& other) noexcept
			{
				instance = other.instance;
				error = other.error;
                stats = other.stats;
				return *this;
			}
		};

		/*
		 *  MUX/DEMUX - Merge and generate mutiple result types.
		 *
		 *  This is a utility class with static members
		 */
		class ResultMuxDemux
		{
			// merge multiple result sets using a sync-sort technique
			// retuns a new result set which can be used to serialize to
			// JSON
        public:
            static void mergeMacroLiterals(
                const openset::query::Macro_s macros,
                std::vector<openset::result::ResultSet*>& resultSets);

            static char* multiSetToInternode(
                const int resultColumnCount,
                const int resultSetCount,
                std::vector<openset::result::ResultSet*>& resultSets,
                int64_t &bufferLength);

			static bool isInternode(char* data, int64_t blockLength);

			static openset::result::ResultSet* internodeToResultSet(
				char* data,
				int64_t blockLength);

            static void resultSetToJson(
                const int resultColumnCount,
                const int resultSetCount,
                std::vector<openset::result::ResultSet*>& resultSets,
                cjson* doc);

            static void jsonResultSortByColumn(cjson* doc, const ResultSortOrder_e sort, const int column);

            static void jsonResultTrim(cjson* doc, const int trim);
        };
	}
}
