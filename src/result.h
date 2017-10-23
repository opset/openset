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

		struct RowKey
		{
			
			int64_t key[keyDepth];

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
			}

			inline void clearFrom(int index)
			{
				for (auto iter = key + index; iter < key + keyDepth; ++iter)
					*iter = NONE;
			}

			inline RowKey keyFrom(int index) const
			{
				auto newKey{ *this };
				newKey.clearFrom(index);
				return newKey;
			}

			inline void keyFrom(int index, RowKey& rowKey) const
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

// yucky - back out of name space to put this in std
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
		struct accumulation_s
		{
			int64_t value;
			int32_t count;
		};

		const int ACCUMULATOR_DEPTH = 16;

		struct Accumulator
		{
			accumulation_s columns[ACCUMULATOR_DEPTH];

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
			bigRing<RowKey, Accumulator*> results;
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

			bigRing<int64_t, char*> localText; // text local to result set

			ResultSet();
			ResultSet(ResultSet&& other) noexcept;

			void makeSortedList();
			void setAtDepth(RowKey& key, function<void(Accumulator*)> set_cb);

			// this is a cache of text values local to our partition (thread), blob requires
			// a lock, whereas this does not, we will merge them after.
			void addLocalText(int64_t hashId, cvar &value)
			{
				const auto textPair = localText.get(hashId);

				if (!textPair)
				{
					const auto textPtr = mem.newPtr(value.getString().length() + 1);
					strcpy(textPtr, value.getString().c_str());
					localText.set(hashId, textPtr);
				}
			}

			void addLocalText(int64_t hashId, const std::string &value)
			{
				const auto textPair = localText.get(hashId);

				if (!textPair)
				{
					const auto textPtr = mem.newPtr(value.length() + 1);
					strcpy(textPtr, value.c_str());
					localText.set(hashId, textPtr);
				}
			}

			void addLocalText(int64_t hashId, char* value, int32_t length)
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
			//OpenSet::result::ResultSet* result;
			int32_t time;
			int32_t iterations;
			int64_t population;
			int64_t totalPopulation;
			int32_t instance;			
			openset::db::TablePartitioned* parts;
			openset::errors::Error error;
			
			CellQueryResult_s(): 
				//result(nullptr), 
				time(0), 
				iterations(0),
				population(0),
				totalPopulation(0),
				instance(0), 
				parts(nullptr)				
			{}

			CellQueryResult_s(
				//OpenSet::result::ResultSet* partitionResult,
				int64_t executionTime,
				int64_t runCount,
				int64_t population,
				int64_t totalPopulation,
				int64_t instanceId,
				openset::errors::Error error,
				openset::db::TablePartitioned* partitionedObjects) :
					//result(partitionResult),
					time(executionTime),
					iterations(runCount),
					population(population),
					totalPopulation(totalPopulation),
					instance(instanceId),
					parts(partitionedObjects),
					error(error)
			{}
			
			CellQueryResult_s(CellQueryResult_s&& other) noexcept
			{
				iterations = other.iterations;
				population = other.population;
				totalPopulation = other.totalPopulation;
				instance = other.instance;
				error = other.error;

				parts = other.parts;
				other.parts = nullptr;

				//result = other.result;
				//other.result = nullptr;

				time = other.time;
			}

			~CellQueryResult_s()
			{
				//if (result)
					//delete result;
			}

			CellQueryResult_s& operator=(CellQueryResult_s&& other) noexcept
			{
				time = other.time;
				iterations = other.iterations;
				population = other.population;
				totalPopulation = other.totalPopulation;
				instance = other.instance;
				error = other.error;

				parts = other.parts;
				other.parts = nullptr;

				//result = other.result;
				//other.result = nullptr;

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
		public:
			// merge multiple result sets using a sync-sort technique
			// retuns a new result set which can be used to serialize to
			// JSON
			static bigRing<int64_t, const char*> mergeText(
				const openset::query::Macro_S macros,
				openset::db::Table* table,
				std::vector<openset::result::ResultSet*> resultSets);

			static ResultSet::RowVector mergeResultSets(
				const openset::query::Macro_S macros,
				openset::db::Table* table,
				std::vector<openset::result::ResultSet*> resultSets);

			// generate a result set from JSON
			static char* resultSetToInternode(
				const openset::query::Macro_S macros,
				openset::db::Table* table,
				ResultSet::RowVector& rows,
				bigRing<int64_t, const char*>& mergedText,
				int64_t &bufferLength);

			static bool isInternode(char* data, int64_t blockLength);

			static openset::result::ResultSet* internodeToResultSet(
				char* data,
				int64_t blockLength);

			static void resultSetToJSON(
				const openset::query::Macro_S macros,
				openset::db::Table* table,
				cjson* doc,
				ResultSet::RowVector& rows,
				bigRing<int64_t, const char*>& mergedText);
		};
	}
}
