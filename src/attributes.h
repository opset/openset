#pragma once

#include <vector>
#include <unordered_set>
#include "mem/bigring.h"
#include "heapstack/heapstack.h"

#include "dbtypes.h"
#include "attributeblob.h"
#include "columns.h"
#include "indexbits.h"

using namespace std;

namespace openset
{
	namespace db
	{

		const int32_t COL_STAMP = 0;
		const int32_t COL_ACTION = 1; // below are fake columns used for indexing
		const int32_t COL_UUID = 2; // below are fake columns used for indexing
		const int32_t COL_TRIGGERS = 3; 
		const int32_t COL_EMIT = 4;
		const int32_t COL_SEGMENT = 5;

		const int32_t COL_OMIT_FIRST = COL_UUID; // omit >=
		const int32_t COL_OMIT_LAST = COL_SEGMENT; // omit <=

		struct BitData_s;
		class Columns;

#pragma pack(push,1)

		struct attr_changes_s
		{
			int32_t linId; // linear ID of Person
			int32_t state; // 1 or 0
			attr_changes_s* prev; // tail linked.
			attr_changes_s() :
				linId(0), state(0), prev(nullptr)
			{}

			attr_changes_s(int32_t linId, int32_t state, attr_changes_s* prev) :
				linId(linId), state(state), prev(prev)
			{}
		};

		union attr_value_u
		{
			int64_t numeric;
			char* blob; // shared location in attributes blob
		};

		/*
		attr_s defines an index item. It is cast over a variable length
		chunk of memory. "people" is an array of bytes containing
		a compressed bit index
		*/
		struct attr_s
		{
			attr_changes_s* changeTail;
			char* text;
			int32_t ints; // number of unsigned int64 integers uncompressed data uses
			int32_t comp; // compressed size in bytes
			char index[1]; // char* (1st byte) of packed index bits struct

			attr_s();
			void addChange(int32_t LinearId, bool State);
			IndexBits* getBits();
		};

#pragma pack(pop)


		class Attributes
		{
#pragma pack(push,1)
			struct serializedAttr_s
			{
				int32_t column;
				int64_t hashValue;
				int32_t ints; // number of int64_t's used when decompressed
				int32_t textSize;
				int32_t compSize;
			};
#pragma pack(pop)

		public:

			enum class listMode_e : int32_t
			{
				EQ,
				NEQ,
				GT,
				GTE,
				LT,
				LTE,
				PRESENT
			};

			using AttrList = vector<attr_s*>;

			// value and attribute info
			using ColumnIndex = bigRing<int64_t, attr_s*>;
			using AttrPair = pair<int64_t, attr_s*>;

			unordered_set<attr_key_s> dirty;
			unordered_map<int32_t, ColumnIndex*> columnIndex;

			AttributeBlob* blob;
			Columns* columns;
			int partition;

			explicit Attributes(int parition, AttributeBlob* attributeBlob, Columns* columns);
			~Attributes();

			ColumnIndex* getColumnIndex(int32_t column);

			attr_s* getMake(int32_t column, int64_t value);
			attr_s* getMake(int32_t column, string value);

			attr_s* get(int32_t column, int64_t value);
			attr_s* get(int32_t column, string value);

			void setDirty(int32_t linId, int32_t column, int64_t value, attr_s* attrInfo);
			void clearDirty();

			// replace an indexes bits with new ones, used when generating segments
			void swap(int32_t column, int64_t value, IndexBits* newBits);

			AttributeBlob* getBlob() const;

			AttrList getColumnValues(int32_t column, listMode_e mode, int64_t value);

			bool operator==(const Attributes& other) const
			{
				return (partition == other.partition);
			}

			void serialize(HeapStack* mem);
			int64_t deserialize(char* mem);
		};
	};
};

namespace std
{
	template <>
	struct hash<openset::db::Attributes>
	{
		size_t operator()(const openset::db::Attributes& x) const
		{
			return x.partition;
		}
	};
};