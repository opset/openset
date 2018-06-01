#pragma once

#include <vector>
#include <limits>
#include <unordered_map>
#include <cstring>

#include "common.h"
#include "cjson/cjson.h"
#include "columnmapping.h"

namespace openset
{
	namespace db
	{
		class Table;
		class Attributes;
		class AttributeBlob;
        class ColumnMapping;
        class Grid;
        struct ColumnMap_s;

		const int64_t int16_min = numeric_limits<int16_t>::min();
		const int64_t int16_max = numeric_limits<int16_t>::max();
		const int64_t int32_min = numeric_limits<int32_t>::min();
		const int64_t int32_max = numeric_limits<int32_t>::max();

#pragma pack(push,1)
		/**
		This is the actual user record, Person is a class that
		can be assigned (Mounted) to one of these records, to perform
		operations.
		*/

		enum class flagType_e : int16_t
		{
			feature_trigger = 1, // trigger
			future_trigger = 2, // scheduled trigger
		};

		struct Flags_s // 26 bytes.
		{
			int64_t reference; // what this flag refers to (i.e. a trigger ID)
			int64_t context; // value 1 (i.e. hash of function name)
			int64_t value; // value 2 (i.e. the future run-stamp of a trigger)
			flagType_e flagType;

			void set(
				const flagType_e flagType,
				const int64_t reference,
				const int64_t context,
				const int64_t value)
			{
				this->flagType = flagType;
				this->reference = reference;
				this->context = context;
				this->value = value;
			};

			// Note: flags are implemented using a Pool block and that pointer
			// is stored in a personData_s structure. The count is not part of
			// personData_s structure as instead the list is terminated with a
			// single 2 byte section filled with 0 (feature_eof). This allows us
			// to cast and check the buffer. 
		};
#pragma pack(pop)

        class IndexDiffing
        {
            using ColVal = std::pair<int32_t,int64_t>;
            using CVMap = unordered_map<ColVal, int>;
            using CVList = vector<ColVal>;

            CVMap before;
            CVMap after;

        public:

            enum class Mode_e : int
            {
                before,
                after
            };

            void reset();

            void add(int32_t column, int64_t value, Mode_e mode);
            void add(Grid* grid, Mode_e mode);

            CVList getRemoved();
            CVList getAdded();
            void iterRemoved(const std::function<void(int32_t,int64_t)> cb);
        };

#pragma pack(push,1)
		struct PersonData_s
		{
			/*
			*  A User Record is a packed structure with the following layout
			*
			*  personData_s
			*  ------------
			*  idBytes
			*  ------------
			*  flags_s records
			*  ------------
			*  compressed event rows
			*
			*/

			int64_t id;
			int32_t linId;
			int32_t bytes; // bytes when uncompressed
			int32_t comp; // bytes when compressed
			int16_t idBytes; // number of bytes in id string
			int16_t flagRecords; // number of flag records
			char events[1]; // char* (1st byte) of packed event struct            

			std::string getIdStr() const
			{
				return std::string(events, idBytes);
			}

			// personData_s must already be sized, null not required
			void setIdStr(std::string &idString)
			{
                const int16_t idMaxLen = (idString.length() > 64) ? 64 : static_cast<int16_t>(idString.length());
				std::memcpy(events, idString.c_str(), idMaxLen);
                idBytes = idMaxLen;
			}

			void setIdStr(char* idString, const int len)
		    {
                const int16_t idMaxLen = (len > 64) ? 64 : static_cast<int16_t>(len);
				std::memcpy(events, idString, idMaxLen);
				idBytes = idMaxLen;
			}

			int64_t flagBytes() const
			{
				return (flagRecords * sizeof(Flags_s));
			}

			int64_t size() const
			{
				return (sizeof(PersonData_s) - 1LL) + comp + idBytes + flagBytes();
			}

			Flags_s* getFlags() 
			{
				return reinterpret_cast<Flags_s*>(events + idBytes);
			}

			char* getIdPtr()
			{
				return events;
			}

			char* getComp() 
			{
				return events + idBytes + flagBytes();
			}
		};

        const int64_t PERSON_DATA_SIZE = sizeof(PersonData_s) - 1LL;

		struct Col_s
		{
			int64_t cols[MAXCOLUMNS];
		};

        using Row = Col_s;
		using Rows = vector<Row*>;

        struct SetInfo_s
        {
            int32_t length{0};
            int32_t offset{0};

            SetInfo_s(const int32_t length, const int32_t offset) :
                length(length),
                offset(offset)
            {}
        };

#pragma pack(pop)

		class Grid
		{
		private:
			using LineNodes = vector<cjson*>;
			using ExpandedRows = vector<LineNodes>;
            using SetVector = vector<int64_t>;

#pragma pack(push,1)
			struct Cast_s
			{
				int16_t columnNum; 
				int64_t val64;
			};
#pragma pack(pop)			

			const static int sizeOfCastHeader = sizeof(Cast_s::columnNum);
			const static int sizeOfCast = sizeof(Cast_s);

            ColumnMap_s* colMap{ nullptr };

			// we will get our memory via stack
			// so rows have tight cache affinity 
			HeapStack mem;
            Row* propRow{ nullptr }; // full unmapped row
			Rows rows;
            SetVector setData;
			PersonData_s* rawData{ nullptr };

			int64_t sessionTime{ 60'000LL * 30LL }; // 30 minutes

			Table* table{ nullptr };
			Attributes* attributes{ nullptr };
			AttributeBlob* blob{ nullptr };

            IndexDiffing diff;

		public:
			Grid() = default;
			~Grid();

			/**
			* Why? The schema can have up to 8192 columns. These columns have
			* numeric indexes that allow allocated columns to be distributed
			* throughout that range. The Column map is a sequential list of
			* indexes into the actual schema, allowing us to create compact
			* grids that do not contain 8192 columns (which would be bulky
			* and slow)
			*/
			bool mapSchema(Table* tablePtr, Attributes* attributesPtr);
			bool mapSchema(Table* tablePtr, Attributes* attributesPtr, const vector<string>& columnNames);

			void setSessionTime(const int64_t sessionTime)
			{
				this->sessionTime = sessionTime;
			}

			void mount(PersonData_s* personData);
			void prepare();
			void insert(cjson* rowData);

			PersonData_s* addFlag(const flagType_e flagType, const int64_t reference, const int64_t context, const int64_t value);
			PersonData_s* clearFlag(const flagType_e flagType, const  int64_t reference, const int64_t context);

			// re-encodes and compresses the row data after inserts
			PersonData_s* commit();

            // remove old records, or trim sets that have gotten to large.
            // returns true if culling occured - de-index unreferrened items
            bool cull(); 

            // remove old records, or trim sets that have gotten to large.
            // similar to `cull()` but quicker routine for use with queries,
            // doesn't de-index or write back.
            //void queryCull();

			// given an actual schema column, what is the 
			// column in the grid (which is compact)
		    int getGridColumn(const int schemaColumn) const;

			string getUUIDString() const
			{
				return rawData ? string{ rawData->events, static_cast<size_t>(rawData->idBytes) } : "";
			}

			int64_t getUUID() const
			{
				return rawData->id;
			}

			int64_t getLinId() const
			{
				return rawData->linId;
			}

		    bool isFullSchema() const;

            Table* getTable() const
            {
                return table;
            }

			const Rows* getRows() const
			{
				return &rows;
			}

            const Row* getPropRow() const
            {
                return propRow;
            }

            const SetVector& getSetData() const
			{
                return setData;
			}

			Attributes* getAttributes() const
			{
				return attributes;
			}

            PersonData_s* getMeta() const
			{
			    return rawData;
			}

            ColumnMap_s* getColumnMap() const
            {
                return colMap;
            }

			AttributeBlob* getAttributeBlob() const;

			cjson toJSON() const;

			// brings object back to zero state
			void reinit();

		private:
			Col_s* newRow();
			void reset();
		};
	};
};
