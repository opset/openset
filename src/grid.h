#pragma once
#include <vector>
#include <limits>
#include <unordered_map>
#include <cstring>

#include "common.h"
#include "property_mapping.h"
#include "var/var.h"
#include "cjson/cjson.h"

#include "robin_hood.h"
#include "customer_props.h"
#include "../lib/sba/sba.h"

namespace openset
{
    namespace db
    {
        class Properties;
        class Table;
        class Attributes;
        class AttributeBlob;
        class PropertyMapping;
        class CustomerProps;
        class Grid;
        struct PropertyMap_s;
        const int64_t int16_min = numeric_limits<int16_t>::min();
        const int64_t int16_max = numeric_limits<int16_t>::max();
        const int64_t int32_min = numeric_limits<int32_t>::min();
        const int64_t int32_max = numeric_limits<int32_t>::max();

        class IndexDiffing
        {
            using ColVal = std::pair<int32_t, int64_t>;
            using CVMap = robin_hood::unordered_map<ColVal, int, robin_hood::hash<ColVal>>;
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

            void add(int32_t propIndex, int64_t value, Mode_e mode);
            void add(const Grid* grid, Mode_e mode);
            void add(const Grid* grid, const cvar& props, Mode_e mode);

            void iterAdded(const std::function<void(int32_t, int64_t)>& cb);
            void iterRemoved(const std::function<void(int32_t, int64_t)>& cb);
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
            int32_t bytes;       // bytes when uncompressed
            int32_t comp;        // bytes when compressed
            int16_t idBytes;     // number of bytes in id string
            char*   props;       // pointer to props - mutable and may change during a query, slow to repack into structure
            char    events[1];   // char* (1st byte) of packed event struct

            std::string getIdStr() const { return std::string(events, idBytes); }

            // personData_s must already be sized, null not required
            void setIdStr(std::string& idString)
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

            int64_t size() const { return (sizeof(PersonData_s) - 1LL) + comp + idBytes; }
            char* getIdPtr() { return events; }
            char* getComp() { return events + idBytes; }
        };

        const int64_t PERSON_DATA_SIZE = sizeof(PersonData_s) - 1LL;

        struct Col_s
        {
            int64_t cols[MAX_PROPERTIES];
        };

        using Row = Col_s;
        using Rows = vector<Row*>;

        struct SetInfo_s
        {
            int32_t length { 0 };
            int32_t offset { 0 };

            SetInfo_s(const int32_t length, const int32_t offset) :
                length(length),
                offset(offset) {}
        };
#pragma pack(pop)
        class Grid
        {
        private:
            using LineNodes = vector<cjson*>;
            using SetVector = vector<int64_t>;
#pragma pack(push,1)
            struct Cast_s
            {
                int16_t propIndex;
                int64_t val64;
            };
#pragma pack(pop)

            const static int sizeOfCastHeader = sizeof(Cast_s::propIndex);
            const static int sizeOfCast = sizeof(Cast_s);
            PropertyMap_s* propertyMap { nullptr };
            // we will get our memory via stack
            // so rows have tight cache affinity
            HeapStack mem;
            Rows rows;
            Row* emptyRow { nullptr };
            SetVector setData;

            PersonData_s* rawData { nullptr };

            int64_t sessionTime { 60'000LL * 30LL }; // 30 minutes

            Table* table { nullptr };
            Attributes* attributes { nullptr };
            AttributeBlob* blob { nullptr };
            bool hasInsert { false };

            CustomerProps customerProps;

            mutable IndexDiffing diff;

            public:
            Grid() = default;
            ~Grid();

            /**
            * Why? The schema can have up to 4096 properties. These properties have
            * numeric indexes that allow allocated properties to be distributed
            * throughout that range. The Column map is a sequential list of
            * indexes into the actual schema, allowing us to create compact
            * grids that do not contain 4096 properties (which would be bulky
            * and slow)
            */
            bool mapSchema(Table* tablePtr, Attributes* attributesPtr);
            bool mapSchema(Table* tablePtr, Attributes* attributesPtr, const vector<string>& propertyNames);
            void setSessionTime(const int64_t sessionTime) { this->sessionTime = sessionTime; }
            void mount(PersonData_s* personData);
            void prepare();
        private:
            enum class RowType_e : int
            {
                event,
                prop,
                event_and_prop,
                junk
            };

            RowType_e insertParse(Properties* properties, cjson* doc, Col_s* insertRow);
        public:
            void insertEvent(cjson* rowData);
            // re-encodes and compresses the row data after inserts
            PersonData_s* commit();

            // remove old records, or trim sets that have gotten to large.
            // returns true if culling occured - de-index unreferenced items
            bool cull();

            // given an actual schema property, what is the
            // property in the grid (which is compact)
            int getGridProperty(int propIndex) const;

            string getUUIDString() const
            {
                return rawData ? string { rawData->events, static_cast<size_t>(rawData->idBytes) } : "";
            }

            int64_t getUUID() const { return rawData->id; }
            int64_t getLinId() const { return rawData->linId; }

            bool isFullSchema() const;

            Table* getTable() const { return table; }

            const Rows* getRows() const
            {
                return &rows;
            }

            const Row* getEmptyRow() const
            {
                return emptyRow;
            }

            const SetVector& getSetData() const { return setData; }
            Attributes* getAttributes() const { return attributes; }
            PersonData_s* getMeta() const { return rawData; }
            PropertyMap_s* getPropertyMap() const { return propertyMap; }
            AttributeBlob* getAttributeBlob() const;

            openset::db::CustomerProps * getCustomerPropsManager();

            openset::db::CustomerPropMap* getCustomerProps();
            void setCustomerProps();

            cjson toJSON(); // brings object back to zero state
            void reinitialize();
        private:
            Col_s* newRow();

            void reset();
        };
    };
};
