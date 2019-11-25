#pragma once

#include "common.h"
#include <vector>
//#include "mem/bigring.h"
#include "mem/blhash.h"
#include "heapstack/heapstack.h"

#include "robin_hood.h"
#include "dbtypes.h"
#include "indexbits.h"
#include "customer_index.h"

using namespace std;

namespace openset::db
{

    const int32_t PROP_STAMP = 0;
    const int32_t PROP_EVENT = 1;
    const int32_t PROP_UUID = 2;
    // below are fake properties used for indexing
    const int32_t PROP_SEGMENT = 5;
    const int32_t PROP_SESSION = 6;

    // user defined table properties start at this index
    const int32_t PROP_INDEX_USER_DATA = 7;

    // don't encode properties between these ranges
    const int32_t PROP_INDEX_OMIT_FIRST = PROP_UUID; // omit >=
    const int32_t PROP_INDEX_OMIT_LAST = PROP_SESSION; // omit <=

    struct BitData_s;
    class Properties;
    class Table;
    class AttributeBlob;

#pragma pack(push,1)

    struct Attr_changes_s
    {
        int32_t linId{ 0 }; // linear ID of Customer
        int32_t state{ 0 }; // 1 or 0

        Attr_changes_s() = default;

        Attr_changes_s(const int32_t linId, const int32_t state) :
            linId(linId), state(state)
        {}
    };

    union Attr_value_u
    {
        int64_t numeric;
        char* blob; // shared location in attributes blob
    };

    class Attributes;

    /*
    attr_s defines an index item. It is cast over a variable length
    chunk of memory. "people" is an array of bytes containing
    a compressed bit index
    */
    struct Attr_s
    {
        /*
         * The Attr_s is an index structure.
         *
         * Standard layout:
         *   ints - the number of uint64_t in the bit index array decompressed
         *   comp - how much space they take compressed (in bytes)
         *
         * Sparse Layout:
         *   ints - negative number, abs value is number of int32_ts in list
         *   comp - size of array in bytes (4 x length)
         *
         * Note: some indexes are really sparce, and while they compress well
         *   if there are thousands or millions of bits, compressing just one or
         *   two bits can be a waste of processor cycles and space as the result
         *   is still larger than it need be. In situtations where the population
         *   of the index is low, we will use an array of int32_t values, where
         *   each int32_t is a linear_id (linear user id).
         *
         */
        char* text{ nullptr };
        char* data{ nullptr };

        Attr_s() = default;
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
            int32_t ofs;
            int32_t len;
            int32_t textSize;
            int32_t compSize;
            int32_t linId;
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
            PRESENT,
        };

        using AttrListExpanded = vector<std::pair<int64_t,Attr_s*>>; // pair, value and bits
        using AttrList = vector<attr_key_s>;

        // value and attribute info
        using ColumnIndex = robin_hood::unordered_map<attr_key_s, Attr_s*, robin_hood::hash<attr_key_s>>;
        using ChangeIndex = robin_hood::unordered_map<attr_key_s, std::vector<Attr_changes_s>, robin_hood::hash<attr_key_s>>;
        using AttrPair = pair<attr_key_s, Attr_s*>;

        ColumnIndex propertyIndex; // prop/value store
        ChangeIndex changeIndex; // cache for property changes
        CustomerIndexing customerIndexing; // indexes for customer_list sort ordering
        IndexLRU indexCache;

        Table* table;
        AttributeBlob* blob;
        Properties* properties;
        int partition;

        explicit Attributes(const int partition, Table* table, AttributeBlob* attributeBlob, Properties* properties);
        ~Attributes();

        IndexBits* getBits(const int32_t propIndex, const int64_t value);

        void addChange(const int64_t customerId, const int32_t propIndex, const int64_t value, const int32_t linearId, const bool state);

        Attr_s* getMake(const int32_t propIndex, const int64_t value);
        Attr_s* getMake(const int32_t propIndex, const string& value);

        Attr_s* get(const int32_t propIndex, const int64_t value) const;
        Attr_s* get(const int32_t propIndex, const string& value) const;

        void drop(const int32_t propIndex, const int64_t value);

        void setDirty(const int64_t customerId, const int32_t linId, const int32_t propIndex, const int64_t value, const bool on);
        void clearDirty();

        // replace an indexes bits with new ones, used when generating segments
        //void swap(const int32_t propIndex, const int64_t value, IndexBits* newBits);

        AttributeBlob* getBlob() const;

        AttrListExpanded getPropertyValues(const int32_t propIndex);
        AttrList getPropertyValues(const int32_t propIndex, const listMode_e mode, const int64_t value);

        bool operator==(const Attributes& other) const
        {
            return (partition == other.partition);
        }

        void createCustomerPropIndexes();

        void serialize(HeapStack* mem);
        int64_t deserialize(char* mem);
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