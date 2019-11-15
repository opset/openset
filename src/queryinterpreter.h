#pragma once

#include <array>
#include <vector>
#include <unordered_map>

#include "xxhash.h"
#include "robin_hood.h"

#include "querycommon.h"
#include "result.h"
#include "errors.h"

using namespace openset::db;

namespace openset
{
    namespace db
    {
        class Customer;
        class Grid;
        class AttributeBlob;
        class IndexBits;
    }
}

namespace openset
{
    namespace query
    {
        struct EventDistinct_s // 128bit hash
        {
            int64_t hash1{0};
            int64_t hash2{0};

            EventDistinct_s() = default;

            void set(const int64_t value, const int64_t distinct, const int64_t countKey, const int64_t column)
            {
                hash1 = XXH64(&distinct, 8, column);
                hash2 = XXH64(&value, 8, countKey);
            }

            bool operator ==(const EventDistinct_s& right) const
            {
                return (hash1 == right.hash1 && hash2 == right.hash2);
            }
        };
    }
}

namespace std
{
    // hasher for EventDistinct_s
    template <>
    struct hash<openset::query::EventDistinct_s>
    {
        size_t operator()(const openset::query::EventDistinct_s& v) const
        {
            return XXH64(&v.hash1, 16, v.hash2);
        }
    };
}

namespace openset
{
    namespace query
    {
        enum class InterpretMode_e: int
        {
            query,
            job,
            count
        };

        enum class LoopState_e : int
        {
            run,
            in_break,
            in_continue,
            in_exit
        };

        //using Stack = cvar*;//vector<cvar>;

        using DebugLog = vector<cvar>;
        using BitMap = unordered_map<string, IndexBits*>;

        struct StackFrame_s
        {
            // data
            int64_t currentRow;
            int64_t iterCount;

            explicit StackFrame_s(
                //instruction_s* executionPtr,
                StackFrame_s* lastFrame):
                //execPtr(executionPtr),
                currentRow(0),
                iterCount(0)
            {
                if (lastFrame) // copy up from last stack frame
                {
                    //execPtr = lastFrame->execPtr;
                    currentRow = lastFrame->currentRow;
                    iterCount = lastFrame->iterCount;
                }
            }

            StackFrame_s(StackFrame_s&& other) noexcept
            {
                currentRow = other.currentRow;
                iterCount = other.iterCount;
            }
        };

        class Interpreter
        {
        public:

            using Range = std::vector<std::pair<int64_t, int64_t>>;

            Range filterRangeStack;

            // we compare hashes of strings, rather than strings
            const int64_t breakAllHash = MakeHash("all");
            const int64_t breakTopHash = MakeHash("top");

            // execution
            Macro_s& macros;
            cvar* stack;
            cvar* stackPtr;

            // data
            int64_t uuid{ 0 };
            int64_t linid{ 0 };

            // database objects
            Grid* grid{ nullptr };
            const Rows* rows{ nullptr };
            const Row* propRow{ nullptr };
            int rowCount{ 0 };

            AttributeBlob* blob{ nullptr };
            Attributes* attrs{ nullptr };
            result::ResultSet* result{ nullptr };
            result::RowKey rowKey;

            // indexing
            IndexBits* bits{ nullptr };
            int maxBitPop{ 0 }; // largest linear user_id in table/partition

            cvar props;
            int propsIndex{ -1 };
            bool propsChanged{ false };

            // counters
            int loopCount{ 0 };
            int recursion{ 0 };

            int nestDepth{ 0 }; // how many nested loops are we in
            int breakDepth{ 0 }; // how many nested loops do we want to break

            // property offsets and indexes used for queries with segments
            int segmentColumnShift{ 0 };
            std::vector<IndexBits*> segmentIndexes;

            // switches
            InterpretMode_e interpretMode{ InterpretMode_e::query };
            LoopState_e loopState{ LoopState_e::run }; // run, continue, break, exit
            bool isConfigured{ false };

            // debug - log entries are entered in order by calling debug
            DebugLog debugLog;
            errors::Error error;
            int32_t eventCount{ -1 }; // -1 is uninitialized, calculation cached here

            // callbacks to external code (i.e. triggers)
            function<IndexBits*(const string&, bool&)> getSegment_cb{ nullptr };

            // distinct counting structures
            using ValuesSeenKey = EventDistinct_s;//tuple<int32_t, int64_t, int64_t, int64_t>;
            using ValuesSeen = robin_hood::unordered_map<ValuesSeenKey, int32_t, robin_hood::hash<ValuesSeenKey>>;
            using MarshalParams = std::array<cvar, 24>;

            // object global non-heap container for marhal function parameters
            // regular function local vectors were impacting performance > 6%
            MarshalParams marshalParams = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};


            // distinct counting (with property as key)
            ValuesSeen eventDistinct; // distinct to group id
            ValuesSeenKey distinctKey;

            // used to load global variables into user variable space
            bool firstRun{ true };
            bool inReturn{ false };
            bool exportCustomerId { false };

            // this will always point to the last debug message
            Debug_s* lastDebug{ nullptr };

            string calledFunction;

            // return values when calling exec with a function
            // this is a vector because calling with segment will have
            // multiple return values
            using Returns = std::vector<cvar>;
            Returns returns;

            explicit Interpreter(
                Macro_s& macros,
                const InterpretMode_e interpretMode = InterpretMode_e::query);

            ~Interpreter();

            void setResultObject(result::ResultSet* resultSet);

            void configure();

            vector<string> getReferencedColumns() const;
            void mount(Customer* person);

            static constexpr bool within(
                const int64_t compStamp,
                const int64_t rowStamp,
                const int64_t milliseconds)
            {
                return (rowStamp >= compStamp - milliseconds &&
                    rowStamp <= compStamp + milliseconds);
            }

            SegmentList* getSegmentList() const;
            static int64_t convertStamp(const cvar& stamp);

            void extractMarshalParams(const int paramCount);

            void tally(const int paramCount, const Col_s* columns, const int currentRow);
            void autoTally();

            void marshal_tally(const int paramCount, const Col_s* columns, const int currentRow);
            void marshal_log(const int paramCount);
            void marshal_break(const int paramCount);
            void marshal_dt_within(const int paramCount, const int64_t rowStamp);
            void marshal_ISO8601_to_stamp(int paramCount, int64_t rowStamp);
            void marshal_dt_between(const int paramCount, const int64_t rowStamp);
            void marshal_bucket(const int paramCount);
            void marshal_round(const int paramCount);
            void marshal_fix(const int paramCount);
            void marshal_makeDict(const int paramCount);
            void marshal_makeList(const int paramCount);
            void marshal_makeSet(const int paramCount);
            void marshal_population(const int paramCount);
            void marshal_intersection(const int paramCount);
            void marshal_union(const int paramCount);
            void marshal_compliment(const int paramCount);
            void marshal_difference(const int paramCount);
            void marshal_slice(const int paramCount);
            void marshal_find(const int paramCount, const bool reverse = false);
            void marshal_split(const int paramCount) const;
            void marshal_strip(const int paramCount) const;
            void marshal_url_decode(const int paramCount) const;
            void marshal_get_row(const int paramCount) const;

            // get a string from the literals script block by ID
            string getLiteral(const int64_t id) const;

            bool marshal(Instruction_s* inst, int64_t& currentRow);
            cvar* lambda(int lambdaId, int currentRow);
            void opRunner(Instruction_s* inst, int64_t currentRow = 0);

            void setScheduleCB(const function<void (int64_t functionHash, int seconds)> &cb);
            void setEmitCB(const function<void (string emitMessage)> &cb);
            void setGetSegmentCB(const function<IndexBits*(const string&, bool&)>& cb);
            // where our result bits are going to end up
            void setBits(IndexBits* indexBits, int maxPopulation);
            void setCompareSegments(IndexBits* querySegment, std::vector<IndexBits*> segments);

            // reset class cvariables before running
            // check for firstrun, check for globals.
            void execReset();
            void exec();
            void exec(const string &functionName);
            void exec(const int64_t functionHash);

            void setGridProps();
            void getGridProps();

            // if you need the result of exec+function this will get it or return value NONE
            Returns& getLastReturn();
        };
    }
}
