#pragma once
#include "querycommon.h"
#include "person.h"
#include "grid.h"
#include "result.h"
#include "errors.h"

using namespace openset::db;

namespace openset
{
	namespace db
	{
		class AttributeBlob;
		class IndexBits;
	}
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

			// we compare hashes of strings, rather than strings
			const int64_t breakAllHash = MakeHash("all");
			const int64_t breakTopHash = MakeHash("top");

			// execution
			Macro_S& macros;
			cvar* stack;
			cvar* stackPtr;

			// data
			int64_t uuid{ 0 };
			int64_t linid{ 0 };

			// database objects
			const Grid* grid{ nullptr };
			const Rows* rows{ nullptr };
			int rowCount{ 0 };

			AttributeBlob* blob{ nullptr };
			result::ResultSet* result{ nullptr };
			result::RowKey rowKey;

			// indexing
			IndexBits* bits{ nullptr };
			int maxBitPop{ 0 }; // largest linear user_id in table/partition

			// counters
			int loopCount{ 0 };
			int recursion{ 0 };

			int nestDepth{ 0 }; // how many nested loops are we in
			int breakDepth{ 0 }; // how many nested loops do we want to break

			// column offsets and indexes used for queries with segments
			int segmentColumnShift{ 0 };
			std::vector<IndexBits*> segmentIndexes;

			// row match values
			int64_t matchStampTop{ 0 };
			vector<int64_t> matchStampPrev{ 0 };

			// switches
			InterpretMode_e interpretMode{ InterpretMode_e::query };
			LoopState_e loopState{ LoopState_e::run }; // run, continue, break, exit
			bool isConfigured{ false };
			bool jobState{ false };

			// debug - log entries are entered in order by calling debug
			DebugLog debugLog;
			errors::Error error;
			int32_t eventCount{ -1 }; // -1 is uninitialized, calculation cached here

			// callbacks to external code (i.e. triggers)
			function<bool(int64_t functionHash, int seconds)> schedule_cb{ nullptr };
			function<bool(string emitMessage)> emit_cb{ nullptr };
			function<IndexBits*(string, bool&)> getSegment_cb{ nullptr };

			// distinct counting structures
			using ValuesSeenKey = tuple<int32_t, int64_t, int64_t, int64_t>;
			using ValuesSeen = bigRing<ValuesSeenKey, int32_t>;

			// distinct counting (with column as key)
			ValuesSeen eventDistinct{ ringHint_e::lt_compact }; // distinct to group id
			ValuesSeenKey distinctKey;

			// used to load global variables into user variable space
			bool firstRun{ true };

			// this will always point to the last debug message
			Debug_s* lastDebug{ nullptr };

			explicit Interpreter(
				Macro_S& macros,
				const InterpretMode_e interpretMode = InterpretMode_e::query);

			~Interpreter();

			void setResultObject(result::ResultSet* resultSet);

			void configure();

			vector<string> getReferencedColumns() const;
			void mount(Person* person);

			static constexpr bool within(
				const int64_t compStamp, 
				const int64_t rowStamp, 
				const int64_t milliseconds)
			{
				return (rowStamp >= compStamp - milliseconds &&
					rowStamp <= compStamp + milliseconds);
			}

			SegmentList* getSegmentList() const;

			void marshal_tally(const int paramCount, const Col_s* columns, const int currentRow);

			void marshal_schedule(const int paramCount);
			void marshal_emit(const int paramCount);
			void marshal_log(const int paramCount);
			void marshal_break(const int paramCount);
			void marshal_dt_within(const int paramCount, const int64_t rowStamp);
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

			void marshal_url_decode(const int paramCount);

			// get a string from the literals script block by ID
			string getLiteral(const int64_t id) const;

			bool marshal(Instruction_s* inst, int& currentRow);
			void opRunner(Instruction_s* inst, int currentRow = 0);

			void setScheduleCB(function<bool(int64_t functionHash, int seconds)> cb);
			void setEmitCB(function<bool(string emitMessage)> cb);
			void setGetSegmentCB(function<IndexBits*(string, bool& deleteAfterUsing)> cb);
			// where our result bits are going to end up
			void setBits(IndexBits* indexBits, int maxPopulation);
			void setCompareSegments(IndexBits* querySegment, std::vector<IndexBits*> segments);

			// reset class cvariables before running
			// check for firstrun, check for globals.
			void execReset();
			void exec();
			void exec(string functionName);
			void exec(int64_t functionHash);
		};
	}
}
