#pragma once

#include "querycommon.h"
#include "columns.h"
#include "errors.h"
#include "var/var.h"

using namespace openset::db;

namespace openset
{
	namespace query
	{

		class QueryParser
		{
		public:

			// structure for parse exception handling
			struct ParseFail_s
			{
				errors::errorClass_e eClass;
				errors::errorCode_e eCode;
				std::string message;
				Debug_s debug;

				ParseFail_s(
					errors::errorClass_e eClass,
					errors::errorCode_e eCode,
					string message) :
					eClass(eClass),
					eCode(eCode),
					message(std::string{ message })
				{}

				ParseFail_s(
					errors::errorClass_e eClass,
					errors::errorCode_e eCode,
					string message,
					Debug_s debug) :
					eClass(eClass),
					eCode(eCode),
					message(std::string{ message }),
					debug(debug)
				{}
			
				std::string getMessage() const
				{
					return message;
				}

				std::string getDetail() const
				{
					return debug.text.length() ? debug.toStrShort() : "";
				}

			};

			errors::Error error;
			
			using LineParts = vector<string>;

			enum class parseMode_e : int
			{
				query
			};

			mutable Debug_s lastDebug;

			struct FirstPass_s
			{
				string text;
				int indent;
				LineParts parts;
				Debug_s debug;
				int block; // block id -1 is undefined
				bool isFunction;
				bool isLambda;
				bool isConditional; // code is within condition block

				FirstPass_s():
					indent(0),
					block(-1),
					isFunction(false),
					isLambda(false),
					isConditional(false)
				{}

				FirstPass_s(LineParts parts, Debug_s debug, int indent):
					FirstPass_s()
				{
					this->parts = parts;
					this->debug = debug;
					this->indent = indent;
				}
					
				void clear()
				{
					text.clear();
					indent = 0;
					debug.text.clear();
					debug.number = 0;
					isFunction = false;
					isLambda = false;
					isConditional = false;
					block = -1;
				}

				bool hasBlock() const
				{
					return (block == -1) ? false : true;
				}
			};

			using FirstPass = vector<FirstPass_s>;

			struct BlockList_s
			{
				int blockId;
				FirstPass code;
				bool isFunction;

				BlockList_s(int blockId, FirstPass code) :
					blockId(blockId),
					code(code),
					isFunction(false)
				{}
			};

			using BlockList = vector<BlockList_s>;

			struct MiddleOp_s
			{
				opCode_e op;
				int64_t params;
				int64_t value;
				string valueString;
				string nameSpace;
				bool isString;
				Debug_s debug;
				int64_t lambda;
				string deferredStr; // value stored for final pass processing
				int64_t deferredInt; // dito

				// constructors for emplace_back
				MiddleOp_s():
					op(opCode_e::NOP),
					params(0),
					value(0),
					isString(false),
					lambda(-1),
					deferredInt(0)
				{}

				MiddleOp_s(opCode_e op, int64_t value) :
					op(op),
					params(0),
					value(value),
					isString(false),
					lambda(-1),
					deferredInt(0)
				{}

				MiddleOp_s(opCode_e op, int64_t value, Debug_s& debugCopy, int64_t lambda = -1) :
					op(op),
					params(0),
					value(value),
					isString(false),
					lambda(lambda),
					deferredInt(0)
				{
					debug.number = debugCopy.number;
					debug.text = debugCopy.text;
					debug.translation = debugCopy.translation;
				}

				MiddleOp_s(opCode_e op, string valueString, Debug_s& debugCopy, int64_t lambda = -1) :
					op(op),
					params(0),
					value(0),
					valueString(valueString),
					isString(true),
					lambda(lambda),
					deferredInt(0)
				{
					debug.number = debugCopy.number;
					debug.text = debugCopy.text;
					debug.translation = debugCopy.translation;
				}
			};

			using MiddleOpList = vector<MiddleOp_s>;

			struct MiddleBlock_s
			{
				int64_t blockId;
				int64_t refs; // block reference count
				MiddleOpList code;
				blockType_e type;
				string blockName; // function name in general
				MiddleBlock_s() :
					blockId(-1),
					refs(0),
					type(blockType_e::code)
				{}
			};

			using MiddleBlockList = vector<MiddleBlock_s>;	
			using VarMap = unordered_map<std::string, variable_s>;
			using LiteralsMap = unordered_map<std::string, int>;
			using HintList = std::vector<std::string>; // this will probably get fancier 
			using HintMap = unordered_map<std::string, LineParts>;

			// structure for variables
			struct middleVariables_s
			{
				VarMap userVars;
				VarMap tableVars;
				VarMap columnVars;
				VarMap groupVars;
				SortList sortOrder;
				SegmentList segmentNames;
				LiteralsMap literals;
			};

			middleVariables_s vars;

			int blockCounter;

			HintList hintNames;
			HintMap hintMap;			

			Columns* tableColumns;

			parseMode_e parseMode;

			ParamVars* templating;

			bool isSegment{ false };
			bool isSegmentMath;
			bool useSessions;

			bool useGlobals{ false };

			// segments will always last 15 seconds unless otherwise specified, 0 is forever
			int64_t segmentTTL{ -1 };
			int64_t segmentRefresh{ -1 };
			mutable int autoCounter{ 0 };
			bool segmentUseCached{ false };

			explicit QueryParser(parseMode_e parseMode = parseMode_e::query);
			~QueryParser();

			static bool isVar(VarMap& vars, string name)
			{
				return (vars.find(name) != vars.end());
			}

			static variable_s& getVar(VarMap& vars, string name)
			{
				return vars.find(name)->second;
			}

			// is the column in the table definition
			bool isTableColumn(string name) const
			{
				if (name.find("column.") == 0)
					name = name.substr(name.find('.') + 1);

				return (tableColumns->getColumn(name) != nullptr);
			}

			bool isTableVar(string name)
			{
				if (name.find("column.") == 0)
					name = name.substr(name.find('.') + 1);

				return (vars.tableVars.find(name) != vars.tableVars.end());
			}

			bool isColumnVar(string name)
			{
				return (vars.columnVars.find(name) != vars.columnVars.end());
			}

			bool isUserVar(string name)
			{
				return (vars.userVars.find(name) != vars.userVars.end());
			}

			bool isGroupVar(string name)
			{
				return (vars.groupVars.find(name) != vars.groupVars.end());
			}

			bool isNonuserVar(string name)
			{
				if (isTableColumn(name))
					return true;

				if (isColumnVar(name))
				{
					auto var = vars.columnVars.find(name);
					return (var->second.modifier != modifiers_e::var);
				}

				if (isGroupVar(name))
				{
					auto var = vars.groupVars.find(name);
					return (var->second.modifier != modifiers_e::var);
				}

				return false;
			}

			static std::string stripQuotes(string text)
			{
				if (text[0] == '"' || text[0] == '\'')
					text = text.substr(1, text.length() - 2);
				return text;
			}

			static bool isDigit(char value);
			static bool isNumeric(string value);
			static bool isFloat(string value);
			static bool isString(string value);
			static bool isTextual(string value);
			static bool isValue(string value);

			static bool checkBrackets(LineParts& conditions);
			static LineParts extractVariable(LineParts& conditions, int startIdx, int& reinsertIdx);
			static LineParts extractVariableReverse(LineParts& conditions, int startIdx, int& reinsertIdx);
			static void extractFunction(LineParts& conditions, int startIdx, int& endIdx);

			// extract until , ) or end of line
			static void extractParam(LineParts& conditions, int startIdx, int& endIdx);

			static int64_t expandTime(string value);

			static LineParts breakLine(const string &text);

			FirstPass mergeLines(FirstPass& lines) const;
			FirstPass extractLines(const char* query) const;

			void lineTranslation(FirstPass& lines) const;

			// converts line list into blocks by indent level, sets
			// references for sub-block in parent block
			int64_t extractBlocks(int indent, FirstPass& lines, BlockList& blockList);
			static BlockList_s* getBlockByID(int64_t blockId, BlockList& blockList);

			int64_t parseHintConditions(LineParts& conditions, HintOpList& opList, int64_t index, bool stopOnConditions);
			void evaluateHints(std::string hintName, HintOpList& hintOps);

			// convert conditions, maths and function calls into stack
			int64_t parseConditions(LineParts& conditions, MiddleOpList& opList, int64_t index, Debug_s& debug, bool stopOnConditions, string stackOp = "");
			int64_t parseCall(LineParts& conditions, MiddleOpList& opList, int64_t index, Debug_s& debug);

			// parse dictionaries - they have an evil format
			// int64_t parseDictionary(LineParts& conditions, MiddleOpList& opList, int64_t index, Debug_s& debug);

			// convert a code block into tokens
			void tokenizeBlock(FirstPass& lines, int blockId, BlockList &blockList, MiddleBlockList& outputBlocks);

			void build(
				Columns* columnsPtr,
				MiddleBlockList& input,
				InstructionList& finCode,
				variables_s& finVars);

		public:

			// compile a query into a macro_s block
			bool compileQuery(const char* query, Columns* columnsPtr, macro_s& macros, ParamVars* templateVars = nullptr);

			static std::vector<std::pair<std::string, std::string>> extractCountQueries(const char* query);
		};

		string MacroDbg(macro_s& macro);
	};
};
