#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "../lib/str/strtools.h"

#include "queryparserosl.h"
#include "querycommon.h"
#include "columns.h"
#include "errors.h"
#include "var/var.h"
#include <queue>

namespace openset::query
{
    class Blocks
    {
    public:
        int blockCount {0}; // block zero is the main code block

        using Line = std::vector<std::string>;

        struct LineItem_s
        {
            Line words;
            int  codeBlock {-1};
            int  conditionBlock {-1};

            LineItem_s(Line line) :
                words(std::move(line))
            {}
        };

        struct Block_s
        {
            int blockId;
            std::vector<LineItem_s> lines;

            Block_s(const int blockId):
                blockId(blockId)
            {};
        };

        std::unordered_map<int, Block_s> blocks;

        Block_s* getBlock(const int blockId)
        {
            if (const auto block = blocks.find(blockId); block != blocks.end())
                return &block->second;
            return nullptr;
        };

        Block_s* newBlock()
        {
            auto iter = blocks.emplace(blockCount, blockCount);
            ++blockCount;
            return &iter.first->second;
        }
    };

    struct Debugger_s
    {
        Blocks::Line line;
        std::string debugLine;
        std::string cursor;

        Debugger_s() = default;

        void set(Blocks::Line words, const int index = -1)
        {
            line = std::move(words);
            consolidate(line, index);
        }

    private:

        static std::string pad(const int width)
        {
            std::string result = "";
            while (static_cast<int>(result.size()) < width)
                result += " ";
            return result;
        }

        void consolidate(const Blocks::Line& line, const int index = -1)
        {
            debugLine = "";
            int count = 0;
            for (const auto &word: line)
            {
                if (count == index)
                {
                    cursor = pad(debugLine.length()) + "^";
                }
                debugLine += word + " ";
                ++count;
            }
        }
    };

    // structure for parse exception handling
    class QueryParse2Error_s : public std::exception
    {
    public:
        errors::errorClass_e eClass;
        errors::errorCode_e eCode;
        std::string message;
        Debugger_s debug;

        QueryParse2Error_s(
            const errors::errorClass_e eClass,
            const errors::errorCode_e eCode,
            const string& message)
            : eClass(eClass),
              eCode(eCode),
              message(message)
        {}

        QueryParse2Error_s(
            const errors::errorClass_e eClass,
            const errors::errorCode_e eCode,
            const string& message,
            const Debugger_s debug)
            : eClass(eClass),
              eCode(eCode),
              message(message),
              debug(debug)
        {}

        std::string getMessage() const
        {
            return message;
        }

        std::string getDetail() const
        {
            return debug.debugLine;
        }
    };

    enum class MiddleOp_e
    {
        push_user,
        push_literal,
        push_int,
        push_double,
        push_bool,
        push_column,
        push_user_ref,
        push_user_obj,
        push_user_obj_ref,
        push_true,
        push_false,
        push_nil,
        pop_user_var,
        pop_user_obj,
        pop_user_ref,
        pop_user_obj_ref,
        eq,
        neq,
        gt,
        gte,
        lt,
        lte,
        in,
        contains,
        any,
        op_and,
        op_or,
        add,
        sub,
        mul,
        div,
        block,
        ret,
        marshal,
        logic_filter,
        column_filter,
        if_call,
        each_call,
        for_call,
        sum_call,
        avg_call,
        max_call,
        min_call,
        count_call,
        dcount_call,
        test_call,
        row_call,
        term,
    };

    struct MiddleOp_s
    {
        MiddleOp_e op;
        cvar value1 {LLONG_MIN};
        cvar value2 {LLONG_MIN};
        int filterIndex {-1};
        Debugger_s debug;
        int index {-1};

        explicit MiddleOp_s(const MiddleOp_e op) :
            op(op)
        {}

        MiddleOp_s(const MiddleOp_e op, cvar value) :
            op(op),
            value1(std::move(value))
        {}

        MiddleOp_s(const MiddleOp_e op, cvar value, cvar value2) :
            op(op),
            value1(std::move(value)),
            value2(std::move(value2))
        {}

        MiddleOp_s(const MiddleOp_e op, const Blocks::Line& line, const int index) :
            op(op),
            index(index)
        {
            debug.set(line, index);
        }

        MiddleOp_s(const MiddleOp_e op, cvar value, const Blocks::Line& line, const int index) :
            op(op),
            value1(std::move(value)),
            index(index)
        {
            debug.set(line, index);
        }

        MiddleOp_s(const MiddleOp_e op, cvar value, cvar value2, const Blocks::Line& line, const int index) :
            op(op),
            value1(std::move(value)),
            value2(std::move(value2)),
            index(index)
        {
            debug.set(line, index);
        }

    };

    static const unordered_map<string, MiddleOp_e> ConditionToMiddleOp = {
        { "==", MiddleOp_e::eq },
        { "!=", MiddleOp_e::neq },
        { ">", MiddleOp_e::gt },
        { ">=", MiddleOp_e::gte },
        { "<", MiddleOp_e::lt },
        { "<=", MiddleOp_e::lte },
        { "<=", MiddleOp_e::lte },
        { "in", MiddleOp_e::in },
        { "contains", MiddleOp_e::contains },
        { "any", MiddleOp_e::any },
        { "&&", MiddleOp_e::op_and },
        { "||", MiddleOp_e::op_or },
        { "+", MiddleOp_e::add },
        { "-", MiddleOp_e::sub },
        { "*", MiddleOp_e::mul },
        { "/", MiddleOp_e::div },
    };

    static const unordered_map<string, MiddleOp_e> inlineIterators = {
        {"sum", MiddleOp_e::sum_call },
        {"avg", MiddleOp_e::avg_call },
        {"max", MiddleOp_e::max_call },
        {"min", MiddleOp_e::min_call },
        {"count", MiddleOp_e::count_call },
        {"dcount", MiddleOp_e::dcount_call },
        {"test", MiddleOp_e::test_call },
        {"row", MiddleOp_e::row_call }
    };

    class QueryParser
    {
    public:

        struct SectionDefinition_s
        {
            string sectionType;
            string sectionName;
            cvar flags { cvar::valueType::DICT };
            cvar params { cvar::valueType::DICT };
            string code;
        };

        using SectionDefinitionList = std::vector<SectionDefinition_s>;

        using MidOps = std::vector<MiddleOp_s>;
        using Tracking = std::vector<std::string>;

        MidOps middle;
        FilterList filters;

        db::Columns* tableColumns { nullptr };
        bool usesSessions { false };
        std::string rawScript;

        Blocks blocks;

        Blocks::Line indexLogic;

        Tracking userVars;
        Tracking stringLiterals;
        Tracking columns;
        Tracking selects;
        std::vector<Variable_s> selectColumnInfo;

        std::unordered_map<std::string, int> userVarAssignments;
        std::vector<std::string> currentBlockType;

        Debugger_s lastDebug;
        errors::Error error;

        QueryParser() = default;
        ~QueryParser() = default;

        static int getTrackingIndex(const Tracking& tracking, const std::string& item)
        {
            auto idx = 0;
            for (const auto& entry: tracking)
            {
               if (entry == item)
                   return idx;
               ++idx;
            }
            return -1;
        }

        static bool isDigit(const char value)
        {
            return (value >= '0' && value <= '9');
        }

        static bool isNumeric(const string& value)
        {
            return ((value[0] >= '0' && value[0] <= '9') || (value[0] == '-' && value[1] >= '0' && value[1] <= '9'));
        }

        static bool isTextual(const std::string& value)
        {
            return ((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z') || value[0] == '_');
        }

        static bool isFloat(const string& value)
        {
            return (((value[0] >= '0' && value[0] <= '9') ||
                (value[0] == '-' && value[1] >= '0' && value[1] <= '9')) &&
                value.find('.') != string::npos);
        }

        static bool isString(const string& value)
        {
            return (value[0] == '"' || value[0] == '\'');
        }

        static bool isNil(const string& value)
        {
            return (value == "nil" || value == "Nil" || value == "null");
        }

        static bool isBool(const string& value)
        {
            return (value == "True" || value == "true" || value == "False" || value == "false");
        }

        static bool isValue(const string& value)
        {
            return (isString(value) || isNumeric(value));
        }

        static bool isNameOrNumber(const std::string& value)
        {
            return isString(value) || isNumeric(value) || isTextual(value);
        }

        static cvar expandTime(const string& value, const Debugger_s lastDebug = {})
        {
            cvar result = 0;

            if (const auto usIndex = value.find('_'); usIndex != value.npos)
            {
                const auto numberPart = value.substr(0, usIndex);
                const auto timePart = value.substr(usIndex + 1, value.length() - usIndex - 1);

                if (isFloat(numberPart))
                    result = stod(numberPart);
                else
                    result = stoll(numberPart);

                if (timePart == "ms")
                {
                    // no translation
                }
                else if (timePart == "seconds" || timePart == "second")
                {
                    result *= 1000;
                }
                else if (timePart == "minutes" || timePart == "minute")
                {
                    result = result * 60 * 1000;
                }
                else if (timePart == "hours" || timePart == "hour")
                {
                    result = result * 60 * 60 * 1000;
                }
                else if (timePart == "days" || timePart == "day")
                {
                    result = result * 24 * 60 * 60 * 1000;
                }
                else if (timePart == "weeks" || timePart == "week")
                {
                    result = result * 7 * 24 * 60 * 60 * 1000;
                }
                else if (timePart == "months" || timePart == "month")
                {
                    result = result * 31 * 24 * 60 * 60 * 1000;
                }
                else if (timePart == "years" || timePart == "year")
                {
                    result = result * 365 * 24 * 60 * 60 * 1000;
                }
                else
                {
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "invalid time shorthand",
                        lastDebug

                    };
                }

                return result;
            }

            if (isFloat(value))
                result = stod(value);
            else
                result = stoll(value);

            return result;
        }

        bool isTableColumn(std::string name) const
        {
            if (name.find("column.") == 0)
                name = name.substr(name.find('.') + 1);

            return (tableColumns->getColumn(name) != nullptr);
        }

        static bool isMarshal(const std::string& name)
        {
            return (Marshals.find(name) != Marshals.end());
        }

        bool isUserVar(const std::string& name) const
        {
            return (getTrackingIndex(userVars, name) != -1);
        }

        int userVarIndex(const std::string& name)
        {
            if (const auto idx = getTrackingIndex(userVars, name); idx != -1)
                return idx;
            userVars.emplace_back(name);
            return static_cast<int>(userVars.size()-1);
        }

        int stringLiteralIndex(const std::string& name)
        {
            if (const auto idx = getTrackingIndex(stringLiterals, name); idx != -1)
                return idx;
            stringLiterals.emplace_back(name);
            return static_cast<int>(stringLiterals.size()-1);
        }

        int columnIndex(const std::string& name)
        {
            if (const auto idx = getTrackingIndex(columns, name); idx != -1)
                return idx;
            columns.emplace_back(name);
            return static_cast<int>(columns.size()-1);
        }

        int selectsIndex(const std::string& name)
        {
            if (const auto idx = getTrackingIndex(selects, name); idx != -1)
                return idx;
            selects.emplace_back(name);
            return static_cast<int>(selects.size()-1);
        }

        void incUserVarAssignmentCount(const std::string& name)
        {
            if (const auto& iter = userVarAssignments.find(name); iter != userVarAssignments.end())
                ++iter->second;
            userVarAssignments.emplace(name, 1);
        }

        bool isAssignedUserVar(const std::string& name)
        {
            if (name == "props")
                return true;
            return userVarAssignments.find(name) != userVarAssignments.end();
        }

        static std::string stripQuotes(const string& text)
        {
            auto result = text;
            if (result.length() && (result[0] == '"' || result[0] == '\''))
                result = result.substr(1, result.length() - 2);
            return result;
        }

        // step 1 - parse raw query string, generate array of tokens
        std::vector<std::string> parseRawQuery(const std::string& query) const
        {
            std::vector<std::string> accumulated;
            std::string current;

            auto c         = query.c_str(); // cursor
            const auto end = query.c_str() + query.length();

            while (c < end)
            {

                if (c[0] == '#')
                {
                    current = trim(current);
                    if (current.length())
                    {
                        accumulated.push_back(current);
                        current.clear();
                    }

                    while (c < end)
                    {
                        if (*c == '\n' || *c == '\r')
                            break;
                        ++c;
                    }
                    continue;
                }

                // negative number, not math
                if (c[0] == '-' && isDigit(c[1]))
                {
                    current += c[0];
                    ++c;
                    continue;
                }

                // a period (.) not followed by a number (i.e. a member function)
                if (c[0] == '.' && !isDigit(c[1]))
                {
                    current = trim(current);
                    if (current.length())
                        accumulated.push_back(current);
                    current.clear();

                    current = "__chain_";
                    ++c;
                    continue;
                }

                // quoted strings - with expansion of escaped values
                if (*c == '\'' || *c == '\"')
                {
                    const auto endChar = *c;

                    current = trim(current);
                    if (current.length())
                        accumulated.push_back(current);
                    current.clear();

                    current += *c;
                    ++c;
                    while (c < end)
                    {

                        if (*c == '\\')
                        {
                            ++c;
                            switch (*c)
                            {
                            case 't':
                                current += '\t';
                                break;
                            case 'r':
                                current += '\r';
                                break;
                            case 'n':
                                current += '\n';
                                break;
                            case '\'':
                                current += '\'';
                                break;
                            case '"':
                                current += '"';
                            case '\\':
                                current += '\\';
                            case '/':
                                current += '/';
                                break;
                            default:
                                throw QueryParse2Error_s {
                                    errors::errorClass_e::parse,
                                    errors::errorCode_e::syntax_error,
                                    "invalid character escape sequence",
                                    lastDebug
                                };
                                break;
                            }
                            ++c;
                            continue;
                        }
                        current += *c;
                        if (*c == endChar)
                            break;
                        ++c;
                    }

                    ++c;
                    accumulated.push_back(current);
                    current.clear();
                    continue;
                }

                // double symbols == != >= <=, etc.
                if (c + 1 < end &&
                    ((c[0] == '!' && c[1] == '=') ||
                    (c[0] == '>' && c[1] == '=') ||
                    (c[0] == '<' && c[1] == '=') ||
                    (c[0] == '+' && c[1] == '=') ||
                    (c[0] == '-' && c[1] == '=') ||
                    (c[0] == '*' && c[1] == '=') ||
                    (c[0] == '/' && c[1] == '=') ||
                    (c[0] == '<' && c[1] == '<') ||
                    (c[0] == '<' && c[1] == '>') ||
                    (c[0] == ':' && c[1] == ':') ||
                    (c[0] == '=' && c[1] == '=')))
                {
                    current = trim(current);
                    if (current.length())
                        accumulated.push_back(current);
                    current.clear();

                    current += c[0];
                    current += c[1];
                    accumulated.push_back(current);
                    current.clear();

                    c += 2;
                    continue;
                }

                // everything else
                switch (*c)
                {
                case '(':
                case ')':
                case '{':
                case '}':
                case '[':
                case ']':
                case ',':
                case ':':
                case '+':
                case '-':
                case '*':
                case '/':
                    current = trim(current);
                    if (current.length())
                    {
                        accumulated.push_back(current);
                        current.clear();
                    }
                    accumulated.emplace_back(c, 1);
                    break;

                // whitespace and line wraps
                case '\r':
                case '\t':
                case '\n':
                case ' ':
                    current = trim(current);
                    if (current.length())
                    {
                        accumulated.push_back(current);
                        current.clear();
                    }
                    break;
                default:
                    current += *c;
                }

                ++c;
            }

            current = trim(current);
            if (current.length())
                accumulated.push_back(current);

            return accumulated;
        }

        const std::unordered_set<std::string> blockStartWords = {
            "if",
            "else",
            "elsif",
            "select",
            "for",
            "each_row",
        };

        int __blockExtractionSeekEnd(std::vector<std::string>& tokens, int start, int end) const
        {
            auto count = 1;

            while (start < end)
            {
                if (blockStartWords.count(tokens[start]))
                    ++count;

                if (tokens[start] == "end")
                {
                    --count;
                    if (count == 0)
                        return start;
                }

                ++start;
            }
            return -1;
        }

        static int lookBack(const Blocks::Line& words, int start)
        {
            auto count = 0;

            while (start >= 0)
            {
                const auto token = words[start];

                if (token == "(")
                {
                    --count;
                    if (count == 0)
                        return start;
                }
                else if (token == ")")
                    ++count;
                --start;
            }
            return -1;
        }

        void pushLogic(const Blocks::Line& words, const int start = 0, int end = -1)
        {
            if (end == -1)
                end = static_cast<int>(words.size());

            auto logicFound = false;

            std::for_each(words.begin() + start, words.begin() + end, [&](auto token) {
                if (isTableColumn(token))
                    logicFound = true;
            });

            if (!logicFound)
                return;

            if (indexLogic.size())
                indexLogic.push_back("||");

            indexLogic.push_back("(");
            indexLogic.insert(indexLogic.end(), words.begin() + start, words.begin() + end);
            indexLogic.push_back(")");
        }

        bool validNext(std::vector<std::string>&tokens, int offset) const
        {
            const std::unordered_set<std::string> forceNewLine = {
                "if",
                "else",
                "elsif",
                "for",
                "end",
                "each_row",
                "<<"
            };

            const std::unordered_set<std::string> validAfterVarOrNum = {
                "&&",
                "||",
                "==",
                "!=",
                ">=",
                "<=",
                ">",
                "<",
                "in",
                "any",
                "contains",
                ")",
                "(",
                "}",
                "{",
                "[",
                "]",
                "+",
                "=",
                "-",
                "*",
                "in",
                "/",
                ",",
                ":",
                "where"
            };

            const std::unordered_set<std::string> validAfterCondition = {
                "(",
            };

            const std::unordered_set<std::string> validAfterClosingBracket = {
                "||",
                "&&",
                "==",
                "=",
                ">=",
                "<=",
                ">",
                "<",
                "in",
                "any",
                "contains",
                "where",
                ",",
                ")",
                "(",
                "]",
                "[",
                "{",
                "}",
                "+",
                "-",
                "*",
                "/"
            };

            const auto token = tokens[offset];
            const auto nextToken = offset + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[offset + 1];
            const auto prevToken = offset - 1 < 0 ? std::string() : tokens[offset - 1];
            const auto isAfterBracketValid = validAfterClosingBracket.count(nextToken) != 0;

            const auto isItem = isNameOrNumber(token) && Operators.count(token) == 0;
            const auto isNextAnItem = isNameOrNumber(nextToken) && Operators.count(nextToken) == 0;
            const auto isNextChain = nextToken.find("__chain_") == 0;

            const int lookBackIndex = lookBack(tokens, offset);
            const auto inChain = token == ")" && lookBackIndex > 0 ?
                tokens[lookBackIndex-1].find("__chain_") == 0 : false;

            // end means stop
            if (token == "end")
                return false;

            // we are ok with almost anything after a where
            if (token == "where" || nextToken == "where")
                return true;

            // is it an a condition or iterator?
            if (forceNewLine.count(token))
            {
                if (isNameOrNumber(nextToken) || validAfterCondition.count(nextToken))
                    return true;
                return false;
            }

            // closing brackets...
            if (token == ")" && !isNextChain && isNextAnItem)
                return false;

            if (token == ")" && !isNextChain && !inChain && !isAfterBracketValid)
                return false;

            // closing brackets...
            if ((token == "]" || token == "}") && (isNextAnItem || !isAfterBracketValid))
                return false;

            // we should never have two numbers, words or funtions side by side, if we do
            // it's the end of a line, unless it's a `for x in y` scenario
            if (isItem && isNextAnItem && !isNextChain && nextToken != "in" && token != "in")
                return false;

            if (isNextChain)
                return true;

            // is the next thing valid following a name or number
            if (isItem && validAfterVarOrNum.count(nextToken))
                return true;

            // is the current thing an operator
            if (validAfterVarOrNum.count(token))
                return true;

            return false;
        }

        bool checkForForcedLine(Blocks::Line& words, const int start) const
        {
            auto idx = start;

            if (start >= static_cast<int>(words.size()))
                return true;

            const auto variable = words[idx];
            ++idx;

            // no subscript, just a variable (checked for function/table var by parseItem)
            if (idx >= static_cast<int>(words.size()) || words[idx] != "[")
            {
                const auto nextToken = idx >= static_cast<int>(words.size()) ? std::string() : words[idx];
                return nextToken == "=";
            }

            auto end = seekMatchingSquare(words, idx);
            ++idx;

            while (idx < end)
            {
                auto value = extract(words, idx, end);
                idx = end;

                const auto nextToken = idx + 1 >= static_cast<int>(words.size()) ? std::string() : words[idx + 1];

                // Test for multi-depth-subscripts foo[index][nestedIndex]
                if (nextToken == "[")
                {
                    end = seekMatchingSquare(words, idx + 1);
                    idx += 2;
                }
                else
                {
                    return nextToken == "=";
                }
            }

            return false;
        }

        // select
        int parseSelect(Blocks::Line& tokens, const int start)
        {
            const std::unordered_set<std::string> newStatementWords = {
                "count",
                "min",
                "max",
                "avg",
                "sum",
                "value",
                "var",
                "code"
            };

            auto idx = start + 1;
            const auto end = static_cast<int>(tokens.size());

            while (idx < end)
            {
                auto token = tokens[idx];
                auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];

                // end of select definition
                if (token == "end")
                    return idx + 1;

                // should be a modifier?
                if (!ColumnModifiers.count(token))
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "expecting an aggregate in `select` statement",
                        lastDebug
                    };

                // should be a textual word
                if (!isTextual(nextToken))
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "expecting a text value in `as` statement",
                        lastDebug
                    };


                auto modifier = ColumnModifiers.find(token)->second;
                const auto columnName = nextToken; // actual column name in table
                auto keyColumn = columnName; // distinct to itself
                auto asName = columnName; // aliased as itself

                if (!isTableColumn(columnName))
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "expecting a table column",
                        lastDebug
                    };

                idx += 2;

                token = tokens[idx];
                nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];

                if (token == "as")
                {

                    if (!nextToken.length() || !isTextual(nextToken))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "expecting a name in `as` portion of `select` statement",
                            lastDebug
                        };

                    if (isTableColumn(nextToken))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "`as` portion of `select` statement cannot be a table column",
                            lastDebug
                        };

                    asName = nextToken;
                    idx += 2;

                    token = tokens[idx];
                    nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];
                }

                if (token == "key")
                {

                    if (!nextToken.length() || !isTextual(nextToken))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "expecting a name in `key` portion of `select` statement",
                            lastDebug
                        };

                    if (!isTableColumn(nextToken))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "`key` portion of `select` must be a table column",
                            lastDebug
                        };

                    keyColumn = nextToken;
                    idx += 2;
                }


                // already used, then throw and suggest using `as`
                if (getTrackingIndex(selects, asName) != -1)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "`as` name in `select` already in use",
                        lastDebug
                    };

                // register this column as having been referenced
                const auto columnIdx = columnIndex(columnName);

                const auto selectIdx = selectsIndex(asName);

                if (columnName == "session")
                {
                    usesSessions = true;
                    // session counting uses a specialized count method
                    modifier = ColumnModifiers.find("dist_count_person")->second;

                    // reference session so it becomes part of data set
                    columnIndex("session");
                }

                const auto colInfo = tableColumns->getColumn(columnName);

                Variable_s var(columnName, asName, "column", modifier);
                var.distinctColumnName = keyColumn;

                var.index = selectIdx; // index in variable array
                var.column = columnIdx; // index in grid
                var.schemaColumn = colInfo->idx;
                var.schemaType = colInfo->type;

                // if this is selection is keyed to another column lets reference it as well
                const auto keyIdx = columnIndex(keyColumn);
                var.distinctColumn = keyIdx; // index of key column in grid

                selectColumnInfo.push_back(var);
            }

            // THROW - should have found `end`

        }

        int extractLine(Blocks::Line& tokens, const int start, Blocks::Line& extraction)
        {
            const std::unordered_set<std::string> forceNewLine = {
                "if",
                "else",
                "elsif",
                "for",
                "end",
                "each_row",
                "<<"
            };

            extraction.clear();
            auto idx = start;
            const auto end = static_cast<int>(tokens.size());

            while (idx < end)
            {
                const auto token = tokens[idx];

                if (token == "select")
                    return parseSelect(tokens, idx);

                if (token == "(")
                {
                    const auto matchingIndex = seekMatchingBrace(tokens, idx);

                    if (matchingIndex == -1)
                    {
                        //THROW
                    }

                    extraction.insert(extraction.end(), tokens.begin() + idx, tokens.begin() + matchingIndex + 1);
                    idx = matchingIndex + 1;

                    if (!validNext(tokens, matchingIndex) || checkForForcedLine(tokens, idx))
                        return idx;

                    continue;
                }

                if (token == "[")
                {
                    const auto matchingIndex = seekMatchingSquare(tokens, idx);

                    if (matchingIndex == -1)
                    {
                        //THROW
                    }

                    extraction.insert(extraction.end(), tokens.begin() + idx, tokens.begin() + matchingIndex + 1);
                    idx = matchingIndex + 1;

                    if (!validNext(tokens, matchingIndex) || checkForForcedLine(tokens, idx))
                        return idx;

                    continue;
                }

                if (token == "{")
                {
                    const auto matchingIndex = seekMatchingCurly(tokens, idx);

                    if (matchingIndex == -1)
                    {
                        //THROW
                    }

                    extraction.insert(extraction.end(), tokens.begin() + idx, tokens.begin() + matchingIndex + 1);
                    idx = matchingIndex + 1;

                    if (!validNext(tokens, matchingIndex) || checkForForcedLine(tokens, idx))
                        return idx;

                    continue;
                }

                // force new line immediately
                if (forceNewLine.count(token) && idx != start)
                {
                    return idx;
                }

                // force new line if the next thing is word or number and an assignment is immediately next
                if ((isNameOrNumber(token) && checkForForcedLine(tokens, idx) && idx != start))
                {
                    extraction.push_back(token);
                    return idx + 1;
                }

                extraction.push_back(token);

                if (!validNext(tokens, idx))
                    return idx + 1;

                ++idx;
            }

            return idx;
        }

        void __extractBlock(Blocks::Line& tokens, Blocks::Block_s* block, const int start, const int end)
        {

            auto idx = start;

            Blocks::Line line;

            while (idx < end)
            {

                const auto startWord = tokens[idx];
                const auto isNewBlock = blockStartWords.count(tokens[idx]) != 0;
                idx = extractLine(tokens, idx, line);

                if (startWord == "select")
                    continue;

                for (const auto &i : line)
                    cout << i << " ";
                cout << endl;

                auto blockId = -1;

                // go recursive for sub block
                if (isNewBlock)
                {
                    const auto blockEnd = __blockExtractionSeekEnd(tokens, idx, end);

                    Debugger_s debug;
                    debug.set(line);

                    if (blockEnd == -1)
                    {
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "unable to find end of code block (are you missing an `end` after a code block?)",
                            lastDebug
                        };
                    }

                    const auto subBlock = blocks.newBlock();
                    __extractBlock(tokens, subBlock, idx, blockEnd);

                    idx = blockEnd;
                    blockId = subBlock->blockId;
                }

                block->lines.emplace_back(line);
                block->lines.back().codeBlock = blockId;
            }
        }

        void extractBlocks(std::vector<std::string>& tokens)
        {
            const auto block = blocks.newBlock();
            __extractBlock(tokens, block,0, tokens.size());
        }

        // seek for `seek` outside of parenthesis nesting
        static int seek(const std::string& seek, const Blocks::Line& words, int start, int end = -1)
        {
            if (end == -1)
                end = words.size();

            auto count = 0;

            while (start < end)
            {
                const auto token = words[start];

                if (token == "(" || token == "[" || token == "{")
                    ++count;
                else if (token == ")" || token == "]" || token == "}")
                    --count;

                if (!count && token == seek)
                    return start;
                ++start;
            }
            return -1;
        }

        // seek, not caring about parenthesis
        static int seekRaw(const std::string& seek, const Blocks::Line& words, int start, int end = -1)
        {
            if (end == -1)
                end = words.size();

            while (start < end)
            {
                const auto token = words[start];

                if (token == seek)
                    return start;
                ++start;
            }
            return -1;
        }

        int seekMatchingBrace(const Blocks::Line& words, int start, int end = -1) const
        {
            if (end == -1)
                end = words.size();

            auto count = 0;

            while (start < end)
            {
                if (words[start] == "(")
                {
                    ++count;
                }
                else if (words[start] == ")")
                {
                    --count;
                    if (!count)
                        return start;
                }
                ++start;
            }

            throw QueryParse2Error_s {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                "missing closing ')' bracket",
                lastDebug
            };
        }

        int seekMatchingSquare(const Blocks::Line& words, int start, int end = -1) const
        {
            if (end == -1)
                end = words.size();

            auto count = 0;

            while (start < end)
            {
                if (words[start] == "[")
                {
                    ++count;
                }
                else if (words[start] == "]")
                {
                    --count;
                    if (!count)
                        return start;
                }
                ++start;
            }

            throw QueryParse2Error_s {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                "missing closing ']' bracket",
                lastDebug
            };
        }

        int seekMatchingCurly(const Blocks::Line& words, int start, int end = -1) const
        {
            if (end == -1)
                end = words.size();

            auto count = 0;

            while (start < end)
            {
                if (words[start] == "{")
                {
                    ++count;
                }
                else if (words[start] == "}")
                {
                    --count;
                    if (!count)
                        return start;
                }
                ++start;
            }

            throw QueryParse2Error_s {
                errors::errorClass_e::parse,
                errors::errorCode_e::syntax_error,
                "missing closing '}' bracket",
                lastDebug
            };
        }

        int parseInlineIterator(const Blocks::Line& words, int start)
        {

            const auto end = static_cast<int>(words.size());
            auto idx = start;


            const auto iteratorName = words[idx];
            const auto iteratorOp = inlineIterators.find(iteratorName)->second;

            ++idx;

            // -1 means no agg block
            auto aggBlockId = -1;

            // row and test don't need an aggregator statement
            if (iteratorName != "row" && iteratorName != "test")
            {

                // throw if we require an aggregator statement
                if (idx >= end || words[idx] != "(")
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "aggregation '" + iteratorName + "' takes one parameter",
                        lastDebug
                    };

                // extract the `summing statement` passed to the inline aggregator i.e. count(product_name)
                std::vector<std::pair<Blocks::Line,int>> params;
                idx = parseParams(words, idx, params);

                if (params.size() != 1)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "aggregation '" + iteratorName + "' takes one parameter",
                        lastDebug
                    };

                aggBlockId = addLinesAsBlock(params[0].first);
            }

            // inline aggregations use `each_row` style filters, lets parse them
            idx = parseFilterChain(false, words, idx);

            if (idx >= end || words[idx] != "where")
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting `where` in '" + iteratorName + "' statement",
                    lastDebug
                };

            ++idx; // skip past where look for logic
            const Blocks::Line logic(words.begin() + idx, words.end());

            pushLogic(logic);

            // if there is no logic, just straight iteration we push the logic block as -1
            // the interpreter will run in a true state for the logic if it sees -1
            const auto logicBlockId = logic.size() == 0 ? -1 : addLinesAsBlock(logic);

            middle.emplace_back(
                iteratorOp,
                aggBlockId,
                logicBlockId,
                lastDebug.line,
                0);
        }


        int parseStatement(int relative, const Blocks::Line& words, int start, int end = -1)
        {
            const std::unordered_set<std::string> operatorWords = {
                "&&",
                "||",
                "+",
                "-",
                "/",
                "*",
            };

            const std::unordered_set<std::string> logicWords = {
                "==",
                "!=",
                ">",
                "<",
                "<=",
                ">=",
                "in",
                "contains",
                "any",
            };

            const std::unordered_set<std::string> isAnListOrDict = {
                ",",
                "(",
                "=",
                "==",
                "in",
                "any",
                "contains",
                "&&",
                "||",
                "[",
                "+",
                "-",
                ":",
                "{",
                ""
            };

            if (end == -1)
                end = static_cast<int>(words.size());
            auto idx = start;

            std::vector<std::pair<std::string,int>> ops;

            while (idx < end)
            {
                const auto token = words[idx];

                const auto nextToken = idx + 1 >= static_cast<int>(words.size()) ? std::string() : words[idx + 1];
                const auto prevToken = idx == 0 ? std::string() : words[idx - 1];

                if (token == "end")
                    return end;

                if (inlineIterators.count(token))
                {
                    idx = parseInlineIterator(words, idx);
                    ++idx;
                    continue;
                }

                if (isMarshal(token))
                {
                    if (nextToken == "(")
                    {
                        const int beforeIdx = idx;
                        std::vector<std::pair<Blocks::Line, int>> params;
                        idx = parseParams(words, idx + 1, params);

                        for (const auto& param: params)
                            parseStatement(relative + param.second, param.first, 0, param.first.size());

                        const auto marshalIndex = static_cast<int>(Marshals.find(token)->second);
                        middle.emplace_back(
                            MiddleOp_e::marshal,
                            marshalIndex,
                            static_cast<int>(params.size()),
                            lastDebug.line,
                            relative + beforeIdx);
                    }
                    else
                    {
                        if (!MacroMarshals.count(token))
                            throw QueryParse2Error_s {
                                errors::errorClass_e::parse,
                                errors::errorCode_e::syntax_error,
                                "function call for '" + token + "' requires parameters",
                                lastDebug
                            };

                        const auto marshalIndex = static_cast<int>(Marshals.find(token)->second);
                        middle.emplace_back(
                            MiddleOp_e::marshal,
                            marshalIndex,
                            0,
                            lastDebug.line,
                            relative + idx);

                        ++idx;
                    }

                    //++idx;
                    continue;
                }

                if (!isTableColumn(token) && nextToken.find("__chain_") == 0)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "filter applied to: '" + token + "' (filters can only be applied to columns)",
                        lastDebug
                    };

                if (isTextual(token) && nextToken == "(")
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "item '" + token + "' is not a function (called with parameters)",
                        lastDebug
                    };

                if (token == ")" || token == "}" || token == "]")
                {
                    ++idx;
                    continue;
                }

                if (token == "(")
                {
                    const auto subEnd = seekMatchingBrace(words, idx, end);
                    idx = parseStatement(relative + idx, words, idx + 1, subEnd);
                    continue;
                }

                // if this is a text and the next token is a [ then this has to be as subscript
                /*if (isTextual(token) && nextToken == "[")
                {
                    idx = parseSubscript(words, idx, false) + 1;
                    continue;
                }*/

                // nested array or accessor?
                // array: `[` is first char, or proceeded by `[`, `==`, `=`, `(` or `,`
                if (token == "[")
                {
                    if (isAnListOrDict.count(prevToken) || prevToken == "")
                    {
                        idx = parseList(words, idx);
                    }
                    else
                    {
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "unexpected subscript or malformed array",
                            lastDebug
                        };
                    }
                    //++idx;
                    continue;
                }

                if (token == "{")
                {
                    if (isAnListOrDict.count(prevToken) || prevToken == "")
                    {
                        idx = parseDictionary(words, idx);
                    }
                    else
                    {
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "unexpected subscript or malformed dictionary",
                            lastDebug
                        };
                    }
                    //++idx;
                    continue;
                }

                if (!operatorWords.count(token) && !logicWords.count(token))
                {
                    idx = parseItem(words, idx, words);
                    continue;
                }

                if (operatorWords.count(token))
                {
                    ops.emplace_back(token, relative + idx);
                    ++idx;
                    continue;
                }

                // if this is an equality/inequality test we push the test immediately to leave
                // a true/false on the stack
                if (logicWords.count(token))
                {
                    if (nextToken.length())
                    {
                        const auto beforeIdx = idx;

                        if (nextToken == "[")
                        {
                            if (isAnListOrDict.count(token))
                            {
                                idx = parseList(words, idx + 1);
                            }
                            else
                            {
                                throw QueryParse2Error_s {
                                    errors::errorClass_e::parse,
                                    errors::errorCode_e::syntax_error,
                                    "unexpected subscript or malformed array",
                                    lastDebug
                                };
                            }
                        }
                        else if (nextToken == "(")
                        {
                            const auto subEnd = seekMatchingBrace(words, idx + 1, end) + 1;
                            idx = parseStatement(idx, words, idx + 1, subEnd) + 1;
                        }
                        else
                        {
                            idx = parseItem(words, idx + 1, words);
                        }

                        middle.emplace_back(
                            ConditionToMiddleOp.find(token)->second,
                            lastDebug.line,
                            beforeIdx);

                    }
                    else
                    {
                        // THROW??
                    }
                    continue;
                }
                ++idx;
            }

            // push any accumulated logical or math operators onto the stack in reverse
            std::for_each(ops.rbegin(), ops.rend(), [&](auto op) {
               middle.emplace_back(
                   ConditionToMiddleOp.find(op.first)->second,
                   lastDebug.line,
                   op.second);
            });

            return idx + 1;
        }

        static Blocks::Line extract(const Blocks::Line& words, int start, int end)
        {
            Blocks::Line result;
            auto idx = start;

            while (idx < end)
            {
                result.push_back(words[idx]);
                ++idx;
            }

            return result;
        }

        int parseParams(const Blocks::Line& words, int start, std::vector<std::pair<Blocks::Line,int>>& params) const
        {
            params.clear();
            std::deque<std::pair<Blocks::Line,int>> result;
            auto idx = start;

            if (words[idx] != "(")
            {
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting '()' brackets for function call",
                    lastDebug
                };
            }

            const auto end = seekMatchingBrace(words, start);
            ++idx;

            while (idx < end)
            {
                const auto commaPosition = seek(",", words, idx);
                if (commaPosition == -1)
                {
                    auto param = extract(words, idx, end);
                    result.push_front(make_pair(param, idx));
                    idx = end;
                }
                else
                {
                    auto param = extract(words, idx, commaPosition);
                    result.push_front(make_pair(param, idx));
                    idx = commaPosition;
                }

                ++idx;
            }

            // push the items into the result in reverse
            for (auto& item : result)
                params.emplace_back(item);

            return idx;
        }

        int parseList(const Blocks::Line& words, int start)
        {
            std::deque<std::pair<Blocks::Line,int>> params;
            auto idx = start;

            if (words[idx] != "[")
            {
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting '[]' brackets for list",
                    lastDebug
                };
            }

            const auto end = seekMatchingSquare(words, start);
            ++idx;

            while (idx < end)
            {
                const auto commaPosition = seek(",", words, idx);
                if (commaPosition == -1 || commaPosition >= end)
                {
                    auto value = extract(words, idx, end);
                    params.push_front(std::make_pair(value, idx));
                    idx = end;
                }
                else
                {
                    auto value = extract(words, idx, commaPosition);
                    params.push_front(std::make_pair(value, idx));
                    idx = commaPosition;
                }

                ++idx;
            }

            // push the items into the result in reverse
            for (auto& item : params)
                parseStatement(item.second, item.first, 0);

            middle.emplace_back(
                MiddleOp_e::marshal,
                static_cast<int>(Marshals_e::marshal_make_list),
                static_cast<int>(params.size()),
                lastDebug.line,
                start);

            return idx;
        }

        int parseSubscript(const Blocks::Line& words, const int start, const bool assignment, const bool asRef = false)
        {
            std::deque<std::pair<Blocks::Line,int>> subScripts;
            auto idx = start;

            const auto variable = words[idx];
            ++idx;

            // no subscript, just a variable (checked for function/table var by parseItem)
            if (idx >= static_cast<int>(words.size()) || words[idx] != "[")
            {
                auto variableIndex = userVarIndex(variable);

                if (assignment)
                    incUserVarAssignmentCount(variable);

                MiddleOp_e op;

                if (asRef)
                    op = assignment ? MiddleOp_e::pop_user_ref : MiddleOp_e::push_user_ref;
                else
                    op = assignment ? MiddleOp_e::pop_user_var : MiddleOp_e::push_user;

                middle.emplace_back(
                    op,
                    variableIndex,
                    lastDebug.line,
                    start);

                return idx;
            }

            auto end = seekMatchingSquare(words, idx);
            ++idx;

            while (idx < end)
            {

                auto value = extract(words, idx, end);
                subScripts.push_front(make_pair(value,idx));
                idx = end;

                const auto nextToken = idx + 1 >= static_cast<int>(words.size()) ? std::string() : words[idx + 1];

                // Test for multi-depth-subscripts foo[index][nestedIndex]
                if (nextToken == "[")
                {
                    end = seekMatchingSquare(words, idx + 1);
                    idx += 2;
                }

            }

            // push the items into the result in reverse
            for (auto& item : subScripts)
                parseStatement(item.second, item.first, 0);

            if (isTableColumn(variable))
            {
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "subscript is not possible on table columns",
                    lastDebug
                };
            }

            if (assignment)
                incUserVarAssignmentCount(variable);

            MiddleOp_e op;

            if (asRef)
                op = assignment ? MiddleOp_e::pop_user_obj_ref : MiddleOp_e::push_user_obj_ref;
            else
                op = assignment ? MiddleOp_e::pop_user_obj : MiddleOp_e::push_user_obj;

            auto variableIndex = userVarIndex(variable);
            middle.emplace_back(
                op,
                variableIndex,
                static_cast<int>(subScripts.size()),
                lastDebug.line,
                start);

            /*
            // convert subscript into function
            middle.emplace_back(
                MiddleOp_e::marshal,
                assignment ? static_cast<int>(Marshals_e::marshal_pop_subscript) : static_cast<int>(Marshals_e::marshal_push_subscript),
                static_cast<int>(subScripts.size() + 1),
                lastDebug.line,
                start);
            */

            return end + 1;
        }

        int parseDictionary(const Blocks::Line& words, int start)
        {
            std::deque<std::pair<Blocks::Line,int>> values;
            std::deque<std::string> keys;
            auto idx = start;

            if (words[idx] != "{")
            {
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting '{}' brackets for dictionary",
                    lastDebug
                };
            }

            const auto end = seekMatchingCurly(words, start);
            ++idx;

            while (idx < end)
            {
                const auto commaPosition = seek(",", words, idx);
                const auto colonPosition = seek(":", words, idx, commaPosition);

                if (colonPosition == -1 || colonPosition >= end)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "expecting ':' after key in dictionary",
                        lastDebug
                    };

                if (commaPosition == -1 || colonPosition >= end)
                {
                    auto key = words[idx];
                    auto value = extract(words, colonPosition + 1, end);

                    keys.push_front(key);
                    values.push_front(make_pair(value, idx));
                    idx = end;
                }
                else
                {
                    auto key = words[idx];
                    auto value = extract(words, colonPosition + 1, commaPosition);

                    keys.push_front(key);
                    values.push_front(make_pair(value, idx));
                    idx = commaPosition;
                }

                ++idx;
            }

            // push the items into the result in reverse
            for (auto& item : values)
            {
                auto key = keys.front();
                keys.pop_front();

                if (!isTextual(key) && !isString(key))
                {
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "dictionaries may not have numeric keys (convert keys to strings)",
                        lastDebug
                    };
                }

                auto cleanString = stripQuotes(key);
                auto litIndex = stringLiteralIndex(cleanString);
                // push the key

                middle.emplace_back(
                    MiddleOp_e::push_literal,
                    litIndex,
                    lastDebug.line,
                    item.second);

                // parse the value (which will leave a single entry on the stack)
                parseStatement(item.second, item.first, 0);

            }

            middle.emplace_back(
                MiddleOp_e::marshal,
                static_cast<int>(Marshals_e::marshal_make_dict),
                static_cast<int>(values.size() * 2),
                lastDebug.line,
                start);

            return idx;
        }

        int addLinesAsBlock(const std::vector<Blocks::Line>& lines)
        {
            const auto newBlock = blocks.newBlock();

            for (auto &line : lines)
            {
                Blocks::LineItem_s lineItem(line);
                newBlock->lines.emplace_back(lineItem);
            }

            return newBlock->blockId;
        }

        int addLinesAsBlock(const Blocks::Line& line)
        {
            return addLinesAsBlock(std::vector<Blocks::Line>{line});
        }

        int parseItem(const Blocks::Line& words, int start, const Blocks::Line& debugLine, const bool assignment = false)
        {
            const auto item = words[start];

            if (assignment && isMarshal(item))
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "left side argument is a reserved word: '" + item + "'",
                    lastDebug
                };

            if (assignment &&
                (
                item == "true" ||
                item == "false" ||
                item == "nil" ||
                isString(item) ||
                isFloat(item) ||
                isNumeric(item) ||
                isTableColumn(item)))
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "left side argument must be a user variable: " + item + "'",
                    lastDebug
                };

            if (item == "true")
            {
                middle.emplace_back(MiddleOp_e::push_true, lastDebug.line, start);
                return start + 1;
            }

            if (item == "false")
            {
                middle.emplace_back(MiddleOp_e::push_false, lastDebug.line, start);
                return start + 1;
            }

            if (item == "nil")
            {
                middle.emplace_back(MiddleOp_e::push_nil, lastDebug.line, start);
                return start + 1;
            }

            if (isString(item))
            {
                auto cleanString = stripQuotes(item);
                auto stringIdx = stringLiteralIndex(cleanString);
                middle.emplace_back(MiddleOp_e::push_literal, stringIdx, lastDebug.line, start);

                return start + 1;
            }

            if (isFloat(item))
            {
                middle.emplace_back(MiddleOp_e::push_double, expandTime(item, lastDebug).getDouble(), lastDebug.line, start);
                return start + 1;
            }

            if (isNumeric(item))
            {
                middle.emplace_back(MiddleOp_e::push_int, expandTime(item, lastDebug).getInt64(), lastDebug.line, start);
                return start + 1;
            }

            if (isTableColumn(item))
            {
                const auto filterEndIndex = parseFilterChain(true, words, start + 1, item);
                auto columnIdx = columnIndex(item);
                middle.emplace_back(MiddleOp_e::push_column, columnIdx, lastDebug.line, start);
                return filterEndIndex;
            }

            if (isMarshal(item))
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "unexpected reserved word: '" + item + "'",
                    lastDebug
                };

            return parseSubscript(words, start, assignment);
        }

        int parseReference(const Blocks::Line& words, const int start, const Blocks::Line& debugLine)
        {
            const auto item = words[start];

            if (isMarshal(item))
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "not expecting reserved word: '" + item + "'",
                    lastDebug
                };

            if (isString(item) ||
                isFloat(item) ||
                isNumeric(item) ||
                isTableColumn(item))
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting user variable: " + item + "'",
                    lastDebug
                };

            return parseSubscript(words, start, false);
        }

        int parseFilterChain(const bool isColumn, const Blocks::Line& words, const int start, const std::string& columnName = "")
        {
            const auto end = static_cast<int>(words.size());
            auto idx = start;

            Filter_s filter;
            auto count = 0;

            auto usedForward = false;

            while (idx < end)
            {
                const auto token = words[idx];
                const auto nextToken = idx + 1 >= static_cast<int>(words.size()) ? std::string() : words[idx + 1];

                // test for missing brackets
                if (token.find("__chain_") == 0 && nextToken != "(")
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "filter '" + token.substr(token.find("__chain_")) + "' is missing brackets",
                        lastDebug
                    };

                if (token == "__chain_limit" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() > 1)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".first() takes one optional parameter",
                            lastDebug
                        };

                    if (params.size())
                        filter.limitBlock = addLinesAsBlock(params[0].first);

                    filter.isLimit = true;
                    ++count;
                }
                else if ((token == "__chain_ever" || token == "__chain_never") && isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 1 || (params.size() && params[0].first.size() < 2))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".ever( <logic> ) requires a comparator",
                            lastDebug
                        };

                    const auto comparator = params[0].first[0];
                    params[0].first.insert(params[0].first.begin(), columnName);

                    if (!Operators.count(comparator))
                    {
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".ever( <logic> ) requires a comparator",
                            lastDebug
                        };
                    }

                    if (params.size())
                        filter.evalBlock = addLinesAsBlock(params[0].first);

                    filter.comparator = static_cast<int>(Operators.find(comparator)->second);
                    filter.isEver = true;

                    if (token == "__chain_never")
                        filter.isNegated = true;

                    ++count;
                }
                else if (token == "__chain_row" && isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);


                    if (params.size() != 1 || (params.size() && params[0].first.size() < 2))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".row( <logic> ) requires a comparator",
                            lastDebug
                        };

                    const auto comparator = params[0].first[0];
                    params[0].first.insert(params[0].first.begin(), columnName);

                    if (!Operators.count(comparator))
                    {
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".is( <logic> ) requires a comparator",
                            lastDebug
                        };
                    }

                    if (params.size())
                        filter.evalBlock = addLinesAsBlock(params[0].first);

                    filter.comparator = static_cast<int>(Operators.find(comparator)->second);

                    filter.isRow = true;

                    ++count;
                }
                else if (token == "__chain_reverse" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size())
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".reverse() takes no parameters",
                            lastDebug
                        };

                    filter.isReverse = true;
                    ++count;
                    ++idx;
                }
                else if (token == "__chain_forward" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size())
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".forward() takes no parameters",
                            lastDebug
                        };

                    usedForward = true;
                    filter.isReverse = false;
                    ++count;
                    ++idx;
                }
                else if (token == "__chain_next" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size())
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".next() takes no parameters",
                            lastDebug
                        };

                    filter.isNext = true;
                    ++count;
                    ++idx;
                }
                else if (token == "__chain_within")
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 2)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".within( <window>, <start> ) takes two parameter",
                            lastDebug
                        };

                    // convert our params into code blocks to be called as lambdas
                    filter.withinWindowBlock = addLinesAsBlock(params[1].first);
                    if (params.size() == 2)
                        filter.withinStartBlock = addLinesAsBlock(params[0].first);
                    filter.isWithin = true;

                    ++count;
                }
                else if (token == "__chain_look_ahead")
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 2)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".look_ahead( <window>, <start> ) takes two parameter",
                            lastDebug
                        };

                    // convert our params into code blocks to be called as lambdas
                    filter.withinWindowBlock = addLinesAsBlock(params[1].first);
                    if (params.size() == 2)
                        filter.withinStartBlock = addLinesAsBlock(params[0].first);
                    filter.isLookAhead = true;
                    ++count;
                }
                else if (token == "__chain_look_back")
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 2)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".look_back( <window>, <start> ) takes two parameter",
                            lastDebug
                        };

                    // convert our params into code blocks to be called as lambdas
                    filter.withinWindowBlock = addLinesAsBlock(params[1].first);
                    if (params.size() == 2)
                        filter.withinStartBlock = addLinesAsBlock(params[0].first);
                    filter.isLookBack = true;
                    ++count;
                }
                else if (token == "__chain_range")
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 2)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".range( <start>, <end> ) takes two parameters",
                            lastDebug
                        };

                    // convert our params into code blocks to be called as lambdas
                    filter.rangeStartBlock = addLinesAsBlock(params[1].first);
                    filter.rangeEndBlock = addLinesAsBlock(params[0].first);
                    filter.isRange = true;

                    ++count;
                }
                else if (token == "__chain_continue" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 0)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".continue() takes no parameters",
                            lastDebug
                        };

                    filter.continueBlock = -1;
                    filter.isContinue = true;
                    ++idx;
                    ++count;
                }
                else if (token == "__chain_from" && !isColumn)
                {
                    std::vector<std::pair<Blocks::Line,int>> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 1)
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            ".from(<row>) takes one parameter",
                            lastDebug
                        };

                    filter.continueBlock = addLinesAsBlock(params[0].first);
                    filter.isContinue = true;

                    ++count;
                }
                else if (token.find("__chain_") == 0)
                {
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        isColumn ?
                            "invalid column filter: '" + token.substr(token.find("__chain_")) + "'" :
                            "invalid logical filter: '" + token.substr(token.find("__chain_")) + "'",
                        lastDebug
                    };
                }
                else
                    break;
            }

            if (count)
            {
                // tests for filter combos that just don't work...

                if (filter.isRow && filter.isEver)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "filter must be either '.row' or '.ever'",
                        lastDebug
                    };

                if (usedForward && filter.isReverse)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "filter must be either '.forward' or '.reverse'",
                        lastDebug
                    };

                if (filter.isLookAhead && filter.isLookBack)
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "use '.within' instead of both '.look_ahead' and '.look_back'",
                        lastDebug
                    };

                if (filter.isWithin && (filter.isLookAhead || filter.isLookBack))
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "'.look_ahead' and '.look_back' cannot be used in conjunction with '.within', they perform similar tasks.",
                        lastDebug
                    };


            }

            const auto filterOp = isColumn ? MiddleOp_e::column_filter : MiddleOp_e::logic_filter;
            if (count)
            {
                // sets active filter in opcode
                middle.emplace_back(filterOp, static_cast<int>(filters.size()), lastDebug.line, idx);
                filters.push_back(filter);
            }
            else
            {
                // set default filter
                middle.emplace_back(filterOp, 0, lastDebug.line, idx);
            }

            return idx;
        }

        void parseCondition(int codeBlockId, const Blocks::Line& words)
        {
            const auto condition = words[0];

            currentBlockType.push_back(condition);

            if (condition == "if")
            {
                const auto idx = parseFilterChain(false, words, 1);
                const Blocks::Line logic(words.begin() + idx, words.end());
                pushLogic(logic);

                const auto logicBlockId = addLinesAsBlock(logic);
                middle.emplace_back(
                    MiddleOp_e::if_call,
                    codeBlockId,
                    logicBlockId,
                    lastDebug.line,
                    0);
            }
            else if (condition == "for")
            {
                if (words.size() < 4 || words[2] != "in")
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "for loop is malformed",
                        lastDebug
                    };

                // push the variable containing source data for our iterator
                parseStatement(0, words, 3);

                // push a reference to the variable we will be filling
                const auto variable = words[1];

                incUserVarAssignmentCount(variable);
                auto variableIndex = userVarIndex(variable);

                middle.emplace_back(
                    MiddleOp_e::push_user_ref,
                    variableIndex,
                    lastDebug.line,
                    1);

                middle.emplace_back(
                    MiddleOp_e::for_call,
                    codeBlockId,
                    0,
                    lastDebug.line,
                    0);
            }
            else // each
            {
                auto idx = parseFilterChain(false, words, 1);

                if (idx >= static_cast<int>(words.size()) || words[idx] != "where")
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "expecting `where` in each statement",
                        lastDebug
                    };

                ++idx; // skip past where look for logic
                const Blocks::Line logic(words.begin() + idx, words.end());
                pushLogic(logic);

                // if there is no logic, just straight iteration we push the logic block as -1
                // the interpreter will run in a true state for the logic if it sees -1
                const auto logicBlockId = logic.size() == 0 ? -1 : addLinesAsBlock(logic);
                middle.emplace_back(
                    MiddleOp_e::each_call,
                    codeBlockId,
                    logicBlockId,
                    lastDebug.line,
                    0);
            }

            currentBlockType.pop_back();
        }

        void parseTally(const Blocks::Line& words)
        {

            if (words.size() == 1)
                throw QueryParse2Error_s {
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "expecting at least one group after `<<`",
                    lastDebug
                };

            // the `<<` statement doesn't take brackets, so we are adding them before
            // we call parseParams
            Blocks::Line modifiedSequence;
            modifiedSequence.push_back("(");
            modifiedSequence.insert(modifiedSequence.end(), words.begin() + 1, words.end());
            modifiedSequence.push_back(")");

            std::vector<std::pair<Blocks::Line,int>> params;
            parseParams(modifiedSequence, 0, params);

            for (const auto& param: params)
                parseStatement(param.second, param.first, 0, param.first.size());

            const auto marshalIndex = static_cast<int>(Marshals.find("tally")->second);
            middle.emplace_back(
                MiddleOp_e::marshal,
                marshalIndex,
                static_cast<int>(params.size()),
                lastDebug.line,
                0);
        }

        void processBlocks()
        {
            auto currentIdx = 0;

            const std::unordered_set<std::string> conditionBlock = {
                "if",
                "for",
                "each_row"
            };

            while (currentIdx < blocks.blockCount)
            {
                const auto& block = blocks.blocks.find(currentIdx)->second;

                if (block.blockId)
                    middle.emplace_back(
                        MiddleOp_e::block,
                        static_cast<int>(block.blockId),
                        Blocks::Line{},
                        -1);

                for (const auto& line : block.lines)
                {
                    const auto& words = line.words;
                    lastDebug.set(words);

                    // push row data to the accumulator
                    if (words[0] == "<<")
                    {
                        parseTally(words);
                        continue;
                    }

                    // is this a condition/loop/search?
                    if (conditionBlock.count(words[0]))
                    {
                        // id of nested block
                        const auto blockId = line.codeBlock;
                        parseCondition(blockId, words);
                        continue;
                    }

                    // is this an assignment?
                    if (const auto eqPos = seek("=", words, 0); eqPos != -1)
                    {
                        if (eqPos == static_cast<int>(words.size()))
                        {
                            throw QueryParse2Error_s {
                                errors::errorClass_e::parse,
                                errors::errorCode_e::syntax_error,
                                "expecting right side value after '=' during assignment",
                                lastDebug
                            };
                        }

                        parseStatement(0, words, eqPos + 1);
                        parseItem(words, 0, line.words, true);
                        continue;
                    }

                    if (words.size())
                        parseStatement(0, words, 0);

                }

                if (block.blockId == 0)
                {
                    // force a `false` onto the stack as a default if none specified before term
                    middle.emplace_back(MiddleOp_e::push_false, lastDebug.line, 0);
                    middle.emplace_back(MiddleOp_e::term, Blocks::Line{}, -1);
                }
                else
                {
                    middle.emplace_back(MiddleOp_e::ret, Blocks::Line{}, -1);
                }

                ++currentIdx;
            }
        }

        void initialParse(const std::string& query)
        {
            auto tokens = parseRawQuery(query);
            extractBlocks(tokens);
            processBlocks();
        }

        void addDefaults()
        {
            // default block type - we want `if` rules
            currentBlockType.push_back("if");

            // these columns are always selected, so we add them by default
            columnIndex("stamp");
            columnIndex("event");

            // default filter is set for row searching with no limiters
            const Filter_s filter;
            filters.push_back(filter);
        }

        void compile(Macro_s& inMacros)
        {

            auto& finCode = inMacros.code;
            auto& lambdas = inMacros.lambdas;

            lambdas.push_back(0); // index zero, instruction index is zero

            int filter = 0;

            for (auto& midOp : middle)
            {
                Debug_s debug;
                debug.text = midOp.debug.debugLine;
                debug.translation = midOp.debug.cursor;

                switch (midOp.op)
                {
                case MiddleOp_e::push_user:
                    if (!isAssignedUserVar(userVars[midOp.value1.getInt64()]))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "variable: '" + userVars[midOp.value1.getInt64()] + "' is used but never assigned a value",
                            lastDebug
                        };

                    finCode.emplace_back(
                        OpCode_e::PSHUSRVAR,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_user_ref:
                    if (!isAssignedUserVar(userVars[midOp.value1.getInt64()]))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "variable: '" + userVars[midOp.value1.getInt64()] + "' is used but never assigned a value",
                            lastDebug
                        };

                    finCode.emplace_back(
                        OpCode_e::PSHUSRVREF,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::pop_user_ref:
                case MiddleOp_e::pop_user_obj_ref:
                    if (!isAssignedUserVar(userVars[midOp.value1.getInt64()]))
                        throw QueryParse2Error_s {
                            errors::errorClass_e::parse,
                            errors::errorCode_e::syntax_error,
                            "variable: '" + userVars[midOp.value1.getInt64()] + "' popref should never be called.",
                            lastDebug
                        };
                    break;

                case MiddleOp_e::push_literal:
                    finCode.emplace_back(
                        OpCode_e::PSHLITSTR,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_int:
                    finCode.emplace_back(
                        OpCode_e::PSHLITINT,
                        0,
                        midOp.value1, // integer value
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_double:
                    finCode.emplace_back(
                        OpCode_e::PSHLITFLT,
                        0,
                        static_cast<int64_t>(midOp.value1 * 1'000'000.0), // float value
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_bool:
                    finCode.emplace_back(
                        midOp.value1.getBool() ? OpCode_e::PSHLITTRUE : OpCode_e::PSHLITFALSE,
                        0,
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_true:
                    finCode.emplace_back(
                        OpCode_e::PSHLITTRUE,
                        0,
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_false:
                    finCode.emplace_back(
                        OpCode_e::PSHLITFALSE,
                        0,
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_nil:
                    finCode.emplace_back(
                        OpCode_e::PSHLITNUL,
                        0,
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_column:
                    finCode.emplace_back(
                        filter == 0 ? OpCode_e::PSHTBLCOL : OpCode_e::PSHTBLFLT,
                        midOp.value1, // index of user var
                        filter,
                        NONE,
                        debug);
                    break;

                case MiddleOp_e::pop_user_var:
                    finCode.emplace_back(
                        OpCode_e::POPUSRVAR,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
                    break;

                case MiddleOp_e::push_user_obj_ref:
                    finCode.emplace_back(
                        OpCode_e::PSHUSROREF,
                        midOp.value1, // index of user var
                        0,
                        midOp.value2,
                        debug);
                    break;

                case MiddleOp_e::push_user_obj:
                    finCode.emplace_back(
                        OpCode_e::PSHUSROBJ,
                        midOp.value1, // index of user var
                        0,
                        midOp.value2,
                        debug);
                    break;

                case MiddleOp_e::pop_user_obj:
                    finCode.emplace_back(
                        OpCode_e::POPUSROBJ,
                        midOp.value1, // index of user var
                        0,
                        midOp.value2,
                        debug);
                    break;

                case MiddleOp_e::eq:
                    finCode.emplace_back(OpCode_e::OPEQ, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::neq:
                    finCode.emplace_back(OpCode_e::OPNEQ, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::gt:
                    finCode.emplace_back(OpCode_e::OPGT, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::gte:
                    finCode.emplace_back(OpCode_e::OPGTE, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::lt:
                    finCode.emplace_back(OpCode_e::OPLT, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::lte:
                    finCode.emplace_back(OpCode_e::OPLTE, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::in:
                    finCode.emplace_back(OpCode_e::OPIN, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::contains:
                    finCode.emplace_back(OpCode_e::OPCONT, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::any:
                    finCode.emplace_back(OpCode_e::OPANY, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::op_and:
                    finCode.emplace_back(OpCode_e::LGCAND, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::op_or:
                    finCode.emplace_back(OpCode_e::LGCOR, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::add:
                    finCode.emplace_back(OpCode_e::MATHADD, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::sub:
                    finCode.emplace_back(OpCode_e::MATHSUB, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::mul:
                    finCode.emplace_back(OpCode_e::MATHMUL, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::div:
                    finCode.emplace_back(OpCode_e::MATHDIV, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::block:
                    lambdas.push_back(static_cast<int>(finCode.size()));
                    finCode.emplace_back(OpCode_e::LAMBDA, midOp.value1, 0, 0, debug);
                    break;

                case MiddleOp_e::ret:
                    finCode.emplace_back(OpCode_e::RETURN, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::term:
                    finCode.emplace_back(OpCode_e::TERM, 0, 0, 0, debug);
                    break;

                case MiddleOp_e::marshal:
                    finCode.emplace_back(
                        OpCode_e::MARSHAL,
                        midOp.value1, // index of function
                        0,
                        midOp.value2, // number of params to function
                        debug);
                    break;

                case MiddleOp_e::logic_filter:
                    filter = midOp.value1;
                    break;

                case MiddleOp_e::column_filter:
                    filter = midOp.value1;
                    break;

                case MiddleOp_e::if_call:
                    finCode.emplace_back(
                        OpCode_e::CALL_IF,
                        midOp.value1, // code block if lambda is true
                        filter,       // filter ID
                        midOp.value2, // lambda for logic
                        debug);
                    break;

                case MiddleOp_e::for_call:
                    finCode.emplace_back(
                        OpCode_e::CALL_FOR,
                        midOp.value1, // code block if lambda is true
                        0,       // filter ID
                        0, // lambda for logic
                        debug);
                    break;

                case MiddleOp_e::each_call:
                    finCode.emplace_back(
                        OpCode_e::CALL_EACH,
                        midOp.value1, // code block if lambda is true
                        filter,       // filter ID
                        midOp.value2, // lambda for logic
                        debug);
                    break;

                default:
                    throw QueryParse2Error_s {
                        errors::errorClass_e::parse,
                        errors::errorCode_e::syntax_error,
                        "unknown middle language op code",
                        lastDebug
                    };
                }
            }

            // add user vars
            //Tracking stringLiterals;
            //Tracking columns;
            //Tracking aggregates;

            auto index = 0;
            for (auto& v : columns)
            {
                const auto schemaInfo = tableColumns->getColumn(v);

                if (v == "session"s)
                    inMacros.sessionColumn = index;

                inMacros.vars.tableVars.push_back(Variable_s{v, ""});
                inMacros.vars.tableVars.back().index = index;
                inMacros.vars.tableVars.back().column = index;
                inMacros.vars.tableVars.back().actual = v;
                inMacros.vars.tableVars.back().isSet = schemaInfo->isSet;
                inMacros.vars.tableVars.back().sortOrder = schemaInfo->idx;
                inMacros.vars.tableVars.back().schemaColumn = schemaInfo->idx;
                inMacros.vars.tableVars.back().schemaType = schemaInfo->type;
                ++index;
            }

            index = 0;
            for (auto& v : userVars)
            {
                inMacros.vars.userVars.push_back(Variable_s{v, ""});
                inMacros.vars.userVars.back().index = index;

                if (v == "globals")
                    inMacros.useGlobals = true;

                if (v == "props")
                  inMacros.useProps = true;

                ++index;
            }

            index = 0;
            for (auto& v : stringLiterals)
            {
                TextLiteral_s literal;
                literal.hashValue = MakeHash(v);
                literal.index = index;
                literal.value = v;
                inMacros.vars.literals.emplace_back(literal);
                ++index;
            }

            inMacros.vars.columnVars = selectColumnInfo;

            inMacros.filters = filters;
        }

        bool processLogic()
        {

            bool countable = true;

            // convert .row, .ever, .never and remove function calls and user vars from logic
            Blocks::Line tokensUnchained;
            {
                auto& tokens = indexLogic;
                auto idx = 0;
                auto end = static_cast<int>(tokens.size());

                while (idx < end)
                {
                    auto& token = tokens[idx];
                    auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];

                    if (isTextual(token))
                    {
                        if (Operators.count(token))
                        {
                            tokensUnchained.emplace_back(token);
                        }
                        else if (token == "session") // can't index at current - computed at querytime
                        {
                            tokensUnchained.emplace_back("VOID");
                        }
                        else if (isTableColumn(token))
                        {
                            tokensUnchained.emplace_back(token);

                            ++idx;

                            while (tokens[idx].find("__chain_") != string::npos)
                            {
                                if (tokens[idx] == "__chain_row" ||
                                    tokens[idx] == "__chain_ever" ||
                                    tokens[idx] == "__chain_never")
                                {
                                    const auto isRow = tokens[idx] == "__chain_row";
                                    const auto isNever = tokens[idx] == "__chain_never";

                                    const auto endOfLogic = seekMatchingBrace(tokens, idx + 1);

                                    if (isRow)
                                        countable = false;

                                    if (isNever)
                                        tokens[idx + 2] = "[!=]";
                                    else if (tokens[idx + 2] == "==")
                                        tokens[idx + 2] = "[==]";

                                    if (!isRow)
                                        tokensUnchained.insert(tokensUnchained.end(), tokens.begin() + idx + 2, tokens.begin() + endOfLogic);

                                    idx = endOfLogic;
                                }
                                else
                                {
                                    idx = seekMatchingBrace(tokens, idx + 1);
                                }

                                ++idx;
                            }

                            continue;

                        }
                        else if (isMarshal(token))
                        {
                            tokensUnchained.emplace_back("VOID");
                            if (nextToken == "(")
                                idx = seekMatchingBrace(tokens, idx);
                        }
                        else if (isUserVar(token))
                        {
                            tokensUnchained.emplace_back("VOID");

                            ++idx;
                            token = idx >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx];

                            // skip subscripts
                            while (token == "[")
                            {
                                idx = seekMatchingSquare(tokens, idx) + 1;
                                token = idx >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx];
                            }

                            continue;
                        }
                        else
                        {
                            // THROW ??
                        }
                    }
                    else
                    {
                        tokensUnchained.emplace_back(std::move(token));
                    }

                    ++idx;
                }
            }

            // expand lists involved with `in`, `contains` and `any` - turn them into ORs
            Blocks::Line tokensExpanded;
            {
                auto& tokens = tokensUnchained;
                auto idx = 0;
                auto end = static_cast<int>(tokens.size());

                while (idx < end)
                {
                    auto& token = tokens[idx];
                    auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];

                    if (token == "in" || token == "contains" || token == "any")
                        token = "==";

                    // convert lists into ORs if left or right side is not a void
                    if (token == "[")
                    {
                        auto endIdx = seekMatchingSquare(tokens, idx);
                        Blocks::Line extraction(tokens.begin() + idx, tokens.begin() + endIdx + 1);

                        // we are going to use the function param parser to capture the array elements
                        // so we must make this array look like a param list
                        extraction.front() = "(";
                        extraction.back() = ")";
                        std::vector<std::pair<Blocks::Line,int>> params;
                        parseParams(extraction, 0, params);

                        const auto before = idx - 1 < 0 ? std::string() : tokens[idx - 1];
                        const auto after = endIdx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[endIdx + 1];

                        if (Operators.count(before) || Operators.count(after))
                        {
                            const auto isBefore = Operators.count(before) != 0;

                            auto op = isBefore ? before : after;
                            auto tableColumn = isBefore ? tokens[idx - 2] : tokens[endIdx + 2];

                            // convert these to == tests - which in the index are inclusion tests
                            if (op == "in" || op == "contains" || op == "any")
                                op = "==";

                            Blocks::Line ors;

                            ors.push_back("(");

                            auto pushCount = 0;

                            for (auto& param: params)
                            {
                                // should be one value - can't see a scenario where it isn't
                                if (param.first.size() != 1)
                                    continue;

                                auto value = param.first[0];

                                // we are only interested in strings and numbers here, stuff that's in the index
                                if (!isNumeric(value) && !isString(value))
                                    continue;

                                if (ors.size() > 1)
                                    ors.push_back("||");
                                ors.insert(ors.end(), {tableColumn, op, value});
                                ++pushCount;
                            }

                            if (!pushCount)
                                ors.push_back("VOID");

                            ors.push_back(")");

                            if (isBefore)
                                tokensExpanded.erase(tokensExpanded.end() - 2, tokensExpanded.end());

                            tokensExpanded.insert(tokensExpanded.end(), ors.begin(), ors.end());

                            idx = endIdx + (isBefore ? 1 : 3);
                        }
                        else
                        {
                            tokensExpanded.push_back("VOID");
                        }

                        continue;
                    }

                    tokensExpanded.push_back(token);

                    ++idx;
                }
            }

            // remove math from logic
            Blocks::Line tokensWithoutMath;
            {
                auto& tokens = tokensExpanded;
                auto idx = 0;
                auto end = static_cast<int>(tokens.size());

                while (idx < end)
                {
                    auto& token = tokens[idx];
                    auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];
                    auto prevToken = idx < 1 ? std::string() : tokens[idx - 1];

                    if (Math.count(token))
                    {
                        if (isNumeric(prevToken))
                            tokensWithoutMath.back() = "VOID";

                        tokensWithoutMath.emplace_back("VOID");

                        if (isNumeric(nextToken))
                        {
                            tokensWithoutMath.emplace_back("VOID");
                            ++idx;
                        }
                    }
                    else
                    {
                        tokensWithoutMath.emplace_back(token);
                    }

                    ++idx;
                }
            }

            // swap logic so table columns are on the left and values are on the right
            // and strip out VOID == VOID type occurences.
            {
                auto& tokens = tokensWithoutMath;
                auto idx = 0;
                auto end = static_cast<int>(tokens.size());

                while (idx < end)
                {
                    auto& token = tokens[idx];
                    auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];
                    auto prevToken = idx < 1 ? std::string() : tokens[idx - 1];

                    if (Operators.count(token))
                    {
                        if (nextToken == "VOID" || prevToken == "VOID")
                        {
                            countable = false;
                            tokens[idx-1] = "";
                            tokens[idx]   = "";
                            tokens[idx+1] = "";
                        }
                        else if (isTableColumn(nextToken) && (isNumeric(prevToken) || isString(prevToken)))
                        {
                            tokens[idx-1] = nextToken;
                            tokens[idx+1] = prevToken;
                        }
                    }

                    ++idx;
                }
            }

            // remove all "VOIDS" and blanks and collapse
            Blocks::Line tokensVoidCleaned;
            {
                // REMOVE VOIDS
                auto& tokens = tokensWithoutMath;
                auto idx = 0;
                auto end = static_cast<int>(tokens.size());

                std::string last = "";

                while (idx < end)
                {
                    auto& token = tokens[idx];
                    auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];
                    auto prevToken = idx < 1 ? std::string() : tokens[idx - 1];

                    if (token != "" && token != "VOID")
                    {
                        tokensVoidCleaned.emplace_back(token);
                        last = token;
                    }
                    else
                    {
                        countable = false;
                    }

                    ++idx;
                }
            }

            // remove redundant logic and brackets
            Blocks::Line tokensFinalClean = tokensVoidCleaned;
            {
                while (true)
                {
                    auto stripped = false;
                    // REMOVE VOIDS
                    auto& tokens = tokensFinalClean;
                    auto idx = 0;
                    auto end = static_cast<int>(tokens.size());

                    Blocks::Line output;

                    std::string last = "";

                    while (idx < end)
                    {
                        auto& token = tokens[idx];
                        auto nextToken = idx + 1 >= static_cast<int>(tokens.size()) ? std::string() : tokens[idx + 1];
                        auto prevToken = idx < 1 ? std::string() : tokens[idx - 1];

                        if ((prevToken == "" || nextToken == "") && LogicalOperators.count(token))
                        {
                            stripped = true;
                        }
                        else if (token == "(" && nextToken == ")")
                        {
                            stripped = true;
                            ++idx;
                        }
                        else if (LogicalOperators.count(token) && prevToken == token)
                        {
                            stripped = true;
                        }
                        else if (
                            (Operators.count(token) || LogicalOperators.count(token)) &&
                            (nextToken == ")" || prevToken == "(")
                            )
                        {
                            stripped = true;
                        }
                        // look for stranded values
                        else if (!isTableColumn(token) && prevToken == "(" && nextToken == ")")
                        {
                            stripped = true;
                        }
                        // look for columns with no condition
                        else if (
                            isTableColumn(token) &&
                            (
                                (LogicalOperators.count(prevToken) || prevToken == "(") &&
                                (LogicalOperators.count(nextToken) || nextToken == ")")
                            )
                        )
                        {
                            // once a column has been stripped to down to a standalone column
                            // with no conditions we simply test for presence of the column (!= nil)
                            stripped = true;
                            output.emplace_back(token);
                            output.emplace_back("!=");
                            output.emplace_back("nil");
                        }
                        else if (isTableColumn(token) && nextToken == "!=")
                        {
                            // if it isn't a not_equal from an ever/never (which was changed to `[!=]`)
                            // change this for presence checking (ever != nil)
                            output.emplace_back(token);
                            tokens[idx + 2] = "nil";
                        }
                        else
                        {
                            output.emplace_back(token);
                        }

                        ++idx;
                    }

                    tokensFinalClean = std::move(output);

                    if (!stripped)
                        break;
                }
            }

            indexLogic = tokensFinalClean;

            return countable;
        }


        int parseIndex(HintOpList& index, const Blocks::Line& words, int start, int end = -1)
        {
            const std::unordered_set<std::string> operatorWords = {
                "&&",
                "||",
            };

            const std::unordered_set<std::string> logicWords = {
                "==",
                "!=",
                "[==]",
                "[!=]",
                ">",
                "<",
                "<=",
                ">=",
            };

            const auto pushValue = [&](const std::string& value)
            {
                if (isTableColumn(value))
                    index.emplace_back(HintOp_e::PUSH_TBL, value);
                else if (isNil(value))
                    index.emplace_back(HintOp_e::PUSH_VAL, NONE);
                else if (isBool(value))
                    index.emplace_back(HintOp_e::PUSH_VAL, (value == "false" || value == "False") ? 0 : 1);
                else if (isString(value))
                    index.emplace_back(HintOp_e::PUSH_VAL, stripQuotes(value));
                else if (isFloat(value))
                    index.emplace_back(HintOp_e::PUSH_VAL, std::stof(value));
                else
                    index.emplace_back(HintOp_e::PUSH_VAL, static_cast<int64_t>(std::stoll(value)));
            };

            if (end == -1)
                end = static_cast<int>(words.size());
            auto idx = start;

            std::vector<std::string> ops;

            while (idx < end)
            {
                const auto token = words[idx];

                const auto nextToken = idx + 1 >= static_cast<int>(words.size()) ? std::string() : words[idx + 1];
                const auto prevToken = idx == 0 ? std::string() : words[idx - 1];

                // check for function call that is not a marshal and THROW
                if (token == ")")
                {
                    ++idx;
                    continue;
                }

                if (token == "(")
                {
                    const auto subEnd = seekMatchingBrace(words, idx, end);
                    idx = parseIndex(index, words, idx + 1, subEnd);
                    continue;
                }

                if (!operatorWords.count(token) && !logicWords.count(token))
                {
                    pushValue(token);
                    ++idx;
                    continue;
                }

                if (operatorWords.count(token))
                {
                    ops.emplace_back(token);
                    ++idx;
                    continue;
                }

                // if this is an equality/inequality test we push the test immediately to leave
                // a true/false on the stack
                if (logicWords.count(token))
                {
                    if (nextToken.length())
                    {
                        const auto beforeIdx = idx;

                        if (nextToken == "(")
                        {
                            const auto subEnd = seekMatchingBrace(words, idx + 1, end) + 1;
                            idx = parseIndex(index, words, idx + 1, subEnd) + 1;
                        }
                        else
                        {
                            pushValue(nextToken);
                        idx += 2;

                        }

                        index.emplace_back(OpToHintOp.find(token)->second);
                    }
                    else
                    {
                        // THROW??
                    }
                    continue;
                }
                ++idx;
            }

            // push any accumulated logical or math operators onto the stack in reverse
            std::for_each(ops.rbegin(), ops.rend(), [&](auto op) {
               index.emplace_back(OpToHintOp.find(op)->second);
            });

            return idx + 1;
        }

        void compileIndex(Macro_s& inMacros)
        {
            for (const auto &word: indexLogic)
                inMacros.capturedIndex += word + " ";

            // would the count from the segment rules result in the same person count if you actually
            // ran the query (used in segmentation)
            inMacros.indexIsCountable = processLogic();
            parseIndex(inMacros.index, indexLogic, 0);

            inMacros.indexes.emplace_back("_", inMacros.index);

            for (const auto &word: indexLogic)
                inMacros.rawIndex += word + " ";
        }

        bool compileQuery(const std::string& query, openset::db::Columns* columnsPtr, Macro_s& inMacros, ParamVars* templateVars)
        {

            try
            {

                tableColumns = columnsPtr;

                addDefaults();

                rawScript = query;
                inMacros.rawScript = rawScript;

                initialParse(query);


                if (!selectColumnInfo.size())
                {
                    const auto columnName = "id";
                    const auto columnIdx = columnIndex(columnName);
                    const auto selectIdx = selectsIndex(columnName);

                    Variable_s var(columnName, columnName, "column", Modifiers_e::count);
                    var.distinctColumnName = columnName;
                    var.index = selectIdx; // index in variable array
                    var.column = columnIdx; // index in grid
                    var.schemaColumn = tableColumns->getColumn(columnName)->idx;

                    selectColumnInfo.push_back(var);
                }

                compile(inMacros);
                compileIndex(inMacros);

                return true;
            }
            catch (const QueryParse2Error_s& ex)
            {
                error.set(
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    std::string{ ex.getMessage()}+"(0)",
                    ex.getDetail()
                );
                return false;
            }
            catch (const std::exception& ex)
            {
                std::string additional = "";
                if (lastDebug.debugLine.length())
                    additional = lastDebug.debugLine;

                error.set(
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    std::string{ ex.what() }+"(1)",
                    additional
                );
                return false;
            }
            catch (const std::runtime_error& ex)
            {
                std::string additional = "";
                if (lastDebug.debugLine.length())
                    additional = lastDebug.debugLine;

                error.set(
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    std::string { ex.what() } + "(2)",
                    additional);
                return false;
            }
            catch (...) // giant WTF runtime exception
            {
                std::string additional = "";
                if (lastDebug.debugLine.length())
                    additional = lastDebug.debugLine;

                error.set(
                    errors::errorClass_e::parse,
                    errors::errorCode_e::syntax_error,
                    "unknown exception in parser (3)",
                    additional);
                return false;
            }
        }

        static SectionDefinitionList extractSections(const char* query, const Debugger_s lastDebug = {})
        {
            vector<SectionDefinition_s> result;
            vector<string> accumulatedLines;
            string current;
            string functionName;
            auto c         = query; // cursor
            const auto end = query + strlen(query) + 1;
            cvar params(cvar::valueType::DICT);
            cvar flags(cvar::valueType::DICT);
            std::string sectionType;
            std::string sectionName;
            const auto storeSection = [&]()
            {
                string code;
                for (auto& s : accumulatedLines)
                    code += s + '\n';
                code += '\n';

                // this allows you to indent the code under the @section if preferred
                accumulatedLines.clear();
                result.emplace_back(
                    SectionDefinition_s {
                        sectionType,
                        sectionName,
                        flags,
                        params,
                        code
                    });
                sectionName = "";
                sectionType = "";
                params.dict(); // clear them
                flags.dict();
            };
            while (c < end)
            {
                switch (*c)
                {
                case '\r':
                    break;
                case '\t':
                    current += "    "; // convert tab to 4 spaces
                    break;
                case 0: case '\n':
                {
                    auto tabDepth = 0; // count our spaces
                    for (const auto s : current)
                    {
                        if (s == ' ')
                            ++tabDepth;
                        else
                            break;
                    }                             // convert spaces to tab counts (4 spaces
                    tabDepth /= 4;                // remove leading spaces (or any other characters we don't want)
                    current = trim(current, " "); // if this line isn't empty, and isn't a comment
                    // store it
                    if (current.length() && current[0] != '#')
                    {
                        // add the tabs back in
                        if (tabDepth) // FIX
                            for (auto i = 0; i < tabDepth; ++i)
                                current = "    " + current;
                        if (current[0] == '@')
                        {
                            if (sectionName.length())
                                storeSection();
                            auto sectionParts = split(current.substr(1), ' ');
                            if (sectionParts.size() >= 2)
                            {
                                sectionType = sectionParts[0];
                                sectionName = sectionParts[1];
                                for (auto idx = 2; idx < static_cast<int>(sectionParts.size()); ++idx)
                                {
                                    auto keyVal = split(sectionParts[idx], '=');

                                    if (keyVal.size() == 1)
                                        keyVal.emplace_back("True");

                                    if (keyVal[0] == "ttl" || keyVal[0] == "refresh")
                                        // these are special and allow for time appends like 's' or 'm', or 'd'
                                        flags[keyVal[0]] = expandTime(keyVal[1], lastDebug) * 1000;

                                    else if (keyVal[0] == "use_cached")
                                        flags["use_cached"] = (keyVal[1].length() == 0 || keyVal[1][0] == 'T' || keyVal[1][0] ==
                                            't');

                                    else if (keyVal[0] == "on_insert")
                                        flags["on_insert"] = (keyVal[1].length() == 0 || keyVal[1][0] == 'T' || keyVal[1][0] ==
                                            't');

                                    else if (keyVal[0] == "z_index")
                                        flags["z_index"] = stoll(keyVal[1]);

                                    else if (isFloat(keyVal[1]))
                                        params[keyVal[0]] = stod(keyVal[1]);
                                    else if (isNumeric(keyVal[1]))
                                        params[keyVal[0]] = stoll(keyVal[1]);
                                    else if (isBool(keyVal[1]))
                                        params[keyVal[0]] = (keyVal[1] == "True" || keyVal[1] == "true");
                                    else
                                        params[keyVal[0]] = stripQuotes(keyVal[1]);
                                }
                            }
                        }
                        else
                        {
                            accumulatedLines.emplace_back(current);
                        }
                    }

                    // reset the line accumulator
                    current.clear();
                }
                    break;
                default:
                    current += *c;
                }
                ++c;
            }
            if (sectionName.length())
                storeSection();
            return result;
        }

    };

    string MacroDbg(Macro_s& macro);

};


