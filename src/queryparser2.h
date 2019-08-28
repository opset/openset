#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include "../lib/str/strtools.h"

#include "queryparser.h"
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

    enum class MiddleOp_e
    {
        push_user,
        push_literal,
        push_int,
        push_double,
        push_bool,
        push_column,
        pop_user,
        eq,
        neq,
        gt,
        gte,
        lt,
        lte,
        and,
        or,
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
    };

    struct Debugger_s
    {
        Blocks::Line line;
        std::string debug;

        Debugger_s() = default;

        void set(Blocks::Line debugLine)            
        {
            line = std::move(debugLine);
            debug = consolidate(line);
        }

    private:
        static std::string consolidate(const Blocks::Line& line)
        {
            std::string result;
            for (const auto &word: line)
                result += word + " ";
            return result;
        }
    };
    
    struct MiddleOp_s
    {
        MiddleOp_e op;
        cvar value1 {LLONG_MIN};
        cvar value2 {LLONG_MIN};
        int filterIndex {-1};
        Debugger_s debug;
               
        MiddleOp_s(const MiddleOp_e op) :
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

        MiddleOp_s(const MiddleOp_e op, const Blocks::Line& line) :
            op(op)
        {
            debug.set(line);
        }

        MiddleOp_s(const MiddleOp_e op, cvar value, const Blocks::Line& line) :
            op(op),
            value1(std::move(value))
        {
            debug.set(line);
        }

        MiddleOp_s(const MiddleOp_e op, cvar value, cvar value2, const Blocks::Line& line) :
            op(op),
            value1(std::move(value)),
            value2(std::move(value2))
        {
            debug.set(line);
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
        { "&&", MiddleOp_e::and },
        { "||", MiddleOp_e::or },
        { "+", MiddleOp_e::add },
        { "-", MiddleOp_e::sub },
        { "*", MiddleOp_e::mul },
        { "/", MiddleOp_e::div },
    };


    class QueryParser2
    {     
    public:

        using MidOps = std::vector<MiddleOp_s>;

        MidOps middle;
        FilterList filters;

        db::Columns* tableColumns { nullptr };
        std::string rawScript;

        Blocks blocks;


        using Tracking = std::vector<std::string>;

        Tracking userVars;
        std::unordered_map<std::string, int> userVarAssignments;

        Tracking stringLiterals;
        Tracking columns;
        Tracking aggregates;

        QueryParser2() = default;
        ~QueryParser2() = default;

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
            return (((value[0] >= '0' && value[0] <= '9') || (value[0] == '-' && value[1] >= '0' && value[1] <= '9')) && value.
                find('.') != string::npos);
        }

        static bool isString(const string& value)
        {
            return (value[0] == '"' || value[0] == '\'');
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

        cvar expandTime(const string& value) const
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
                    // THROW
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

        int aggregatesIndex(const std::string& name)
        {
            if (const auto idx = getTrackingIndex(aggregates, name); idx != -1)
                return idx;
            aggregates.emplace_back(name);
            return static_cast<int>(aggregates.size()-1);
        }

        void incUserVarAssignmentCount(const std::string& name)
        {
            if (const auto iter = userVarAssignments.find(name); iter != userVarAssignments.end())
                ++iter->second;
            userVarAssignments.emplace(name, 1);
        }

        bool isAssignedUserVar(const std::string& name)
        {
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
        static std::vector<std::string> parseRawQuery(const std::string& query)
        {
            std::vector<std::string> accumulated;
            std::string current;

            auto c         = query.c_str(); // cursor
            const auto end = query.c_str() + query.length() + 1;

            while (c < end)
            {

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
                            default: // TODO - throw
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
                if ((c[0] == '!' && c[1] == '=') ||
                    (c[0] == '>' && c[1] == '=') ||
                    (c[0] == '<' && c[1] == '=') || 
                    (c[0] == '+' && c[1] == '=') || 
                    (c[0] == '-' && c[1] == '=') || 
                    (c[0] == '*' && c[1] == '=') || 
                    (c[0] == '/' && c[1] == '=') || 
                    (c[0] == '<' && c[1] == '<') || 
                    (c[0] == '<' && c[1] == '>') || 
                    (c[0] == ':' && c[1] == ':') || 
                    (c[0] == '=' && c[1] == '='))
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
            "for",
            "each",
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

        static bool validNext(std::vector<std::string>&tokens, int offset)
        {
            const std::unordered_set<std::string> forceNewLine = {
                "if",
                "for",
                "end",
                "each",
                "<<"
            };

            const std::unordered_set<std::string> validAfterVarOrNum = {
                "&&",
                "||", 
                "==",
                "!=",
                ">=",
                "<=",
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
            const auto nextToken = offset + 1 >= tokens.size() ? std::string() : tokens[offset + 1];
            const auto prevToken = offset - 1 < 0 ? std::string() : tokens[offset - 1];
            const auto isAfterBracketValid = validAfterClosingBracket.count(nextToken) != 0;

            const auto isItem = isNameOrNumber(token);
            const auto isNextAnItem = isNameOrNumber(nextToken);
            const auto isChain = nextToken.find("__chain_") == 0;

            const int lookBackIndex = lookBack(tokens, offset);
            const auto inChain = token == ")" && lookBackIndex > 0 ? tokens[lookBackIndex-1].find("__chain_") == 0 : false;

            // end means stop
            if (token == "end")
                return false;

            // is it a conditional?
            if (forceNewLine.count(token))
            {
                if (isNameOrNumber(nextToken) || validAfterCondition.count(nextToken))
                    return true;
                return false;
            }

            // closing brackets... 
            if (token == ")" && !isChain && !inChain && !isAfterBracketValid)
                return false;

            // closing brackets... 
            if ((token == "]" || token == "}") && (isNextAnItem || !isAfterBracketValid))
                return false;
            
            if (isChain)
                return true;

            // is the next thing valid following a name or number
            if (isItem && validAfterVarOrNum.count(nextToken))
                return true;

            // is the current thing an operator
            if (validAfterVarOrNum.count(token))
                return true;

            return false;
        }
      
        void __extractBlock(std::vector<std::string>& tokens, Blocks::Block_s* block, int start, int end)
        {

            auto idx = start;

            Blocks::Line line;

            const auto emitLine = [&]()
            {
                if (!line.size())
                    return;
                std::cout << block->blockId << ": ";
                for (const auto &l: line)
                    std::cout << l << " ";
                std::cout << std::endl;                
            };

            while (idx < end)
            {

                if (validNext(tokens,idx))
                {
                    if (tokens[idx].length())
                        line.push_back(tokens[idx]);
                }
                else
                {
                    if (tokens[idx].length())
                        line.push_back(tokens[idx]);

                    emitLine();

                    if (line.size())
                    {

                        auto blockId = -1;

                        // go recursive for sub block
                        if (blockStartWords.count(line[0]))
                        {
                            const auto blockEnd = __blockExtractionSeekEnd(tokens, idx + 1, end);

                            if (blockEnd == -1)
                            {
                                // THROW
                                exit(1);
                            }

                            const auto subBlock = blocks.newBlock();
                            __extractBlock(tokens, subBlock, idx + 1, blockEnd);

                            idx = blockEnd;
                            blockId = subBlock->blockId;
                        }

                        block->lines.emplace_back(line);
                        block->lines.back().codeBlock = blockId;

                    }

                    line.clear();
                }

                ++idx;
            }

            if (line.size())
            {
                emitLine();
                block->lines.emplace_back(line);
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

                if (token == "(")
                    ++count;
                else if (token == ")")
                    --count;

                if (!count && token == seek)
                    return start;              
                ++start;
            }
            return -1;
        }


        static int seekMatchingBrace(const Blocks::Line& words, int start, int end = -1)
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

            // THROW
            return -1;
        }

        static int seekMatchingSquare(const Blocks::Line& words, int start, int end = -1)
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

            // THROW
            return end;
        }

        int __parseLine(const Blocks::Line& words, int start, int end = -1)
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
            };

            const std::unordered_set<std::string> isAnArray = {
                ",",
                "(",
                "=",
                "==",
                "[",
            };

            if (end == -1)
                end = static_cast<int>(words.size());
            auto idx = start;

            std::vector<std::string> ops;

            while (idx < end)
            {
                const auto token = words[idx];
                const auto nextToken = idx + 1 >= words.size() ? std::string() : words[idx + 1];
                const auto prevToken = idx == 0 ? std::string() : words[idx - 1];

                if (isMarshal(token))
                {
                    std::vector<Blocks::Line> params;
                    idx = parseParams(words, idx + 1, params);

                    for (const auto& param: params)
                    {
                        __parseLine(param, 0, param.size());
                    }

                    const auto marshalIndex = static_cast<int>(Marshals.find(token)->second);
                    middle.emplace_back(MiddleOp_e::marshal, marshalIndex, static_cast<int>(params.size()), words);

                    ++idx;
                    continue;
                }

                // check for function call that is not a marshal and THROW

                if (token == ")")
                {
                    ++idx;
                    continue;
                }

                if (token == "(")
                {
                    const auto subEnd = seekMatchingBrace(words, idx, end);
                    if (subEnd == -1)
                    {
                        // THROW
                    }
                    idx = __parseLine(words, idx + 1, subEnd);
                    continue;
                }

                // nested array or accessor?
                // array: `[` is first char, or proceeded by `[`, `==`, `=`, `(` or `,`
                if (token == "[")
                {
                    if (isAnArray.count(prevToken))
                    {
                        idx = parseArray(words, idx);
                    }
                    else
                    {
                        // TODO accessor
                    }
                    ++idx;
                    continue;
                }

                if (!operatorWords.count(token) && !logicWords.count(token))
                {
                    pushItem(token, words);
                    ++idx;
                    continue;
                }

                if (operatorWords.count(token))
                {
                    ops.push_back(token);
                    ++idx;
                    continue;
                }

                // if this is an equality/inequality test we push the test immediately to leave
                // a true/false on the stack
                if (logicWords.count(token))
                {                  
                    if (nextToken.length())
                    {
                        if (nextToken == "(")
                        {
                            const auto subEnd = seekMatchingBrace(words, idx, end) + 1;
                            if (subEnd == -1)
                            {
                                // THROW
                            }
                            idx = __parseLine(words, idx + 1, subEnd) + 1;
                            middle.emplace_back(ConditionToMiddleOp.find(token)->second, words);
                        }
                        else
                        {
                            pushItem(nextToken, words);
                            middle.emplace_back(ConditionToMiddleOp.find(token)->second, words);
                            idx += 2;
                        }
                    }
                    else
                    {
                        // THROW
                    }
                    continue;
                }
                ++idx;
            }

            // push any accumulated logical or math operators onto the stack in reverse
            std::for_each(ops.rbegin(), ops.rend(), [&](auto op) {
               middle.emplace_back(ConditionToMiddleOp.find(op)->second, words);
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

        static int parseParams(const Blocks::Line& words, int start, std::vector<Blocks::Line>& params)
        {
            params.clear();
            std::deque<Blocks::Line> result;
            auto end = static_cast<int>(words.size());
            auto idx = start;

            if (words[idx] != "(")
            {
                // THROW
            }

            end = seekMatchingBrace(words, start);

            if (end == -1)
            {
                // THROW
            }

            ++idx;

            while (idx < end)
            {
                const auto commaPosition = seek(",", words, idx);
                if (commaPosition == -1)
                {
                    auto param = extract(words, idx, end);
                    result.push_front(param);
                    idx = end;
                }
                else
                {
                    auto param = extract(words, idx, commaPosition);
                    result.push_front(param);
                    idx = commaPosition;
                }

                ++idx;
            }

            // push the items into the result in reverse
            for (auto& item : result)
                params.emplace_back(item);

            return idx;
        }

        int parseArray(const Blocks::Line& words, int start)
        {
            std::deque<Blocks::Line> result;
            auto end = static_cast<int>(words.size());
            auto idx = start;

            if (words[idx] != "[")
            {
                // THROW
            }
                        
            end = seekMatchingSquare(words, start);

            if (end == -1)
            {
                // THROW
            }

            ++idx;

            while (idx < end)
            {
                const auto commaPosition = seek(",", words, idx);
                if (commaPosition == -1)
                {
                    auto param = extract(words, idx, end);
                    result.push_front(param);
                    idx = end;
                }
                else
                {
                    auto param = extract(words, idx, commaPosition);
                    result.push_front(param);
                    idx = commaPosition;
                }

                ++idx;
            }

            // push the items into the result in reverse
            for (auto& item : result)
                __parseLine(item, 0);

            middle.emplace_back(
                MiddleOp_e::marshal, 
                static_cast<int>(Marshals_e::marshal_make_list), 
                static_cast<int>(result.size()), 
                words);

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

        void pushItem(const std::string& item, const Blocks::Line& debugLine)
        {
            if (isString(item))
            {
                auto cleanString = stripQuotes(item);
                auto idx = stringLiteralIndex(item);
                middle.emplace_back(MiddleOp_e::push_literal, idx, debugLine);    
            }
            else if (isFloat(item))
            {
                middle.emplace_back(MiddleOp_e::push_double, expandTime(item).getDouble(), debugLine);    
            }
            else if (isNumeric(item))
            {
                middle.emplace_back(MiddleOp_e::push_int, expandTime(item).getInt64(), debugLine);    
            }
            else if (isTableColumn(item))
            {
                auto idx = columnIndex(item);
                middle.emplace_back(MiddleOp_e::push_column, idx, debugLine);    
            }
            else if (isMarshal(item))
            {
                // THROW
            }
            else
            {
                auto idx = userVarIndex(item);
                middle.emplace_back(MiddleOp_e::push_user, idx, debugLine);    
            }
        }

        void popItem(const std::string& item, const Blocks::Line& debugLine)
        {

            if (isString(item) || 
                isFloat(item) ||
                isNumeric(item))
            {
                // THROW
            }

            if (isTableColumn(item))
            {
                // THROW
            }

            if (isMarshal(item))
            {
                // THROW
            }

            userVarIndex(item);
            incUserVarAssignmentCount(item);
            middle.emplace_back(MiddleOp_e::pop_user, item, debugLine);    
        }

        int processLogicChain(const Blocks::Line& words, int start)
        {
            const auto end = words.size();
            auto idx = start;

            Filter_s filter;
            auto count = 0;

            while (idx < end)
            {
                const auto token = words[idx];
                const auto nextToken = idx + 1 >= words.size() ? std::string() : words[idx + 1];

                if (token == "__chain_reverse")
                {
                    std::vector<Blocks::Line> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size())
                    {
                        // THROW
                    }

                    filter.isReverse = true;
                    ++count;
                    ++idx;
                }
                else if (token == "__chain_within")
                {
                    std::vector<Blocks::Line> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 1)
                    {
                        // THROW
                    }

                    // convert our params into code blocks to be called as lambdas
                    filter.withinStartBlock = addLinesAsBlock(params[0]);
                    filter.isWithin = true;

                    ++count;
                }
                else if (token == "__chain_range")
                {
                    std::vector<Blocks::Line> params;
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() != 2)
                    {
                        // THROW
                    }

                    // convert our params into code blocks to be called as lambdas
                    filter.rangeStartBlock = addLinesAsBlock(params[0]);
                    filter.rangeEndBlock = addLinesAsBlock(params[1]);
                    filter.isRange = true;

                    ++count;
                }
                else if (token == "__chain_continue")
                {
                    std::vector<Blocks::Line> params;
                    cout << token << " - ";
                    idx = parseParams(words, idx + 1, params);

                    if (params.size() > 1)
                    {
                        // THROW
                    }

                    if (params.size())
                        filter.continueBlock = addLinesAsBlock(params[0]);
                    filter.isContinue = true;

                    ++count;
                }
                /*else if (token.find("__chain_) == 0)
                {
                    // THROW
                }*/
                else
                {
                    if (count)
                    {
                        // sets active filter in opcode
                        middle.emplace_back(MiddleOp_e::logic_filter, static_cast<int>(filters.size()), words);
                        filters.push_back(filter);
                    }
                    else
                    {
                        // set default filter
                        middle.emplace_back(MiddleOp_e::logic_filter, 0, words);
                    }
                    return idx;
                }
            }

            return idx;
        }

        void parseCondition(int codeBlockId, const Blocks::Line& words)
        {
            const auto condition = words[0];

            Filter_s filter;

            if (condition == "if")
            {
                const auto idx = processLogicChain(words,1);
                const Blocks::Line logic(words.begin() + idx, words.end());
                const auto logicBlockId = addLinesAsBlock(logic);
                middle.emplace_back(MiddleOp_e::if_call, codeBlockId, logicBlockId, words);
            }
            else if (condition == "for")
            {

            }
            else // each
            {
                
            }
        }

        void processBlocks()
        {
            auto currentIdx = 0;

            const std::unordered_set<std::string> conditionBlock = {
                "if",
                //"for",
                //"each",
            };

            while (currentIdx < blocks.blockCount)
            {
                const auto& block = blocks.blocks.find(currentIdx)->second;

                middle.emplace_back(MiddleOp_e::block, static_cast<int>(block.blockId), block.lines.size() ? block.lines[0].words : Blocks::Line{} );

                for (const auto& line : block.lines)
                {
                    const auto& words = line.words;
                    const auto codeBlock = line.codeBlock;

                    // is this a condition/loop/search?
                    if (conditionBlock.count(words[0]))
                    {
                        // id of nested block
                        const auto blockId = line.codeBlock;
                        parseCondition(blockId, words);
                        continue;
                    }

                    // is this an assignment?                    
                    if (const int eqPos = seek("=", words, 0); eqPos != -1)
                    {
                        if (eqPos != 1)
                        {
                            // THROW
                        }

                        if (eqPos == words.size())
                        {
                            // THROW
                        }

                        __parseLine(words, eqPos + 1, words.size());
                        popItem(words[0], line.words);
                        continue;
                    }

                    __parseLine(words, 0, words.size());

                }

                middle.emplace_back(MiddleOp_e::ret);

                ++currentIdx;
            }
        }

        void initialParse(const std::string& query)
        {
            auto tokens = parseRawQuery(query);
            extractBlocks(tokens);
            processBlocks();
            std::cout << "done" << std::endl;
        }

        void addDefaults()
        {
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

            int filter = 0;

            for (auto& midOp : middle)
            {
                Debug_s debug;
                debug.text = midOp.debug.debug;

                switch (midOp.op)
                {
                case MiddleOp_e::push_user:
                    if (isAssignedUserVar(userVars[midOp.value1.getInt64()]))
                    {
                        // THROW
                    }

                    finCode.emplace_back(
                        OpCode_e::PSHUSRVAR,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
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
                        midOp.value1, // float value
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
                case MiddleOp_e::push_column:
                    finCode.emplace_back(
                        OpCode_e::PSHTBLCOL,
                        midOp.value1, // index of user var
                        0,
                        0,
                        debug);
                    break;
                case MiddleOp_e::pop_user:
                    finCode.emplace_back(
                        OpCode_e::POPUSROBJ,
                        midOp.value1, // index of user var
                        0,
                        0,
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
                case MiddleOp_e::and:
                    finCode.emplace_back(OpCode_e::LGCAND, 0, 0, 0, debug);
                    break;
                case MiddleOp_e::or:
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
                    finCode.emplace_back(OpCode_e::NOP, midOp.value1, 0, 0, debug);
                    break;
                case MiddleOp_e::ret:
                    finCode.emplace_back(OpCode_e::RETURN, 0, 0, 0, debug);
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
                        OpCode_e::CNDIF,
                        midOp.value1, // code block if lambda is true
                        filter,       // filter ID
                        midOp.value2, // lambda for logic
                        debug);
                    break;
                default: 
                    // THROW
                    ;
                }
            }
        }

        bool compileQuery(const std::string& query, openset::db::Columns* columnsPtr, Macro_s& inMacros, ParamVars* templateVars)
        {
            tableColumns = columnsPtr;

            addDefaults();

            initialParse(query);
            compile(inMacros);

            const auto debug = MacroDbg(inMacros);

            std::cout << debug << std::endl;

            return true;
        }

    };
};