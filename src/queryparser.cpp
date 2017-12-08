#include "queryparser.h"
#include <unordered_set>
#include "str/strtools.h"
#include "errors.h"

#include <limits>
#include <sstream>
#include <iomanip>
#include <iterator>

using namespace openset::query;

QueryParser::QueryParser(const parseMode_e parseMode) :
	parseMode(parseMode)
{}

QueryParser::~QueryParser()
{}

bool QueryParser::isDigit(const char value)
{
	return (value >= '0' && value <= '9');
}

bool QueryParser::isNumeric(const string value)
{
	return ((value[0] >= '0' && value[0] <= '9') ||
		(value[0] == '-' && value[1] >= '0' && value[1] <= '9'));
}

bool QueryParser::isFloat(const string value)
{
	return ((value[0] >= '0' && value[0] <= '9' ||
		(value[0] == '-' && value[1] >= '0' && value[1] <= '9')) &&
		value.find('.') != string::npos);
}

bool QueryParser::isString(const string value)
{
	return (value[0] == '"' || value[0] == '\'');
}

bool QueryParser::isBool(const string value)
{
    return (value == "True" || value == "true" || value == "False" || value == "false");
}


bool QueryParser::isTextual(const string value)
{
	return ((value[0] >= 'a' && value[0] <= 'z') ||
		(value[0] >= 'A' && value[0] <= 'Z') ||
		value[0] == '_');
}

bool QueryParser::isValue(const string value)
{
	return (isString(value) || isNumeric(value));
}

int64_t QueryParser::expandTime(string value)
{
	int64_t returnValue;

	const auto lastChar = value[value.length() - 1];

	if (lastChar < '0' || lastChar > '9')
	{
		returnValue = stoll(value.substr(0, value.length() - 1));

		// does the number end in s, m, or h
		switch (lastChar)
		{
			case 's':
				break;
			case 'm':
				returnValue *= 60;
				break;
			case 'h':
				returnValue *= 60 * 60;
				break;
			case 'd':
				returnValue *= 60 * 60 * 24;
				break;
			default:
				// TODO throw an error
				break;
		}
	}
	else
	{
		returnValue = stoll(value);
	}

	return returnValue;
}

QueryParser::LineParts QueryParser::breakLine(const string &text) 
{
	LineParts parts; // result

	// lambda to trim, push and clear a word/symbol into the
	// current line
	const auto push = [](string& part, LineParts& partList)
	{
		part = trim(part, " ");
		if (part.length())
			partList.push_back(part);
		part.clear();
	};

	string part;

	auto c = text.c_str(); // cursor
	const auto end = c + text.length();

	while (c < end)
	{
		if (c[0] == '#')
			break;
		if (
			(c[0] == '!' && c[1] == '=') ||
			(c[0] == '+' && c[1] == '=') ||
			(c[0] == '-' && c[1] == '=') ||
			(c[0] == '*' && c[1] == '=') ||
			(c[0] == '/' && c[1] == '=') ||
			(c[0] == '<' && c[1] == '<') ||
			(c[0] == '<' && c[1] == '>') ||
			(c[0] == ':' && c[1] == ':') ||
			(c[0] == '=' && c[1] == '=')
		)
		{
			push(part, parts);
			part += c[0];
			part += c[1];
			++c;
			push(part, parts);
		}
		else if (c[0] == '{' && c[1] == '}')
		{
			push(part, parts);
			auto tStr = "__internal_init_dict"s;
			push(tStr, parts);
			++c;
		}
		else if (c[0] == '[' && c[1] == ']')
		{
			push(part, parts);
			auto tStr = "__internal_init_list"s;
			push(tStr, parts);
			++c;
		}
		else if (c[0] == '-' && isDigit(c[1]))
		{
			part += c[0]; 
		}
		else if (c[0] == '=' || c[0] == '+' || c[0] == '-' ||
			c[0] == '(' || c[0] == ')' ||
			c[0] == '[' || c[0] == ']' ||
			c[0] == '{' || c[0] == '}' ||
			c[0] == ',' ||
			c[0] == '*' || c[0] == '/')
		{
			push(part, parts);
			part += c[0];
			push(part, parts);
		}
		else if (c[0] == ' ')
		{
			push(part, parts);
		}
		else if (c[0] == ':')
		{
			push(part, parts);
			auto tStr = "__MARKER__"s;
			push(tStr, parts);
		}
		else if (*c == '\'' || *c == '\"')
		{
			const auto endChar = *c;
			push(part, parts);

			part += *c;
			++c;

			while (c < end)
			{
				
				if (*c == '\\')
				{
					++c;

					switch (*c)
					{
					case 't':
						part += '\t';
						break;
					case 'r':
						part += '\r';
						break;
					case 'n':
						part += '\n';
						break;
					case '\'':
						part += '\'';
						break;
					case '"':
						part += '"';
					case '\\':
						part += '\\';
					case '/':
						part += '/';
					break;
					default:
						// TODO - throw
						break;
					}

					++c;
					continue;
				}

				part += *c;
				if (*c == endChar)
					break;
				++c;
			}

			push(part, parts);
		}
		else
		{
			part += c[0];
		}

		++c;
	}
	push(part, parts);

	return parts;
}

QueryParser::FirstPass QueryParser::extractLines(const char* query) 
{
	FirstPass result;
	FirstPass_s current;

	auto c = query; // cursor
	const auto end = query + strlen(query);

	auto lineCount = 0;
	auto lastIsContinued = false;

	while (c < end)
	{
		switch (*c)
		{
			case '\r':
				break;

			case '\t':
				current.text += "    "; // convert tab to 4 spaces
				break;

			case 0:
			case 0x1a:
			case '\n':
			{
				++lineCount;
				auto tabDepth = 0;

				// count our spaces
				for (const auto s : current.text)
				{
					if (s == ' ')
						++tabDepth;
					else
						break;
				}

				current.debug.text = current.text;

				if (current.text.size() && current.text.back() == '\\')
				{
					lastIsContinued = true;
				}
				else
				{

					if (!lastIsContinued)
					{
						// ERROR CHECK
						if (cast<int>((tabDepth / 4) * 4) != tabDepth)
							throw ParseFail_s{
								errors::errorClass_e::parse,
								errors::errorCode_e::syntax_indentation,
								"incorrect tab depth (line #" + to_string(lineCount) + ")"
						};

						// convert spaces to tab counts (4 spaces
					}

					lastIsContinued = false;
				}

				tabDepth /= 4;
				// remove leading spaces (or any other characters we don't want)
				current.text = trim(current.text, " ");

				// if this line isn't empty, and isn't a comment
				// store it
				if (current.text.length() && current.text[0] != '#')
				{
					current.indent = tabDepth;
					current.debug.number = lineCount; // line in source

					// caveman text search through line to look for 
					// template variables to replace				
					bool changed;

					do
					{
						changed = false;

						if (templating)
							for (auto k : *templating)
							{
								auto search = "{{" + k.first + "}}";
								size_t idx;
								if ((idx = current.text.find(search)) != current.text.npos)
								{
									changed = true;

									// trim out the variable and the brackets
									current.text.erase(idx, search.length());

									// insert without quotes if this is a column name
									if (isTableColumn(k.second))
										current.text.insert(idx, k.second);
									// if this is a string we insert it with quotes
									else if (k.second.typeof() == cvar::valueType::STR)
										current.text.insert(idx, "'" + k.second + "'");
									// probably an actual number, insert unquoted
									else
										current.text.insert(idx, k.second);
								}
							}
					} while (changed);

					// break line into words
					current.parts = breakLine(current.text);

					result.emplace_back(move(current));
				}

				// reset the line accumulator
				current.clear();
			}
			break;

			default:
				current.text += *c;
		}
		++c;
	}

	result = mergeLines(result);
	lineTranslation(result);

	return result;
}

int64_t QueryParser::extractBlocks(const int indent, FirstPass& lines, BlockList& blockList)
{
	auto blockId = blockCounter;

	FirstPass blocks; // empty collector for lines as we parse

	for (auto i = 0; i < lines.size(); i++)
	{

		lastDebug = lines[i].debug;
		
		if (lines[i].indent > indent)
		{
			if (!blocks.size())
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_indentation,
					"indentation error - check that indentation matches on multi-line statements",
					lines[i].debug 
				};

			FirstPass capture; // place to capture lines
			auto& line = blocks.back();

			for (; i < lines.size(); i++)
			{
				if (lines[i].indent <= indent)
					break;
				capture.push_back(move(lines[i]));
			}

			--i; // back up a line

			if (line.parts[0] == "agg" || line.parts[0] == "aggregate")
			{
				for (auto& c : capture)
				{
					// force format:
					//    modifier name <as> <alias>
					if (!ColumnModifiers.count(c.parts[0]))
						c.parts.insert(c.parts.begin(), isTableColumn(c.parts[0]) ? "count" : "var"); // if in table then value

					auto modifier = ColumnModifiers.find(c.parts[0]);
					if (modifier == ColumnModifiers.end())
						throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::syntax_error,
						"an aggregator function is expected (i.e. var/value/count/sum/min/max/avg)",
						c.debug 
					};

					auto nonDistinct = false;
					auto forceDistinct = false;
					auto lambdaIdx = -1;
					auto lambdaId = -1;

					if (c.parts[1] == "__session")
					{
						useSessions = true;
						// distinct_count_person distinct counts per person, meaning over a query it sums the 
						// distict total for each match for each person, making it only really useful for
						// counting sessions
						modifier = ColumnModifiers.find("dist_count_person");
					}

					auto alias = c.parts[1]; // name				
					auto distinct = std::string{ alias };

					for (auto s = 1; s < c.parts.size(); ++s)
					{
						if (s < c.parts.size() - 1)
						{
							if (c.parts[s] == "as" || c.parts[s] == "AS")
							{
								alias = c.parts[s + 1];
							}
							if (c.parts[s] == "with" || c.parts[s] == "WITH")
							{
								distinct = c.parts[s + 1];
								forceDistinct = true;
							}
						}
						if (c.parts[s] == "all" || c.parts[s] == "ALL")
							nonDistinct = true;
						if (c.parts[s] == "<<")
						{
							lambdaIdx = s;
							break;
						}
					}							
					
					if (lambdaIdx != -1) // found a lambda? Then assign a lambda assignment
					{
						if (modifier->second != Modifiers_e::var)
							throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"lambas can only be used with `var` type aggregaters",
							c.debug
						};

						const auto index = lambdaIdx + 1;
						vector<string> lambdaCapture;
						copy(c.parts.begin() + index, c.parts.end(),
						     back_inserter(lambdaCapture));

						if (!lambdaCapture.size())
							throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"lambda contains no code",
							c.debug
						};

						// this isn't pretty, but, we are making
						// a code block (i.e. nested block, function, lamda, etc).
						// parsing it and injecting it into the block list
						lambdaCapture.insert(lambdaCapture.begin(), {alias, "="});

						lambdaId = vars.columnVars.size();

						FirstPass_s lambda;
						lambda.parts = {"def", "_column_lambda_" + to_string(lambdaId), "(", ")", "__MARKER__"};
						lambda.debug = c.debug;
						lambda.indent = 0;
						lines.push_back(lambda);

						lambda.parts = move(lambdaCapture);
						lambda.debug = c.debug;
						lambda.indent = 1;
						lines.push_back(lambda);
					}

					// are we already counting this? If so alias it with 'as'
					if (vars.columnVars.count(alias))
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::column_already_referenced,
							"column '" + alias + "' already used in 'agg:' try using 'as' to provide an alias.",
							c.debug
						};

					// add this to the select variables
					vars.columnVars.emplace(
						alias, 
						Variable_s{
							c.parts[1],
							alias,
							"column",
							modifier->second,
							cast<int>(vars.columnVars.size())
						});

					if (forceDistinct && 
						!isTableColumn(distinct) && 
						modifier->second != Modifiers_e::var)
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::column_not_in_table,
							"distinction column '" + distinct + "' not in table.",
							c.debug
						};

					if (isTableColumn(distinct))
						vars.tableVars.emplace(
							distinct,
							Variable_s{ distinct, "grid" });

					vars.columnVars[alias].distinctColumnName = distinct;
					vars.columnVars[alias].lambdaIndex = lambdaId;
					vars.columnVars[alias].nonDistinct = nonDistinct;
				}

				blocks.pop_back();
			}
			else if (line.parts[0] == "def")
			{
				++blockCounter;
				line.block = extractBlocks(indent + 1, capture, blockList);

				// find our block
				for (auto b = 0; b < blockList.size(); b++)
				{
					if (blockList[b].blockId == line.block)
					{
						FirstPass_s item;
						item.isFunction = true;
						item.parts = move(line.parts);
						blockList[b].code.insert(blockList[b].code.begin(), item);
						for (auto& c : blockList[b].code)
							c.isFunction = true;
						break;
					}
				}

				line.isFunction = true;

				blocks.pop_back();
			}
			else // just push back our block
			{
				++blockCounter;
				line.block = extractBlocks(indent + 1, capture, blockList);

				if (line.parts.size() >= 1 &&
					(line.parts[0] == "if" ||
						line.parts[0] == "elif" ||
						line.parts[0] == "else"))
				{
					for (auto b = 0; b < blockList.size(); b++)
					{
						if (blockList[b].blockId == line.block)
						{
							for (auto& c : blockList[b].code)
								c.isConditional = true;
							break;
						}
					}
					line.isConditional = true;
				}
			}
		}
		else // indent was the same as current block
		{
			if (lines.size() > i && lines[i].parts.size())
				blocks.push_back(move(lines[i]));
		}
	}

	if (!vars.columnVars.size())
	{
		vars.columnVars.emplace(
			"person", 
			Variable_s{
				"__uuid",
				"person",
				"column",
				Modifiers_e::count,
				cast<int>(vars.columnVars.size())
			});
	}

	if (indent == 0)
		blockList[0].code = blocks;
	else
		blockList.emplace_back(blockId, move(blocks));
	return blockId;
}

QueryParser::BlockList_s* QueryParser::getBlockByID(const int64_t blockId, BlockList& blockList)
{
	for (auto& b : blockList)
		if (b.blockId == blockId)
			return &b;

	return nullptr;
}

int64_t QueryParser::parseConditions(LineParts& conditions, MiddleOpList& opList, int64_t index, Debug_s& debug, const bool stopOnConditions, const string stackOp)
{
	while (index < conditions.size())
	{
		if (conditions[index] == ",")
		{
			break;
		}
		if (conditions[index] == ")" || conditions[index] == "]")
		{
			break;
		}
		if (conditions[index] == "(" || conditions[index] == "[")
		{
			index = parseConditions(
				conditions,
				opList,
				index + 1,
				debug,
				stopOnConditions,
				stackOp);
		}
		// is it a comparative
		else if (Operators.find(conditions[index]) !=
			Operators.end())
		{
			const int newindex = parseConditions(
				conditions,
				opList,
				index + 1,
				debug,
				true);

			opList.emplace_back(
				Operators.find(conditions[index])->second,
				0,
				debug);

			index = newindex;
		}
		else if (Math.find(conditions[index]) !=
			Math.end())
		{
			const int newindex = parseConditions(
				conditions,
				opList,
				index + 1,
				debug,
				true,
				stackOp); // force the stop

			opList.emplace_back(
				Math.find(conditions[index])->second,
				0,
				debug);

			index = newindex;
		}
		else if (LogicalOperators.find(conditions[index]) !=
			LogicalOperators.end())
		{
			if (stopOnConditions)
			{
				index--;
				break;
			}

			const int newIndex = parseConditions(
				conditions,
				opList,
				index + 1,
				debug,
				false,
				conditions[index]);

			// pass in last op for hinting
			// don't stop on conditional (and/or) here!
			opList.emplace_back(
				LogicalOperators.find(conditions[index])->second,
				0,
				debug);

			index = newIndex;
			break;
		}
		else if (conditions[index] == "None")
		{
			opList.emplace_back(
				OpCode_e::PSHLITNUL,
				0,
				debug);
			break;
		}
		else
		{
			// this is a function call
			if (MacroMarshals.count(conditions[index]))
			{
				index = parseCall(conditions, opList, index, debug);
				continue; // because index is already incremented we want to avoid doing so at the bottom of this loop
			}
			else if (index < conditions.size() - 1 &&
				conditions[index + 1] == "(")
			{
				index = parseCall(conditions, opList, index, debug);
			}
			else
			{
				auto value = conditions[index];

				if (isTableColumn(value))
				{
					if (value.find("column.") == 0)
						value = value.substr(value.find('.') + 1);

					opList.emplace_back(
						OpCode_e::PSHTBLCOL,
						value,
						debug);

					if (!isVar(vars.tableVars, value))
					{
						vars.tableVars.emplace(
							value,
							Variable_s{value, "grid"});
					}
				}
				else if (isVar(vars.columnVars, value))
				{
					opList.emplace_back(
						OpCode_e::PSHRESCOL,
						value,
						debug);
				}
				else if (isString(value))
				{
					opList.emplace_back(
						OpCode_e::PSHLITSTR,
						value,
						debug);

					// the index will be set on the final pass
					// we use a map to reduce repeated strings
					// for the string table. Strings are hashed
					// in the final compile.
					vars.literals.emplace(value, -1);
				}
				// is this a number (check for negative)
				else if (isFloat(value))
				{
					double dblValue;
					const auto lastChar = value[value.length() - 1];

					if (lastChar < '0' || lastChar > '9')
					{
						dblValue = strtod(value.substr(0, value.length() - 1).c_str(), nullptr);

						// does the number end in s, m, or h
						switch (lastChar)
						{
							case 's':
								break;
							case 'm':
								dblValue *= 60;
								break;
							case 'h':
								dblValue *= 60 * 60;
								break;
							case 'd':
								dblValue *= 60 * 60 * 24;
								break;
							default:
								// TODO throw an error
								break;
						}
					}
					else
					{
						dblValue = strtod(value.c_str(), nullptr);
					}

					opList.emplace_back(
						OpCode_e::PSHLITFLT,
						cast<int64_t>(dblValue * 1'000'000), // dirty secret, doubles are actually ints multipled for 6 digits
						debug);
				}
				// is this a number (check for negative)
				else if (isNumeric(value))
				{
					int64_t intValue;

					intValue = expandTime(value);

					opList.emplace_back(
						OpCode_e::PSHLITINT,
						intValue,
						debug);
				}
				else if (isTextual(value) || value[0] == '@')
				{
					auto isRef = false;

					if (value[0] == '@')
					{
						value = value.substr(1);
						isRef = true;
					}
			
					if (conditions.size() > index + 1 &&
						conditions[index + 1] == "[")
					{
						vector<MiddleOpList> derefCaptures;

						auto derefEndIdx = index + 1;

						while (derefEndIdx < conditions.size() &&
							conditions[derefEndIdx] == "[")
						{
							MiddleOpList derefOps;

							derefEndIdx = parseConditions(
								conditions,
								derefOps,
								derefEndIdx + 1,
								debug,
								false);

							// TODO - check for zero length

							derefCaptures.emplace_back(derefOps);

							++derefEndIdx;
						}

						--derefEndIdx;

						// insert the deref ops in reverse (stack) order
						for_each(derefCaptures.rbegin(), derefCaptures.rend(), [&opList](auto item)
					         {
						         opList.insert(
							         opList.end(),
							         make_move_iterator(item.begin()),
							         make_move_iterator(item.end()));
					         });

						opList.emplace_back(
							isRef ? OpCode_e::PSHUSROREF : OpCode_e::PSHUSROBJ,
							value,
							debug);

						// params contains the depth of the deref
						opList.back().params = derefCaptures.size();

						index = derefEndIdx;

						if (!isVar(vars.userVars, value))
							vars.userVars.emplace(value, Variable_s{value,""});
					}
					else
					{ // simple variable no deref brackets var[deref]
						if (value == "True")
						{
							opList.emplace_back(
								OpCode_e::PSHLITTRUE,
								value,
								debug);
						}
						else if (value == "False")
						{
							opList.emplace_back(
								OpCode_e::PSHLITFALSE,
								value, 
								debug);
						}
						else
						{
							opList.emplace_back(
								isRef ? OpCode_e::PSHUSRVREF : OpCode_e::PSHUSRVAR,
								value,
								debug);

							if (!isVar(vars.userVars, value))
								vars.userVars.emplace(value, Variable_s{ value,"" });
						}
					}
				}
				else
				{
					throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::syntax_error,
						"general syntax error",
						debug
					};
				}
			}
		}
		++index;
	}

	return index;
}

int64_t QueryParser::parseCall(LineParts& conditions, MiddleOpList& opList, int64_t index, Debug_s& debug)
{
	auto functionName = conditions[index];
	auto params = 0;
	index++;

	// these are function calls that appear like variables, they are read-only
	if (MacroMarshals.count(functionName))
	{
		if (SessionMarshals.count(functionName))
			useSessions = true;

		opList.emplace_back(OpCode_e::CALL, functionName, debug);
		opList.back().params = 0;
		opList.back().deferredStr = "";
		return index;
	}

	if (index >= conditions.size())
	{
		if (SessionMarshals.count(functionName))
			useSessions = true;

		opList.emplace_back(OpCode_e::CALL, functionName, debug);
		opList.back().params = 0;
		return index;
	}
	// if this is a bracketed function call (normal, unlike return for example)
	// move past the bracket
	auto brackets = 0; // nesting counter

	if (conditions[index] == "(")
	{
		brackets = 1;
		index++;
	}
	while (index < conditions.size())
	{
		LineParts capture;


		while (index < conditions.size())
		{
			if (brackets <= 1 && conditions[index] == ",")
				break;

			if (conditions[index] == "@")
			{
				++index;
				continue;
			}

			if (conditions[index] == "(")
				++brackets;

			capture.push_back(conditions[index]);

			if (conditions[index] == ")")
			{
				--brackets;
				if (brackets <= 0)
					break;
			}

			++index;

			if (index >= conditions.size())
				break;
		}

		parseConditions(capture, opList, 0, debug, false);
		++params;

		if (capture.size() == 1 && capture[0] == ")")
			--params; // sometimes we just have the bracket
		
		if (index >= conditions.size())
			break;

		if (conditions[index] == ")")
			break;

		++index;
	}

	if (SessionMarshals.count(functionName))
		useSessions = true;

	opList.emplace_back(OpCode_e::CALL, functionName, debug);
	opList.back().params = params;

	return index;
}

void QueryParser::tokenizeBlock(FirstPass& lines, int blockId, BlockList& blockList, MiddleBlockList& outputBlocks)
{
	MiddleOpList block;

	auto i = 0;

	const auto pushBlock = [&](MiddleOpList& lambdaBlock, int newId, BlockType_e blockType = BlockType_e::code)
	{
		MiddleBlock_s newBlock;
		newBlock.blockId = newId;
		newBlock.code = move(lambdaBlock);
		newBlock.type = blockType;
		outputBlocks.emplace_back(newBlock);
	};

	auto currentBlockType = BlockType_e::code;
	string blockName = "";

	while (i < lines.size())
	{
        if (lines[i].parts[0] == "@flags")
		{					
			

			for (auto x = 1; x < lines[i].parts.size(); ++x)
			{
				// expire a segment after the TTL is up
				if (lines[i].parts[x] == "ttl" && 
					lines[i].parts.size() > x + 2 &&
					lines[i].parts[x + 1] == "=")
				{
					if (isNumeric(lines[i].parts[x + 2]))
						segmentTTL = expandTime(lines[i].parts[x + 2]) * 1000; // milliseconds		
					else if (lines[i].parts[x + 2] == "forever")
						segmentTTL = 0;
					else
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"incorrect TTL format",
							lines[i].debug
						};

					x += 2;
				}

				// auto refresh a segment
				if (lines[i].parts[x] == "refresh" &&
					lines[i].parts.size() > x + 2 &&
					lines[i].parts[x + 1] == "=")
				{
					if (isNumeric(lines[i].parts[x + 2]))
						segmentRefresh = expandTime(lines[i].parts[x + 2]) * 1000; // milliseconds		
					else
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"numeric expected",
							lines[i].debug
						};

					x += 2;
				}

				// use a cached version of the segment if within the TTL
				if (lines[i].parts[x] == "use_cached")
					segmentUseCached = true;

			}

			lines[i].parts.clear();
		}
		else if (lines[i].parts[0] == "def")
		{
			const auto functionName = lines[i].parts[1];

			LineParts varList;

			for (auto x = 3; x < lines[i].parts.size(); x++)
			{
				// functions don't need (), def some_func: is valid in pyql
				if (lines[i].parts[x] == "__MARKER__" ||
					lines[i].parts[x] == ")")
					break;
				varList.push_back(lines[i].parts[x]);
				++x; // skip the comma which will next 
			}

			for (auto v = varList.rbegin(); v != varList.rend(); ++v)
			{
				block.emplace_back(OpCode_e::POPUSRVAR, *v, lines[i].debug);
				block.back().nameSpace = functionName;

				if (!isVar(vars.userVars, *v))
					vars.userVars.emplace(*v, Variable_s{*v, *v, functionName});
			}

			currentBlockType = BlockType_e::function;
			blockName = functionName;
		}
		else if (lines[i].parts[0] == "if" ||
			lines[i].parts[0] == "elif")
		{
			// process conditions get lambda
			++blockCounter;
			auto lambdaId = blockCounter;
			MiddleOpList lambdaBlock;
			parseConditions(lines[i].parts, lambdaBlock, 1, lines[i].debug, false);
			pushBlock(lambdaBlock, lambdaId, BlockType_e::lambda);

			block.emplace_back(
				(lines[i].parts[0] == "if") ? OpCode_e::CNDIF : OpCode_e::CNDELIF,
				lines[i].block, // value is the block ID
				lines[i].debug,
				lambdaId);
		}
		else if (lines[i].parts[0] == "else")
		{
			block.emplace_back(
				OpCode_e::CNDELSE,
				lines[i].block, // value is block ID
				lines[i].debug);
		}
		/*
		patterns:

			match
			match event
			match # events

		*/
		else if (lines[i].parts[0] == "for")
		{
			LineParts left;

			auto idx = 1;

			for (; idx < lines[i].parts.size(); ++idx)
			{
				if (lines[i].parts[idx] == ",")
					continue;

				if (lines[i].parts[idx] == "in")
					break;

				left.push_back(lines[i].parts[idx]);
			}

			// basic parse error check, or did we fail to capture anything?
			if (idx == lines[i].parts.size() ||
				lines[i].parts[idx] != "in" ||
				left.size() == 0 || left.size() > 2)
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"in clause incorrect",
					lines[i].debug
				};

			++idx;

			LineParts right;

			for (; idx < lines[i].parts.size(); ++idx)
			{
				if (lines[i].parts[idx] == "__MARKER__")
					break;

				right.push_back(lines[i].parts[idx]);
			}

			parseConditions(
				right,
				block,
				0,
				lines[i].debug,
				false);

			/* For iterators:
			 *
			 * The left side variables are added to uservars and pushed 
			 * as special usridx ops, which initially contain
			 * the variable name, and on final compile pass get turned
			 * into indexes to those variables
			 * 
			 * Upon compiling the 
			 * 
			 * The right-side (for now) is a stack variable, pushed
			 * before the indexes. Stack:
			 *    right
			 *    left index 2
			 *    left index 1
			 */

			for_each(left.rbegin(), left.rend(), [&](auto item)
		         {
			         vars.userVars.emplace(lines[i].parts[0], Variable_s{lines[i].parts[0], ""});

			         block.emplace_back(
				         OpCode_e::VARIDX, // fancy placeholder, map to index on final pass opcode
				         item,
				         lines[i].debug);
		         });

			block.emplace_back(
				OpCode_e::ITFOR,
				lines[i].block,
				lines[i].debug);

			block.back().params = left.size();
		}
		else if (
			lines[i].parts[0] == "match" ||
			lines[i].parts[0] == "reverse_match")
		{
			// default hintMap is "_", create it, if need be, and get a reference to it.
			if (!hintMap.count("_"))
			{
				hintNames.push_back("_");
				hintMap.emplace("_", LineParts{});
			}

			auto& hints = hintMap["_"];

			if (lines[i].parts.size() == 1)
				lines[i].parts.push_back("where");

			// find where, process conditions get lambda
			auto whereIdx = -1;
			for (auto x = 1; x < lines[i].parts.size(); x++)
			{
				if (lines[i].parts[x] == "where" && lines[i].parts.size() > x + 1)
				{
					whereIdx = x;

					// we don't index `where` when it occurs in if/elif/else blocks
					// or within function calls - we cannot guarantee that indexes built 
					// using blocks in functions or conditionals will not exclude people
					// that should be evaluated
					if (lines[i].isConditional || lines[i].isFunction)
						break;

					if (hints.size())
						hints.push_back("nest_and");

					hints.push_back("(");
					for (auto idx = whereIdx + 1; idx < lines[i].parts.size(); ++idx)
						hints.push_back(lines[i].parts[idx]);
					hints.push_back(")");

					break;
				}
			}

			if (!((lines[i].parts.size() >= 2 && lines[i].parts[1] == "where") ||
				(lines[i].parts.size() >= 3 && lines[i].parts[2] == "where")))
			{
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"match missing where",
					lines[i].debug
				};
			}

			int64_t lambdaId = -1;

			// if there was a "where" in this iterator
			// then lets turn it into an eval lambda
			if (whereIdx >= 1)
			{
				++blockCounter;
				lambdaId = blockCounter;
				MiddleOpList lambdaBlock;

				parseConditions(
					lines[i].parts,
					lambdaBlock,
					whereIdx + 1,
					lines[i].debug,
					false);

				pushBlock(lambdaBlock, lambdaId, BlockType_e::lambda);
			}

			int64_t iterCount = 9999999;

			// is there an iterations count specified
			if (lines[i].parts[1][0] >= '0' &&
				lines[i].parts[1][0] <= '9')
				iterCount = stoll(lines[i].parts[1]);

			block.emplace_back(
				(lines[i].parts[0] == "match") ? OpCode_e::ITNEXT : OpCode_e::ITPREV,
				lines[i].block,
				lines[i].debug,
				lambdaId);

			block.back().params = iterCount;
		}
		// left side variable with indexing
		else if (lines[i].parts.size() >= 2 && lines[i].parts[1] == "[")
		{
			vector<MiddleOpList> derefCaptures;

			auto derefEndIdx = 1;

			while (lines[i].parts[derefEndIdx] == "[")
			{
				MiddleOpList derefOps;

				derefEndIdx = parseConditions(
					lines[i].parts,
					derefOps,
					derefEndIdx + 1,
					lines[i].debug,
					false);

				// TODO - check for zero length

				derefCaptures.emplace_back(derefOps);

				++derefEndIdx;
			}

			// now do everything after the equals sign
			MiddleOpList rightSideOps;

			// RIP Dict

			parseConditions(
				lines[i].parts,
				rightSideOps,
				derefEndIdx + 1,
				lines[i].debug,
				false);

			// insert the right side ops
			block.insert(
				block.end(),
				make_move_iterator(rightSideOps.begin()),
				make_move_iterator(rightSideOps.end()));

			// insert the deref ops
			// insert the deref ops in reverse (stack) order
			for_each(derefCaptures.rbegin(), derefCaptures.rend(), [&block](auto item)
		         {
			         block.insert(
				         block.end(),
				         make_move_iterator(item.begin()),
				         make_move_iterator(item.end()));
		         });

			// the first item on this line will be a user variable (object with deref)
			// we want to pop a value into it

			auto op = OpCode_e::POPUSROBJ;

			// 
			if (lines[i].parts[derefEndIdx] != "=")
			{
				if (MathAssignmentOperators.count(lines[i].parts[derefEndIdx]))
				{
					op = MathAssignmentOperators.find(lines[i].parts[derefEndIdx])->second;
				}
				else
					throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::syntax_error,
						"expecting +=, -=, *= or /-",
						lines[i].debug
					};
			}


			block.emplace_back(
				op,
				lines[i].parts[0],
				lines[i].debug);
			// POPUSROBJ uses params to store the depth of the dereferencing 
			block.back().params = derefCaptures.size();

			vars.userVars.emplace(lines[i].parts[0], Variable_s{lines[i].parts[0], ""});
		}
		else if (lines[i].parts.size() >= 2 &&
			MathAssignmentOperators.count(lines[i].parts[1]))
		{
			MiddleOpList rightSideOps;

			parseConditions(
				lines[i].parts,
				rightSideOps,
				2,
				lines[i].debug,
				false);

			block.insert(
				block.end(),
				make_move_iterator(rightSideOps.begin()),
				make_move_iterator(rightSideOps.end()));

			auto leftSide = lines[i].parts[0]; // clean off the reference marker

			if (!isVar(vars.userVars, leftSide))
				vars.userVars.emplace(leftSide, Variable_s{ leftSide,"" });

			block.emplace_back(
				MathAssignmentOperators.find(lines[i].parts[1])->second,
				leftSide,
				lines[i].debug);
		}
		else if (lines[i].parts.size() >= 2 &&
			(lines[i].parts[1] == "=" || lines[i].parts[1] == "<<"))
		{
			MiddleOpList rightSideOps;

			parseConditions(
				lines[i].parts,
				rightSideOps,
				2,
				lines[i].debug,
				false);

			block.insert(
				block.end(),
				make_move_iterator(rightSideOps.begin()),
				make_move_iterator(rightSideOps.end()));

			// TODO - if we add grid setting, a POPCOL would be needed here

			if (isVar(vars.columnVars, lines[i].parts[0]))
			{
				block.emplace_back(
					OpCode_e::POPRESCOL,
					lines[i].parts[0],
					lines[i].debug);
			}
			else
			{
				block.emplace_back(
					OpCode_e::POPUSRVAR,
					lines[i].parts[0],
					lines[i].debug);

				vars.userVars.emplace(lines[i].parts[0], Variable_s{lines[i].parts[0], ""});
			}
		}
		else if (Marshals.find(lines[i].parts[0]) != Marshals.end())
		{
			MiddleOpList functionCallOps;
			parseCall(
				lines[i].parts,
				functionCallOps,
				0,
				lines[i].debug);

			block.insert(
				block.end(),
				make_move_iterator(functionCallOps.begin()),
				make_move_iterator(functionCallOps.end()));
		}
		else if (lines[i].parts.size() > 1 &&
			(lines[i].parts[1] == "(" && lines[i].parts[0] != "if"))
		{
			MiddleOpList functionCallOps;
			parseCall(
				lines[i].parts,
				functionCallOps,
				0,
				lines[i].debug);

			block.insert(
				block.end(),
				make_move_iterator(functionCallOps.begin()),
				make_move_iterator(functionCallOps.end()));
		}
		else if (lines[i].parts[1] == "[")
		{
			throw ParseFail_s{
				errors::errorClass_e::parse,
				errors::errorCode_e::syntax_error,
				"expecting something (1)",
				lines[i].debug
			};
		}
		else
		{
			if (lines[i].parts[0] != "")
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"syntax error (" + lines[i].parts[0] + ")",
					lines[i].debug
				};
		}

		i++;
	}

	pushBlock(block, blockId, currentBlockType);
	outputBlocks.back().blockName = blockName;
}

bool QueryParser::checkBrackets(QueryParser::LineParts& conditions)
{
	auto curly = 0;
	auto square = 0;
	auto round = 0;

	for (auto &c: conditions)
	{
		if (c == "{")
		{
			++curly;
			continue;
		}
		if (c == "}")
		{
			--curly;
			continue;
		}
		if (c == "[")
		{
			++square;
			continue;
		}
		if (c == "]")
		{
			--square;
			continue;
		}
		if (c == "(")
		{
			++round;
			continue;
		}
		if (c == ")")
		{
			--round;
			continue;
		}
	}

	return (curly == 0 && square == 0 && round == 0);
}

int QueryParser::getMatching(LineParts& conditions, const int index)
{
	auto curly = 0;
	auto square = 0;
	auto round = 0;

	const auto matching = conditions[index];
	auto endIndex = index;

	for (auto c = conditions.begin() + index; c < conditions.end(); ++c, ++endIndex)
	{
		if (*c == "{")
		{
			++curly;
			continue;
		}
		if (*c == "}")
		{
			--curly;

			if (!curly && matching == "{")
				return endIndex;

			continue;
		}
		if (*c == "[")
		{
			++square;
			continue;
		}
		if (*c == "]")
		{
			--square;

			if (!square && matching == "[")
				return endIndex;

			continue;
		}
		if (*c == "(")
		{
			++round;
			continue;
		}
		if (*c == ")")
		{
			--round;
			
			if (!round && matching == "(")
				return endIndex;
			continue;
		}
	}

	return -1;
}

bool QueryParser::isSplice(LineParts& conditions, const int index)
{
	auto curly = 0;
	auto square = 0;
	auto round = 0;

	for (auto c = conditions.begin() + index; c < conditions.end(); ++c)
	{
		if (*c == "{")
		{
			++curly;
			continue;
		}
		if (*c == "}")
		{
			--curly;
			continue;
		}
		if (*c == "[")
		{
			++square;
			continue;
		}
		if (*c == "]")
		{
			--square;
			if (!square)
				return false;
			continue;
		}
		if (*c == "(")
		{
			++round;
			continue;
		}
		if (*c == ")")
		{
			--round;
			continue;
		}
		if (*c == "__MARKER__" && square == 1 && round == 0 && curly == 0)
			return true;
	}

	return false;
}

int QueryParser::search(LineParts& conditions, string value, int startIdx)
{
    if (startIdx >= conditions.size())
        return -1;

    for (; startIdx < conditions.size(); ++startIdx)
        if (conditions[startIdx] == value)
            break;

    return (startIdx == conditions.size()) ? -1 : startIdx;
}

QueryParser::LineParts QueryParser::extractVariable(LineParts& conditions, const int startIdx, int& reinsertIdx)
{
	LineParts result;

	auto bracketComplete = false;
	auto index = startIdx;

	// this variable is not dereferencing a container
	if (index + 1 < conditions.size() &&
		conditions.at(index + 1) != "[")
	{
		bracketComplete = true;
		result.push_back(conditions[index]);
	}
	else
	{
		auto brackets = 0;

		while (index < conditions.size()) // look for ending two brackets
		{
			if (conditions[index] == "[")
				++brackets;
			if (conditions[index] == "]")
				--brackets;

			if ((brackets == 0 && index == conditions.size()-1) ||
				(brackets == 0 && conditions[index + 1] != "["))
			{
				bracketComplete = true;
				break;
			}
			++index;
		}

		++index;

		if (bracketComplete)
		{
			result.insert(
				result.begin(),
				conditions.begin() + startIdx,
				conditions.begin() + index);
		}
	}

	if (!bracketComplete)
	{
		reinsertIdx = -1; // error
		return result;
	}

	conditions.erase(
		conditions.begin() + startIdx,
		conditions.begin() + (startIdx + result.size()));

	reinsertIdx = startIdx;
	
	return result;
}

QueryParser::LineParts QueryParser::extractVariableReverse(LineParts& conditions, const int startIdx, int& reinsertIdx)
{
	LineParts result;

	auto bracketComplete = false;

	auto index = startIdx;

	// this variable is not dereferencing a container
	if (conditions.at(index) != "]")
	{
		bracketComplete = true;
		result.push_back(conditions[index]);
	}
	else
	{
		auto brackets = 0;

		while (index >= 0) // look for ending two brackets
		{
			if (conditions[index] == "[")
				--brackets;
			if (conditions[index] == "]")
				++brackets;

			if ((brackets == 0 && index == 0) ||
				(brackets == 0 && conditions[index - 1] != "]"))
			{
				bracketComplete = true;
				break;
			}
			--index;
		}

		index -= 1; // capture the container name prior to the [

		result.insert(
			result.begin(),
			conditions.begin() + index,
			conditions.begin() + startIdx + 1);

	}

	if (!bracketComplete)
	{
		reinsertIdx = -1; // error
		return result;
	}

	conditions.erase(
		conditions.begin() + index,
		conditions.begin() + startIdx + 1);

	reinsertIdx = index;

	return result;
}

void QueryParser::extractFunction(LineParts& conditions, const int startIdx, int& endIdx)
{
	auto bracketComplete = false;
	auto index = startIdx;

	// this variable is not dereferencing a container
	if (index + 1 < conditions.size() &&
		conditions[index + 1] != "(")
	{
		endIdx = startIdx;
		return;
	}
	else
	{
		auto brackets = 0;

		++index;

		while (index < conditions.size()) // look for ending two brackets
		{
			if (conditions[index] == "(")
				++brackets;
			if (conditions[index] == ")")
				--brackets;

			if (brackets == 0)
			{
				bracketComplete = true;
				break;
			}
			++index;
		}
	}

	if (!bracketComplete)
	{
		endIdx = -1; // error
		return;
	}

	endIdx = index;
}

void QueryParser::extractParam(LineParts& conditions, const int startIdx, int& endIdx)
{
	auto index = startIdx;
	auto brackets = 0;

	while (index < conditions.size()) // look for ending two brackets
	{
		if (conditions[index] == "(" || conditions[index] == "[")
			++brackets;
		if (conditions[index] == ")" || conditions[index] == "]")
			--brackets;

		// found the bracket at the end of a function call?
		if (brackets == -1 && conditions[index] == ")")
		{
			brackets = 0;
			break;
		}
		else if (brackets == 0 && conditions[index] == ",")
			break;

		++index;
	}

	if (brackets != 0)
	{
		endIdx = -1; // error
		return;
	}

	endIdx = index;
}


void QueryParser::lineTranslation(FirstPass& lines) 
{
	auto lineIndex = 0;
	//for (auto& line : lines)
	while (true)
	{
		if (lineIndex == lines.size())
			break;


		auto changes = false;
		auto changeCounter = 0;
		// loop until there are no changes on this line.
		// this allows for changes that are nested.

		do // I never DO these
		{
			auto& line = lines.at(lineIndex);
			auto& conditions = line.parts;
			lastDebug = line.debug;

			auto index = 0;

			if (changes)
				++changeCounter;

			changes = false;

			while (index < conditions.size())
			{
				/* Convert list declarations into function calls
				 *
				 * i.e.
				 *
				 *   some_list = [1, 2, 3, 4, 5]
				 *
				 * becomes:
				 * 
				 *   some_list = __internal_make_list(1, 2, 3, 4, 5)
				 *
				 *  Note: next/whhere in clauses are translated into multiple OR 
				 *        statements so they will be evaulated by the query optimizer
				 */

				if (conditions.size() > index + 1 &&
					conditions[index] == "[" &&
					conditions[0] != "match" && // not next/where
					index > 1 &&
					(conditions[index - 1] == "," ||
						conditions[index - 1] == "=" ||
						conditions[index - 1] == "+" ||
						conditions[index - 1] == "in" ||
						conditions[index - 1] == "notin" ||
						conditions[index - 1] == "-" ||
						conditions[index - 1] == "(" ||
						conditions[index - 1] == "__MARKER__")) 
				{
					auto brackets = 0;
					auto allCounted = false;

					const auto originalIndex = index;

					while (index < conditions.size()) // look for ending two brackets
					{
						if (conditions[index] == "[")
							++brackets;
						if (conditions[index] == "]")
							--brackets;

						if (brackets == 0)
						{
							allCounted = true;
							break;
						}

						++index;
					}

					// note index should be at outer right }

					if (!allCounted)
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"missing closing bracket",
							line.debug
						};

					conditions[originalIndex] = "__internal_make_list";
					conditions[index] = ")";
					conditions.insert(conditions.begin() + originalIndex + 1, "(");

					changes = true;

					break;
				}

				// TRANSLATE NOTIN: convert 'not in' into 'notin'
				if (conditions[index] == "not" &&
					index + 1 < conditions.size() &&
					conditions[index + 1] == "in")
				{
					conditions.erase(conditions.begin() + index + 1);
					conditions[index] = "notin";
					changes = true; // loop again
					break;
				}



				/* Convert aggregators into function calls
				 * containing standard pyql "match" iterators.
				 *
				 *  i.e.
				 *  
				 *     test_sum = sum price where fruit is not 'banana'
				 *
				 *  becomes:
				 *  
				 *     test_sum = __func_agg1()
				 *     
				 *     def __func_agg1():
				 *		   __agg_result = 0
				 *         __agg_saved_iter = iter_get()
				 *         iter_move_first()
				 *         match where fruit is not 'banana':
				 *		      __result += price
				 *		   iter_set(__agg_saved_iter)  
				 *		   return __agg_result
				 *
				 */
				if (conditions[index] == "SUM" ||
					conditions[index] == "AVG" ||
					conditions[index] == "MAX" ||
					conditions[index] == "MIN" ||
					conditions[index] == "COUNT" ||
					conditions[index] == "DISTINCT")
				{

					auto endIdx = 0;
					extractParam(conditions, index, endIdx);

					if (endIdx == -1)
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"error in aggregator",
							line.debug
						};


                    std::string with = "";
                    if (const auto withIdx = search(conditions, "with", index); withIdx != -1)
                    {                       
                        if (withIdx + 1 >= conditions.size())
                        {
                            throw ParseFail_s{
                                errors::errorClass_e::parse,
                                errors::errorCode_e::syntax_error,
                                "aggregaator 'with' missing value",
                                line.debug
                            };
                        }
                        with = conditions[withIdx + 1];                        
                        conditions.erase(conditions.begin() + withIdx, conditions.begin() + endIdx);
                        endIdx -= 2;
                    }

					LineParts aggregate(conditions.begin() + index, conditions.begin() + endIdx);
					conditions.erase(conditions.begin() + index, conditions.begin() + endIdx);

					if (aggregate.size() < 1)
						throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::syntax_error,
						"missing variable in aggregator",
						line.debug
					};

                    if (aggregate.size() > 2 &&
                        aggregate[2] != "where")
                    {
                        // If no `where` we add one, this allows aggregation without the "where".
                        aggregate = LineParts{ aggregate[0], aggregate[1], "where", aggregate[1], "!=", "none" };
					};

					const auto agg = aggregate[0];
					const auto variable = aggregate[1];
					const auto functionName = "__func_agg" + to_string(++autoCounter);

					// replace what captured in aggregate above with a function 
					// call we just made up
					conditions.insert(conditions.begin() + index, { functionName, "(", ")" });

					LineParts where; // empty vector to capture the where logic

					if (aggregate.size() > 2) // there is a where
						where.insert(where.begin(), aggregate.begin() + 2, aggregate.end());
				
					FirstPass newFunction = {
						{
							{"def", functionName, "(", ")", "__MARKER__"},
							lastDebug,
							0
						},
						{
							{"__agg_result", "=", "0"},
							lastDebug,
							1
						}
					};

					LineParts nextWhere = { "match" };
					nextWhere.insert(nextWhere.end(), where.begin(), where.end());
					//nextWhere.push_back("__MARKER__");

                    if (!with.length())
                        with = variable;

					if (agg == "SUM")
					{
						newFunction.insert(
							newFunction.end(),
							{
                                {
                                    { "__agg_dict", "=", "dict", "(", ")" },
                                    lastDebug,
                                    1
                                },
								{
									{ "__agg_saved_iter", "=", "iter_get", "(", ")"},
									lastDebug,
									1
								},
								{
									{ "iter_move_first", "(", ")" },
									lastDebug,
									1
								},
								{
									nextWhere, // match where <conditions>
									lastDebug,
									1
								},
                                { // indent at 2 - under where
                                    { "__agg_key", "=", "str", "(", with, ")", "+", "':'", "+", "str", "(", "__stamp", ")", "+", "':'", "+", "__action" },
                                    lastDebug,
                                    2
                                },
                                {
                                    { "if", "__agg_key", "notin", "__agg_dict" },
                                    lastDebug,
                                    2
                                },
                                {  // indent at 3 - under if
                                    { "__agg_dict", "[", "__agg_key", "]", "=", variable },
                                    lastDebug,
                                    3
                                },
                                {  // indent at 3 - under if
                                    { "__agg_result", "+=", variable },
                                    lastDebug,
                                    3
                                },
							}
						);
					}
					else if (agg == "AVG")
					{
						newFunction.insert(
							newFunction.end(),
							{
								{
									{ "__agg_count", "=", "0" },
									lastDebug,
									1
								},
                                {
                                    { "__agg_dict", "=", "dict", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    { "__agg_saved_iter", "=", "iter_get", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    { "iter_move_first", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    nextWhere, // match where <conditions>
                                    lastDebug,
                                    1
                                },
                                { // indent at 2 - under where
                                    { "__agg_key", "=", "str", "(", with, ")", "+", "':'", "+", "str", "(", "__stamp", ")", "+", "':'", "+", "__action" },
                                    lastDebug,
                                    2
                                },
                                {
                                    { "if", "__agg_key", "notin", "__agg_dict" },
                                    lastDebug,
                                    2
                                },
                                {  // indent at 3 - under if
                                    { "__agg_dict", "[", "__agg_key", "]", "=", variable },
                                    lastDebug,
                                    3
                                },
                                {  // indent at 3 - under if
                                    { "__agg_count", "+=", "1" },
                                    lastDebug,
                                    3
                                },
                                {  // indent at 3 - under if
                                    { "__agg_result", "+=", variable },
                                    lastDebug,
                                    3
                                },                                
								{ // de-nesting down to 1 - after loop, calculate average
									{ "__agg_result", "=", "__agg_result", "/", "__agg_count" },
									lastDebug,
									1
								}

							}
						);
					}
					else if (agg == "MAX")
					{

						// overwite value that __agg_result (set to zero by default above)
						newFunction.back().parts.back() = to_string(std::numeric_limits<int64_t>::min());

						newFunction.insert(
							newFunction.end(),
							{
								{
									{ "__agg_saved_iter", "=", "iter_get", "(", ")" },
									lastDebug,
									1
								},
								{
									{ "iter_move_first", "(", ")" },
									lastDebug,
									1
								},
								{
									nextWhere, // match where <conditions>
									lastDebug,
									1
								},
								{ // indent at 2 - under where
									{ "if", variable, ">", "__agg_result" },
									lastDebug,
									2
								},
								{ // indent to 3 for if block
									{ "__agg_result", "=", variable },
									lastDebug,
									3
								}
							}
						);
					}
					else if (agg == "MIN")
					{

						// overwite value that __agg_result (set to zero by default above)
						newFunction.back().parts.back() = to_string(std::numeric_limits<int64_t>::max());

						newFunction.insert(
							newFunction.end(),
							{
								{
									{ "__agg_saved_iter", "=", "iter_get", "(", ")" },
									lastDebug,
									1
								},
								{
									{ "iter_move_first", "(", ")" },
									lastDebug,
									1
								},
								{
									nextWhere, // match where <conditions>
									lastDebug,
									1
								},
								{ // indent at 2 - under where
									{ "if", variable, "<", "__agg_result" },
									lastDebug,
									2
								},
								{ // indent to 3 for if block
									{ "__agg_result", "=", variable },
									lastDebug,
									3
								}
							}
						);
					}
					else if (agg == "COUNT")
					{
						newFunction.insert(
							newFunction.end(),
							{
                                {
                                    { "__agg_dict", "=", "dict", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    { "__agg_saved_iter", "=", "iter_get", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    { "iter_move_first", "(", ")" },
                                    lastDebug,
                                    1
                                },
                                {
                                    nextWhere, // match where <conditions>
                                    lastDebug,
                                    1
                                },
                                { // indent at 2 - under where
                                    { "__agg_key", "=", "str", "(", with, ")", "+", "':'", "+", "str", "(", "__stamp", ")", "+", "':'", "+", "__action" },
                                    lastDebug,
                                    2
                                },
                                {
                                    { "if", "__agg_key", "notin", "__agg_dict" },
                                    lastDebug,
                                    2
                                },
                                {  // indent at 3 - under if
                                    { "__agg_dict", "[", "__agg_key", "]", "=", variable },
                                    lastDebug,
                                    3
                                },
                                {  // indent at 3 - under if
                                    { "__agg_result", "+=", "1" },
                                    lastDebug,
                                    3
                                },
                            }
						);
					}
					else if (agg == "DISTINCT")
					{
						newFunction.insert(
							newFunction.end(),
							{
								{
									{ "__agg_distinct", "=", "set", "(", ")" },
									lastDebug,
									1
								},
								{
									{ "__agg_saved_iter", "=", "iter_get", "(", ")" },
									lastDebug,
									1
								},
								{
									{ "iter_move_first", "(", ")" },
									lastDebug,
									1
								},
								{
									nextWhere, // match where <conditions>
									lastDebug,
									1
								},
								{ // indent at 2 - under where
									{ "__agg_distinct.add", "(", variable, ")" },
									lastDebug,
									2
								},
							// back nesting down to 1 - after loop, calculate average
								{
									{ "__agg_result", "=", "len", "(", "__agg_distinct", ")" },
									lastDebug,
									1
								}

							}
						);
					}

					// push a return statement
					newFunction.insert(
						newFunction.end(),				
						{
							{
								{ "iter_set", "(", "__agg_saved_iter", ")" },
								lastDebug,
								1
							},
							{
								{"return", "__agg_result"},
								lastDebug,
								1
							}
						}
					);

					// add these lines to the current script
					lines.insert(lines.end(), newFunction.begin(), newFunction.end());

					changes = true;
					break;
				}

				/*  Convert dictionary declarations into function calls
				 *
				 *  i.e. 
				 *  
				 *    some_dict = {'this': 'that', 'up': 'down'}
				 *    
				 *  becomes:
				 *  
				 *    some_dict = __internal_make_dict('this', 'that', 'up', 'down')
				 */

				if (conditions.size() > index + 1 &&
					conditions[index] == "del")
				{
					/* Here we are taking something that looks like this:
					 *     del some_dict['some_key']['other_key']
					 * and making it looks like this:
					 *	   __del(@some_dict['some_key'], 'other_key')
					 *	   
					 * this way __del will get a reference to the object
					 * some_dict['some_key'] and the key name to delete (other_key).
					 * 
					 * We need to do it this way because 
					 */

					auto idx = conditions.size()-1;
					
					for (auto iter = conditions.end()-1; iter != conditions.begin(); --iter, --idx)
						if (*iter == "[")
							break;

					if (conditions[idx] != "[")
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"expecting opening brace",
							line.debug 
						};

					const auto key = conditions[idx + 1];
					LineParts newStatement = { "__del", "(" };
					newStatement.insert(
						newStatement.end(), 
						conditions.begin() + 1, 
						conditions.begin() + idx);
					newStatement.push_back(",");
					newStatement.push_back(key);
					newStatement.push_back(")");

					// make first param a reference
					newStatement[2] = "@" + newStatement[2];

					line.parts.swap(newStatement);

					changes = true;

					break;
				}

				if (conditions.size() > index + 1 &&
					conditions[index] == "{")
				{
					const auto originalIndex = index;
					auto brackets = 0;
					auto allCounted = false;

					while (index < conditions.size()) // look for ending two brackets
					{
						if (conditions[index] == "{")
							++brackets;
						if (conditions[index] == "}")
							--brackets;

						if (brackets == 0)
						{
							allCounted = true;
							break;
						}

						if (conditions[index] == "__MARKER__")
							conditions[index] = ",";

						++index;
					}

					// note index should be at outer right }

					if (!allCounted)					
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_error,
							"missing closing brace",
							line.debug
						};

					conditions[originalIndex] = "__internal_make_dict";
					conditions[index] = ")";
					conditions.insert(conditions.begin() + originalIndex + 1, "(");

					changes = true;

					break;
				}

				if (conditions.size() > index + 2 &&
					(conditions[index + 1] == "in" ||
					 conditions[index + 1] == "notin") &&
					conditions[index + 2] != "[" && // this forces make_dict to be parsed first
					conditions[0] != "match" &&
					conditions[0] != "for")
				{
					// there a three kinds of "in" we care about
					// 1. for x in y
					// 2. x in [y0,y1,y2,y3] # when used in where clause
					// 3. x in y # as in:  if x in y
					// this is the `in` #2
					const auto in = conditions[index + 1];

					int reinsertIdx;
					auto left = extractVariableReverse(
						conditions, index, reinsertIdx);
					
					// conditions at this point no longer contains
					// the left side variable

					index = reinsertIdx + 1; // move to after the word 'in'

					int funcEndIdx;
					extractFunction(conditions, index, funcEndIdx);
                    if (funcEndIdx == -1)
                        funcEndIdx = conditions.size()-1;
                    
					//index += 2; // move past function name to first (
					LineParts right;

					right.insert(
						right.begin(),
						conditions.begin() + index,
						conditions.begin() + (funcEndIdx + 1));

					// strip out all in clause
					conditions.erase(
						conditions.begin() + reinsertIdx,
						conditions.begin() + (funcEndIdx + 1));

					/* convert to: 
					 *    __contains(key,container)
					 *    
					 *  TODO - optimize for second param being reference type
					 *  
					 */
					LineParts replacement{in == "in" ? "__contains" : "__notcontains", "(" };
					replacement.insert(replacement.end(), left.begin(), left.end());
					replacement.push_back(",");
					replacement.insert(replacement.end(), right.begin(), right.end());
					replacement.push_back(")");

					conditions.insert(
						conditions.begin() + reinsertIdx,
						replacement.begin(),
						replacement.end());

					// start over
					changes = true; // loop again
					break;
				}
				
				if (conditions.size() > index + 1 &&
					conditions[index + 1] == "in" &&
					conditions[0] == "match")
				{
					const auto var = conditions[index];

					if (conditions.size() < index + 4)
						throw ParseFail_s{ 
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_in_clause,
							"in-clause error",
							line.debug
						};

					// there a three kinds of in we care about
					// 1. for x in y
					// 2. x in [y0,y1,y2,y3] # when used in where clause
					// 3. x in y # as in:  if x in y
					if (conditions[index + 2] != "[")
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_in_clause,
							"expecting opening brace",
							line.debug
						};

					const auto startIdx = index;
					index += 3; // move past first [

					auto closingIdx = index;

					LineParts inParts;

					auto brackets = 1;

					for (auto idx = index; idx < conditions.size(); idx++)
					{
						if (conditions[idx] == "[")
							++brackets;

						if (conditions[idx] == ",")
							continue;

						if (conditions[idx] == "]")
						{
							--brackets;
							if (!brackets) // closing bracket
							{
								closingIdx = idx + 1;
								break;
							}
						}

						inParts.push_back(conditions[idx]);
					}

					if (!inParts.size())
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::syntax_missing_subscript,
							"expecting in-clause values",
							line.debug
						};

					LineParts orParts;

					orParts.push_back("(");
					for (auto& in : inParts)
					{
						if (orParts.size() > 1)
							orParts.push_back("or");
						orParts.push_back(var);
						orParts.push_back("is");
						orParts.push_back(in);
					}
					orParts.push_back(")");

					//parseConditions(orParts, opList, 0, debug, stopOnConditions, stackOp);

					conditions.erase(conditions.begin() + startIdx, conditions.begin() + closingIdx);
					conditions.insert(conditions.begin() + startIdx, orParts.begin(), orParts.end());

					changes = true; // loop again
					break;
				}

				if (RedundantSugar.count(conditions[index]))
				{
					conditions.erase(conditions.begin() + index);
					changes = true; // loop again
					break;
				}


				/*
				 *  convert Python style array/string slicing into function calls. i.e.
				 *  
				 *      some_string[4:10]
				 *  
				 *  becomes:
				 *  
				 *      __slice(@some_array, start, end)
				 *      
				 */

				if (conditions[index] == "[" &&
					isSplice(conditions, index))
				{

					const auto command = "__slice"s;
					const auto containerRef = "@"s + conditions[index - 1];
										
					const auto startIdx = index;

					const auto closingIdx = getMatching(conditions, index);

					LineParts inParts;
					inParts.insert(inParts.begin(), conditions.begin() + (index + 1), conditions.begin() + closingIdx);

					if (!inParts.size())
						throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::syntax_error,
						"error in slice index",
						line.debug
					};

					if (inParts[0] == "__MARKER__")
						inParts.insert(inParts.begin(), "None");
					if (inParts.back() == "__MARKER__")
						inParts.push_back("None");

					// replace the marker with a comma
					inParts[1] = ",";

					LineParts newSection;
					newSection.push_back(command);
					newSection.push_back("(");
					newSection.push_back(containerRef);

					// if the original member had parameters 
					if (inParts.size())
					{
						newSection.push_back(",");
						newSection.insert(newSection.end(), inParts.begin(), inParts.end());
					}

					newSection.push_back(")");

					conditions.erase(conditions.begin() + startIdx, conditions.begin() + (closingIdx + 1));
					conditions.insert(conditions.begin() + startIdx, newSection.begin(), newSection.end());

					changes = true; // loop again
					break;
				}

			 
				/*
				 *  Member-to-function translantion 
				 *
				 *  We convert member functions into regular function calls, where the first
				 *  param is a reference type (using @) and the params in the member function
				 *  are placed as subsequent members in the function call. 
				 *  
				 *  The function is marshaled to an internal function using the name of the 
				 *  member prepended with "__".
				 *  
				 *  i.e.
				 *  
				 *  some_array.append("a value") becomes __append( @some_array, "a value" )
				 *
				 */
				if (basic_string<char>::size_type pos; 
					(pos = conditions[index].find(".find")) != std::string::npos ||
					(pos = conditions[index].find(".rfind")) != std::string::npos ||
					(pos = conditions[index].find(".split")) != std::string::npos ||
					(pos = conditions[index].find(".strip")) != std::string::npos ||
					(pos = conditions[index].find(".append")) != std::string::npos ||
					(pos = conditions[index].find(".pop")) != std::string::npos ||
					(pos = conditions[index].find(".clear")) != std::string::npos ||
					(pos = conditions[index].find(".keys")) != std::string::npos ||
					(pos = conditions[index].find(".add")) != std::string::npos ||
					(pos = conditions[index].find(".remove")) != std::string::npos ||
					(pos = conditions[index].find(".update")) != std::string::npos)
				{
					const auto command = "__"s + conditions[index].substr(pos+1);
					const auto containerRef = "@"s + conditions[index].substr(0, pos);
					
					const auto startIdx = index;

					index += 2;
					auto closingIdx = index;
					auto brackets = 1;

					LineParts inParts;


					for (auto idx = index; idx < conditions.size(); idx++)
					{
						if (conditions[idx] == "(")
							++brackets;

						if (conditions[idx] == ")")
						{
							--brackets;
							if (!brackets) // closing bracket
							{
								closingIdx = idx + 1;
								break;
							}
						}

						inParts.push_back(conditions[idx]);
					}

					LineParts newSection;
					newSection.push_back(command);
					newSection.push_back("(");
					newSection.push_back(containerRef);

					// if the original member had parameters 
					if (inParts.size())
					{
						newSection.push_back(",");
						newSection.insert(newSection.end(), inParts.begin(), inParts.end());
					}
					
					newSection.push_back(")");

					conditions.erase(conditions.begin() + startIdx, conditions.begin() + closingIdx);
					conditions.insert(conditions.begin() + startIdx, newSection.begin(), newSection.end());

					// start over
					changes = true; // loop again
					break;
				}

				if (conditions[index] == "people")
				{
					conditions[index] = "__uuid";
					changes = true;
					break;
				}

				if (conditions[index] == "session")
				{
					conditions[index] = "__session";
					changes = true;
					useSessions = true;
					break;
				}

				if (conditions[index] == "person")
				{
					conditions[index] = "__uuid";
					changes = true;
					break;
				}

				if (conditions[index] == "action")
				{
					conditions[index] = "__action";
					changes = true;
					break;
				}

				// TRANSLATE ISNOT: convert 'is not' into 'isnot'
				if (conditions[index] == "is" &&
					index + 1 < conditions.size() &&
					conditions[index + 1] == "not")
				{
					conditions.erase(conditions.begin() + index + 1);
					conditions[index] = "isnot";
					changes = true; // loop again
					break;
				}

				if (conditions[index] == "not")
					throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"expecting 'is not' or 'not in'",
					line.debug
				};

				// TRANSLATE ISNOT: convert 'break all' or 'break top' into 'break "all"' or 'break "top"'
				if (conditions[index] == "break" &&
					index + 1 < conditions.size() &&
					(conditions[index + 1] == "all" ||
						conditions[index + 1] == "top"))
				{
					conditions[index + 1] = "'" + conditions[index + 1] + "'";
					changes = true; // loop again
					break;
				}

				// TRANSLATE TIME CONSTANTS: convert two word time constants to underscore names
				if (index + 1 < conditions.size() &&
					(
						(conditions[index] == "row" && conditions[index + 1] == "time") ||
						(conditions[index] == "last" && conditions[index + 1] == "event") ||
						(conditions[index] == "first" && conditions[index + 1] == "event") ||
						(conditions[index] == "prev" && conditions[index + 1] == "match") ||
						(conditions[index] == "previous" && conditions[index + 1] == "match") ||
						(conditions[index] == "first" && conditions[index + 1] == "match")
					))
				{
					if (conditions[index] == "previous") // shorten long form name
						conditions[index] = "prev";

					conditions[index] += "_" + conditions[index + 1];
					conditions.erase(conditions.begin() + index + 1);
					changes = true;
					break;
				}

				// TRANSLATE TIME convert days, minutes, hours, seconds to math
				// we convert: x seconds -> (x * 60)
				if (index + 1 < conditions.size() &&
					TimeConstants.count(conditions[index + 1]))
				{
					const auto item = conditions[index];
					const auto timeConst = conditions[index + 1];

					conditions.erase(conditions.begin() + index, conditions.begin() + index + 2);

					const auto timeValue = TimeConstants.at(timeConst);

					// It's an optimizing compiler now!
					if (isNumeric(item))
					{
						conditions.insert(conditions.begin() + index, to_string(stol(item) * timeValue));
					}
					else
					{
						// inject this
						vector<string> inject = {
								"(",
								item,
								"*",
								to_string(timeValue),
								")"
							};

						conditions.insert(conditions.begin() + index, inject.begin(), inject.end());
					}

					changes = true;
					break;
				}

				// bottom of line iteration
				++index;
			}

		}
		while (changes);

		if (changeCounter)
		{
			std::string translation = "";
			for (const auto c : lines[lineIndex].parts)
				translation += c + " ";
			lines[lineIndex].debug.translation.assign(translation);
		}

		++lineIndex;
	}
}

QueryParser::FirstPass QueryParser::mergeLines(FirstPass& lines) const
{
	FirstPass result;
	// make some space
	result.reserve(lines.size());

	for (auto i = 0; i < lines.size(); i++)
	{
		LineParts parts;
		string debugText;
		string debugTranslation;

		auto& line = lines[i];

		lastDebug = line.debug;

		// these are easy, iterate lines until we find the marker

		if (parseMode == parseMode_e::query && // in query mode these are 
			line.parts.size() && 
			line.parts.back() == "\\")
		{

			for (; i < lines.size(); i++)
			{
				debugText += (!debugText.length()) ?
					lines[i].debug.text :
					" " + trim(lines[i].debug.text, " ");

				debugTranslation += (!debugTranslation.length()) ?
					lines[i].debug.translation :
					" " + trim(lines[i].debug.translation, " ");

				for (auto& p : lines[i].parts)
				{
					if (p != "\\")
						parts.push_back(p);
				}

				if (lines[i].parts.size() && lines[i].parts.back() != "\\")
					break;
			}

			FirstPass_s temp;
			temp.parts = move(parts);
			temp.text = line.text;
			temp.indent = line.indent;
			temp.debug.text = debugText;
			temp.debug.translation = debugTranslation;
			temp.debug.number = line.debug.number;
			result.emplace_back(move(temp));
		}
		else if (parseMode == parseMode_e::query && // in query mode these are 
			(line.parts[0] == "if" ||
				line.parts[0] == "elif" ||
				line.parts[0] == "else" ||
				line.parts[0] == "for" ||
				line.parts[0] == "match" ||
				line.parts[0] == "reverse_match"))
		{
			for (; i < lines.size(); i++)
			{
				debugText += (!debugText.length()) ?
					lines[i].debug.text :
					" " + trim(lines[i].debug.text, " ");

				debugTranslation += (!debugTranslation.length()) ?
					lines[i].debug.translation :
					" " + trim(lines[i].debug.translation, " ");

				// TODO check for keywords at beginning of line
				// this would indicate a skipped `:`

				auto idx = 0;
				for (auto& p : lines[i].parts)
				{
					// TODO check for values that cannot exist in 
					// logic like assignment operators

					if (p == "__MARKER__" && idx == lines[i].parts.size() - 1)
						goto completed;
					parts.push_back(p);
					++idx;
				}
			}

		completed: // legit use of goto as double break.

			if (!checkBrackets(parts))
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"bracket count mismatch (1)",
					line.debug
				};

			FirstPass_s temp;
			temp.parts = move(parts);
			temp.text = line.text;
			temp.indent = line.indent;
			temp.debug.text = debugText;
			temp.debug.translation = debugTranslation;
			temp.debug.number = line.debug.number;
			result.emplace_back(move(temp));
		}
		else
		{
			// here we are going to scan for open brackets i.e. ( { and [ 
			// and combine lines until we find their the closing brackets
			// or we encounter an error.

			auto brackets = 0; // we don't care what type they are as long as they add up

			for (; i < lines.size(); i++)
			{
				debugText += (!debugText.length()) ?
					lines[i].debug.text :
					" " + trim(lines[i].debug.text, " ");

				debugTranslation += (!debugTranslation.length()) ?
					lines[i].debug.translation :
					" " + trim(lines[i].debug.translation, " ");

				for (auto& p : lines[i].parts)
				{
					// TODO check for values that cannot exist in 
					// logic like assignment operators

					if (p == "(" || p == "[" || p == "{")
						++brackets;
					else if (p == ")" || p == "]" || p == "}")
						--brackets;

					parts.push_back(p);
				}

				if (!brackets)
					break;
			}

			if (!checkBrackets(parts))
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::syntax_error,
					"bracket count mismatch (2)",
					line.debug
				};

			FirstPass_s temp;
			temp.parts = move(parts);
			temp.text = line.text;
			temp.indent = line.indent;
			temp.debug.text = debugText;
			temp.debug.translation = debugTranslation;
			temp.debug.number = line.debug.number;
			result.emplace_back(move(temp));
		}
	}

	return result;
}

int64_t QueryParser::parseHintConditions(LineParts& conditions, HintOpList& opList, int64_t index, const bool stopOnConditions)
{
	LineParts accumulation;

	// helper to convert our regular macro Ops to Hint style Ops
	auto convertOp = [](string op) -> HintOp_e
	{		
		const auto opIter = Operators.find(op); 

		if (opIter == Operators.end())
		{
			auto logIter = LogicalOperators.find(op);
			if (logIter == LogicalOperators.end())
				return HintOp_e::UNSUPPORTED;
			auto iter = OpToHintOp.find(logIter->second);
			return iter != OpToHintOp.end() ? iter->second : HintOp_e::UNSUPPORTED;
		}

		auto iter = OpToHintOp.find(opIter->second);
		return iter != OpToHintOp.end() ? iter->second : HintOp_e::UNSUPPORTED;
	};

	// store textual values as we come across them
	// we will call store when we hit a logical operator
	const auto accumulate = [&accumulation](string item)
	{
		accumulation.push_back(item);
	};

	// this will evaluate what conditions were
	// accumulated and store them in OpList if 
	// possible, will return FALSE if it could not
	// so the logical operator can be bypassed in the
	// output (we cull functions calls and user variables)
	const auto store = [&]()
	{
		if (!accumulation.size())
		{
			// nothing accumulated - this is OK
			return; // this is not an error
		}

		if (accumulation.size() == 1)
		{
			// TODO handle this - simply checking whether value present

			auto left = accumulation[0];

			if (isColumnVar(left))
				left = vars.columnVars.find(left)->second.actual;

			if (left.find("column.") == 0)
				left = left.substr(left.find('.') + 1);

			if (isNonuserVar(left))
			{
				// use PUSH_PRESENT (value 0 is ignored)
				opList.emplace_back(HintOp_e::PUSH_PRESENT, left, 0);
				accumulation.clear();
				return;
			}
			else
			{
				// we don't know what this is, so we will push an
				// ignorable value into here
				opList.emplace_back(HintOp_e::PUSH_NOP);
				accumulation.clear();
				return;
			}
		}

		if (accumulation.size() > 3)
		{
			// this accumulation likely contains math or
			// function calls, either way, we can't optimize
			// with math (especially when variables are involved)
			// so we will emit a NOP as a placeholder for the logic
			opList.emplace_back(HintOp_e::PUSH_NOP);
			accumulation.clear();
			return;
		}

		auto left = accumulation[0];
		HintOp_e op = convertOp(accumulation[1]);
		auto right = accumulation[2];

		if (op == HintOp_e::UNSUPPORTED)
		{
			// we can't index on this operator so emit a NOP placeholder
			opList.emplace_back(HintOp_e::PUSH_NOP);
			accumulation.clear();
			return;
		}

		auto leftIsTableVar = isNonuserVar(left);
		auto rightIsTableVar = isNonuserVar(right);

		if (leftIsTableVar && rightIsTableVar)
		{
			// skip column to column comparisons, emit a NOP placeholder
			opList.emplace_back(HintOp_e::PUSH_NOP);
			accumulation.clear();
			return;
		}

		if (!leftIsTableVar && !rightIsTableVar)
		{
			// skip non-table variable comparisons, emit a NOP placeholder
			opList.emplace_back(HintOp_e::PUSH_NOP);
			accumulation.clear();
			return;
		}

		if (isUserVar(left) || isUserVar(right))
		{
			opList.emplace_back(HintOp_e::PUSH_NOP);
			accumulation.clear();
			return;
		}

		/**
		 * so, we have a condition written with he variable
		 * on the right and the value on the left. We will swap em
		 */
		if (rightIsTableVar)
		{
			swap(left, right); // nice

			// swap certain operators if right is a var
			switch (op)
			{
				case HintOp_e::PUSH_GT:
					op = HintOp_e::PUSH_LTE;
					break;
				case HintOp_e::PUSH_LT:
					op = HintOp_e::PUSH_GTE;
					break;
				case HintOp_e::PUSH_GTE:
					op = HintOp_e::PUSH_LT;
					break;
				case HintOp_e::PUSH_LTE:
					op = HintOp_e::PUSH_GT;
					break;
				default: ;
			}
		}

		/** 
		 * OK, now at a point where we know we aren't dealing with
		 * an unsupported operator, values on the left and right,
		 * or table columns on the left and right, and that the table 
		 * column is on the left! 
		 */

		// lets get the "real" non-alias names for these columns
		if (isColumnVar(left))
			left = vars.columnVars.find(left)->second.actual;
		//else if (isGroupVar(left))
		//left = vars.groupVars.find(left)->second.actual;

		if (left.find("column.") == 0)
			left = left.substr(left.find('.') + 1);

		if (isNumeric(right))
			opList.emplace_back(op, left, stoll(right));
		else
			opList.emplace_back(op, left, right);

		accumulation.clear();
		return;
	};

	while (index < conditions.size())
	{
		if (conditions[index] == ",")
		{
			break;
		}
		if (conditions[index] == ")")
		{
			//++index;
			break;
		}
		if (conditions[index] == "(")
		{
			index = parseHintConditions(
				conditions,
				opList,
				index + 1,
				false);
		}
		else if (LogicalOperators.find(conditions[index]) !=
			LogicalOperators.end())
		{
			store();

			if (stopOnConditions)
			{
				index--;
				break;
			}

			const int newIndex = parseHintConditions(
				conditions,
				opList,
				index + 1,
				false);

			opList.emplace_back(convertOp(conditions[index]));
			index = newIndex;

			break;
		}
		else
		{
			// this is a function call
			if (index < conditions.size() - 1 &&
				conditions[index + 1] == "(")
			{
				// We skip function calls in where. Will
				// strip the function, and rip any nested operations
				// basically replacing anything between the brackets
				// with an OP BIT_NOP 
				auto brackets = 1;

				auto idx = index + 2; // move past function name and (
				for (; idx < conditions.size(); idx++)
				{
					if (conditions[idx] == "(")
						++brackets;
					else if (conditions[idx] == ")")
						--brackets;

					if (!brackets)
						break;
				}

				index = idx;

				opList.emplace_back(HintOp_e::PUSH_NOP);
				accumulation.clear();
				//break;
			}
			else
			{
				accumulate(conditions[index]);
			}
		}
		++index;
	}

	store();

	return index;
}

void QueryParser::evaluateHints(const string hintName, HintOpList& hintOps)
{
	const auto hint = hintMap.find(hintName);

	if (hint == hintMap.end())
		return; // this is valid

	parseHintConditions(
		(*hint).second, // the hints under this hint name
		hintOps,
		0,
		false);
}

void QueryParser::build(
	Columns* columns,
	MiddleBlockList& input,
	InstructionList& finCode,
	Variables_S& finVars)
{
	/*
	We are converting our maps to lists. We used maps to eliminate
	duplicates without having to iterate. We are using lists
	in the final variable structure (variables_s) because they can
	be directly indexed. The code will reference variables by index
	only. It will also reference text literals by index only.

	Both the original maps and the lists are index values set,
	this allows our compile pass to set the index while looking up
	by name.

	Text literals are actually stored as hash values. Hash values
	are also what are stored in the DB grid. This makes strings
	comparable with an integer operation.

	TODO - there is a possibility for strings hashes to collide. We need
	to address that.
	*/

	// index the select variables
	for (auto& v : vars.columnVars)
	{
		// add non var types (aka db columns) to the
		// tableVars list
		if (v.second.modifier != Modifiers_e::var)
		{
			vars.tableVars[v.second.actual] = v.second;
			const auto colInfo = columns->getColumn(v.second.actual);

			if (!colInfo)
				throw ParseFail_s{
					errors::errorClass_e::parse,
					errors::errorCode_e::column_not_in_table,
					"agg result column: " + v.second.actual
				};
			v.second.schemaColumn = colInfo->idx;
			v.second.schemaType = colInfo->type;
		}
		
		v.second.index = v.second.sortOrder; // FIX finVars.columnVars.size();
		finVars.columnVars.push_back(v.second);
	}

	std::sort(finVars.columnVars.begin(), finVars.columnVars.end(),
	     [](const Variable_s a, const Variable_s b)
     {
	     return a.sortOrder < b.sortOrder;
     });

	// index the column variables
	for (auto& v : vars.tableVars)
	{
		v.second.index = finVars.tableVars.size();
		v.second.column = v.second.index; // map column same as index

		// find this column in the schema
		const auto schemaInfo = columns->getColumn(v.second.actual);
		if (!schemaInfo)
			throw ParseFail_s{
				errors::errorClass_e::parse,
				errors::errorCode_e::column_not_in_table,
				"column_check: " + v.second.actual
			};
		// set the sort to the actual schema index - so they will be in order
		v.second.sortOrder = schemaInfo->idx;
		v.second.schemaColumn = schemaInfo->idx;
		v.second.schemaType = schemaInfo->type;
		finVars.tableVars.push_back(v.second);
	}

	std::sort(finVars.tableVars.begin(), finVars.tableVars.end(),
	     [](const Variable_s a, const Variable_s b)
     {
	     return a.sortOrder < b.sortOrder;
     });

	auto idx = -1;
	for (auto& cv : finVars.tableVars)
	{
		++idx;
		cv.index = idx;
		cv.column = idx;
		vars.tableVars[cv.actual].index = cv.index;
		vars.tableVars[cv.actual].column = cv.column;
	}

	// index the user variables
	for (auto& v : vars.userVars)
	{
		v.second.index = finVars.userVars.size();
		finVars.userVars.push_back(v.second);
	}

	// index the text literals
	for (auto& t : vars.literals)
	{
		const auto trimmed = stripQuotes(t.first);
		t.second = finVars.literals.size(); // index - set to current length 
		TextLiteral_s literal;
		literal.hashValue = MakeHash(trimmed);
		literal.value = trimmed;
		literal.index = t.second; // index from above
		finVars.literals.emplace_back(move(literal));
	}

	for (auto& v : finVars.columnVars)
	{
		if (v.modifier == Modifiers_e::var)
			continue;
		const auto tableVar = vars.tableVars.find(v.actual);
		if (tableVar == vars.tableVars.end())		
			throw ParseFail_s{
				errors::errorClass_e::parse,
				errors::errorCode_e::column_not_in_table,
				"select column: " + v.actual 
			};
		// map column variable to actual column
		v.column = tableVar->second.column;
	}

	// map the distinct key column
	for (auto& v : finVars.columnVars)
	{
		if (v.modifier == Modifiers_e::var)
			continue;
		const auto tableVar = vars.tableVars.find(v.distinctColumnName);
		if (tableVar == vars.tableVars.end())
			throw ParseFail_s{
				errors::errorClass_e::parse,
				errors::errorCode_e::column_not_in_table,
				"select column distinct: " + v.distinctColumnName
			};
		// map column variable to actual column
		v.distinctColumn = tableVar->second.column;
	}

	// see if this query uses global variables
	// note: we may want to move this further up in the parser
	for (auto&s: vars.userVars)
		if (s.second.actual == "globals")
		{
			useGlobals = true;
			break;
		}

	// this map holds the instruction position for
	// a given block. The code that is generated is a 
	// single array, each block is appended to the 
	// the array, the blocks IDs in function calls and
	// lambdas are converted in the final pass to
	// the index here.
	unordered_map<int64_t, int64_t> blockIndex;

	// this maps a function name to a specific index
	// in the final code
	unordered_map<string, int64_t> functionMap;

	// this records the function name and the position in the
	// final code, this is used post compile to map a function
	// name to an index
	unordered_map<int64_t, string> callMap;

	//for (auto blockFilter = 0; blockFilter < 2; ++blockFilter)
	{
		for (auto& b : input) // iterate the blocks
		{
			/*
			if (!blockFilter && (
				b.type == blockType_e::function ||
				b.type == blockType_e::lambda))
				continue;
			else if (blockFilter && 
				b.type != blockType_e::function &&
				b.type != blockType_e::lambda)
				continue;
			*/

			// first line of this block will be tail of
			// of the final Code at this point.
			blockIndex[b.blockId] = finCode.size();

			// if this is a function map its name to the 
			// final Code line
			if (b.type == BlockType_e::function)
				functionMap[b.blockName] = finCode.size();

			// iterate instruction
			for (auto& c : b.code)
			{
				switch (c.op)
				{
					case OpCode_e::NOP:
						break;
					case OpCode_e::PSHTBLCOL:
						finCode.emplace_back(
							c.op,
							vars.tableVars[c.valueString].index,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::PSHRESCOL:
						finCode.emplace_back(
							c.op,
							vars.columnVars[c.valueString].index,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::VARIDX:
						finCode.emplace_back(
							c.op,
							vars.userVars[c.valueString].index,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::PSHLITTRUE:
					case OpCode_e::PSHLITFALSE:
						finCode.emplace_back(
							c.op,
							0,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::PSHUSROBJ:
					case OpCode_e::PSHUSRVAR:
					case OpCode_e::PSHUSRVREF:
					case OpCode_e::PSHUSROREF:
						finCode.emplace_back(
							c.op,
							vars.userVars[c.valueString].index,
							0,
							c.params,
							c.debug);
						break;
					case OpCode_e::PSHLITSTR:
						finCode.emplace_back(
							c.op,
							vars.literals[c.valueString], // map is to index
							0,
							0,
							c.debug);
						break;
					case OpCode_e::PSHLITINT:
						finCode.emplace_back(
							c.op,
							0,
							c.value,
							0,
							c.debug);
						break;
					case OpCode_e::PSHLITFLT:
						finCode.emplace_back(
							c.op,
							0,
							c.value, // TODO fix floats - which are ints, but different
							0,
							c.debug);
						break;
					case OpCode_e::PSHPAIR:
					case OpCode_e::PSHLITNUL:
						finCode.emplace_back(
							c.op,
							0,
							0,
							0,
							c.debug);
						break;

					case OpCode_e::POPUSRVAR:
					case OpCode_e::POPUSROBJ:
						finCode.emplace_back(
							c.op,
							vars.userVars[c.valueString].index,
							0,
							c.params,
							c.debug);
						break;
					case OpCode_e::POPTBLCOL:
						finCode.emplace_back(
							c.op,
							vars.tableVars[c.valueString].index,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::POPRESCOL:
						finCode.emplace_back(
							c.op,
							vars.columnVars[c.valueString].index,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::CNDIF:
					case OpCode_e::CNDELIF:
						finCode.emplace_back(
							c.op,
							c.value, // value for the moment is block_id
							0,
							c.lambda, // extra maps to lambda
							c.debug);
						break;
					case OpCode_e::CNDELSE:
						finCode.emplace_back(
							c.op,
							c.value, // value for the moment is block_id
							0,
							0,
							c.debug);
						break;
					case OpCode_e::ITFOR:
						finCode.emplace_back(
							c.op,
							c.value, // value for the moment is block_id
							c.params,
							0,
							c.debug);
						break;
					case OpCode_e::ITNEXT:
					case OpCode_e::ITPREV:
						finCode.emplace_back(
							c.op,
							c.value, // value for the moment is block_id
							c.params,
							c.lambda, // extra maps to lambda
							c.debug);
						break;
					case OpCode_e::MATHADD:
					case OpCode_e::MATHSUB:
					case OpCode_e::MATHMUL:
					case OpCode_e::MATHDIV:
					case OpCode_e::OPGT:
					case OpCode_e::OPLT:
					case OpCode_e::OPGTE:
					case OpCode_e::OPLTE:
					case OpCode_e::OPEQ:
					case OpCode_e::OPNEQ:
					case OpCode_e::OPWTHN:
					case OpCode_e::OPNOT:
					case OpCode_e::LGCAND:
					case OpCode_e::LGCOR:
						finCode.emplace_back(
							c.op,
							0,
							0,
							0,
							c.debug);
						break;
					case OpCode_e::MATHADDEQ:
					case OpCode_e::MATHSUBEQ:
					case OpCode_e::MATHMULEQ:
					case OpCode_e::MATHDIVEQ :
						finCode.emplace_back(
							c.op,
							vars.userVars[c.valueString].index,
							0,
							c.params,
							c.debug);
						break;

					case OpCode_e::MARSHAL:
						// these should not appear in generated code
						// something is rotten in Denmark
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::missing_function_definition,
							"missing function",
							c.debug 
						};

					case OpCode_e::CALL:
						if (Marshals.find(c.valueString) != Marshals.end())
						{
							auto marshal = Marshals.find(c.valueString)->second;

							if (SegmentMathMarshals.count(marshal))
								isSegmentMath = true;

							marshalsReferenced.emplace(marshal);

							finCode.emplace_back(
								OpCode_e::MARSHAL,
								cast<int64_t>(marshal),
								0,
								c.params, // extra has param count
								c.debug);
						}
						else
						{
							// map this output line to this function name
							// next loop will resolve this
							callMap[finCode.size()] = c.valueString;

							finCode.emplace_back(
								OpCode_e::CALL,
								c.value, // this is the code block index
								0,
								0,
								c.debug);
						}
						break;
					case OpCode_e::RETURN:
						finCode.emplace_back(
							c.op,
							0,
							0,
							0,
							c.debug);
						break;

					case OpCode_e::TERM:
						finCode.emplace_back(
							c.op,
							0,
							0,
							0,
							c.debug);
						break;

					default:
						// WTF happened here... 
						throw ParseFail_s{
							errors::errorClass_e::parse,
							errors::errorCode_e::general_error,
							"something strange happenned",
							c.debug
						};

				}
			}
		}
	}

	// add the term code to the end of the program
	finCode.emplace_back(OpCode_e::TERM, 0, 0, 0);

	int64_t lineNumber = 0;

	for (auto& f : finCode)
	{
		switch (f.op)
		{
			case OpCode_e::CNDIF:
			case OpCode_e::CNDELIF:
			case OpCode_e::ITFOR:
			case OpCode_e::ITNEXT:
			case OpCode_e::ITPREV:
				// map index to index of BlockID
				f.index = blockIndex[f.index];
				// map lambda (extra) to index of BlockId
				f.extra = blockIndex[f.extra];
				break;

			case OpCode_e::CNDELSE:
				// map index to index of BlockID
				f.index = blockIndex[f.index];
				break;

			case OpCode_e::CALL:
				if (!functionMap.count(callMap[lineNumber]))
					throw ParseFail_s{
						errors::errorClass_e::parse,
						errors::errorCode_e::missing_function_definition,
						"call missing function",
						(finCode.begin() + lineNumber)->debug
					};
				f.index = functionMap[callMap[lineNumber]];
				break;
			default: break;
		}
		++lineNumber;
	}

	for (auto& s: finVars.columnVars)
	{
		if (s.lambdaIndex != -1)
		{
			s.lambdaIndex = functionMap["_column_lambda_" + to_string(s.lambdaIndex)];
			// we cache a list of select lambas to call when generating results.
			// groups are evaluated iteratively. 
			finVars.columnLambdas.push_back(s.lambdaIndex);
		}
	}

	for (auto& f : functionMap)
	{
		finVars.functions.emplace_back(
			Function_s{
					f.first,
					f.second
				});
	}
}

bool QueryParser::compileQuery(const char* query, Columns* columnsPtr, Macro_s& macros, ParamVars* templateVars)
{	

	try
	{
		rawScript = query;

		templating = templateVars;

		// we will be suing the schema to validate the
		// select and group and column. variables
		tableColumns = columnsPtr;

		vars.tableVars.emplace(
			"__stamp",
			Variable_s{ "__stamp", "grid" });
		vars.tableVars.emplace(
			"__action",
			Variable_s{ "__action", "grid" });

		// breaks it out into lines
		auto firstPass = extractLines(query);

		BlockList blockList;
		// convert lines into blocks by indentation
		blockList.emplace_back(0, FirstPass{});

		extractBlocks(0, firstPass, blockList);

		// iterate through our list of blocks, converting line parts
		// into opcodes and breaking out into conditionals such as
		// if, elif, next, and prev into condition lambdas
		MiddleBlockList middleBlocks;
		for (auto& b : blockList)
			tokenizeBlock(b.code, b.blockId, blockList, middleBlocks);

		for (auto& m : middleBlocks)
		{
			Debug_s dbg;
			m.code.emplace_back(OpCode_e::RETURN, 0, dbg);
		}

		// sort blocks into descending block order, blocks are likely to be accessed
		// in this order, with the main block being block 1.
		sort(middleBlocks.begin(), middleBlocks.end(), [](MiddleBlock_s& a, MiddleBlock_s& b)
		{
			return a.blockId < b.blockId;
		});


		if (useSessions && vars.tableVars.count("__session") == 0)
			vars.tableVars.emplace(
				"__session",
				Variable_s{ "__session", "grid" });

		// fill class variable 'macro' with the built code.
		// the macro structure can be passed to the interpretor
		build(tableColumns, middleBlocks, macros.code, macros.vars);

		// this will build the instruction list for generating index
		// stacks that will result in an index containing all and any
		// column type variables used in the query. 
		//
		// note: indexes is a list of <name,hintOpList> pairs, we make
		// and fill them here.

		if (!hintNames.size())
			hintNames.push_back("_");

		for (const auto &n : hintNames)
		{
			auto hintPair = HintPair(n, {});
			evaluateHints(n, hintPair.second);
			macros.indexes.emplace_back(hintPair);
		}

		macros.marshalsReferenced = marshalsReferenced;
		macros.segmentTTL = segmentTTL;
		macros.segmentRefresh = segmentRefresh;
		macros.useCached = segmentUseCached;
		macros.isSegment = isSegment;
		macros.isSegmentMath = isSegmentMath;
		macros.useGlobals = useGlobals;

		macros.rawScript = rawScript;

		macros.useSessions = useSessions;
		if (macros.useSessions)
		{
			for (auto &c: macros.vars.tableVars)
				if (c.actual == "__session"s)
				{
					macros.sessionColumn = c.index;
					break;
				}
		}

		return true;
	}
	catch (ParseFail_s & caught)
	{
		if (lastDebug.text.length() && caught.debug.text.length() == 0)
			caught.debug = lastDebug;

		error.set(
			caught.eClass,
			caught.eCode,
			caught.getMessage(),
			caught.getDetail()
		);
		return false;
	}
/*	catch (const std::exception& ex)
	{
		std::string additional = "";
		if (lastDebug.text.length())
			additional = lastDebug.toStrShort();

		error.set(
			errors::errorClass_e::parse,
			errors::errorCode_e::run_time_exception_triggered,
			std::string{ ex.what() }+"(1)",
			additional
		);
		return false;
	}*/
	catch (const std::runtime_error& ex)
	{
		std::string additional = "";
		if (lastDebug.text.length())
			additional = lastDebug.toStrShort();

		error.set(
			errors::errorClass_e::parse,
			errors::errorCode_e::run_time_exception_triggered,
			std::string{ ex.what() }+"(1)",
			additional
		);
		return false;
	}
	catch (...) // giant WTF runtime exception
	{
		std::string additional = "";
		if (lastDebug.text.length())
			additional = lastDebug.toStrShort();

		error.set(
			errors::errorClass_e::parse,
			errors::errorCode_e::run_time_exception_triggered,
			"unknown exception in parser",
			additional
		);
		return false;
	}
	
}

std::string QueryParser::fixIndent(const std::string source)
{
    std::vector<string> res;

    auto parts = split(source, '\n');

    auto indent = -1;

    for (auto p : parts)
    {
        int t;
        // replace tabs
        while ((t = p.find('\t')) != -1)
        {
            p.erase(t, 1);
            p.insert(t, "    ");
        }

        // skip empty lines or lines with just whitespace
        if (trim(p, " ").size() == 0)
            continue;

        if (indent == -1)
        {
            indent = 0;
            for (auto i = 0; i < p.length(); ++i)
            {
                indent = i;
                if (p[i] != ' ')
                    break;
            }
        }

        p = p.erase(0, indent);

        res.push_back(p);
    }

    std::string outputString;

    for (const auto r : res)
        outputString += r + "\n";

    return outputString;
}

QueryParser::SectionDefinitionList QueryParser::extractSections(const char* query)
{
	vector<QueryParser::SectionDefinition_s> result;

	vector<string> accumulatedLines;
	string current;
	string functionName;

	auto c = query; // cursor
	const auto end = query + strlen(query) + 1;

    cvar params(cvar::valueType::DICT);
    cvar flags(cvar::valueType::DICT);
    
    std::string sectionType;
    std::string sectionName;

    auto storeSection = [&]()
    {
        string code;

        for (auto& s : accumulatedLines)
            code += s + '\n';
        code += '\n';

        // this allows you to indent the code under the @section if preferred
        code = fixIndent(code);

        accumulatedLines.clear();

        result.emplace_back(SectionDefinition_s{
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

			case 0:
			case '\n':
			{
				auto tabDepth = 0;
				// count our spaces
				for (const auto s : current)
				{
					if (s == ' ')
						++tabDepth;
					else
						break;
				}

				// convert spaces to tab counts (4 spaces
				tabDepth /= 4;

				// remove leading spaces (or any other characters we don't want)
				current = trim(current, " ");

				// if this line isn't empty, and isn't a comment
				// store it
				if (current.length() && current[0] != '#')
				{
					// add the tabs back in
					if (tabDepth > 1)
						for (auto i = 0; i < tabDepth - 1; ++i)
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

                            for (auto idx = 2; idx < sectionParts.size(); ++idx)
                            {
                                auto keyVal = split(sectionParts[idx], '=');
                                
                                if (keyVal.size() == 1)
                                    keyVal.push_back("True");

                                if (keyVal[0] == "ttl" || keyVal[0] == "refresh") // these are special and allow for time appends like 's' or 'm', or 'd'
                                    flags[keyVal[0]] = expandTime(keyVal[1]) * 1000;
                                else if (keyVal[0] == "use_cached")
                                    flags["use_cached"] = (keyVal[1] == "True" || keyVal[1] == "true");
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

string padding(string text, const int length, const bool left = true, const char filler = ' ')
{
	while (text.length() < length)
		text = (left) ? filler + text : text + filler;
	return text;
}

string padding(const int64_t number, const int length, const bool left = true, const char filler = ' ')
{
	return padding(to_string(number), length, left, filler);
}

string openset::query::MacroDbg(Macro_s& macro)
{
	stringstream ss;

	auto outSpacer = [&ss]()
	{
		ss << "--------------------------------------------------------------------------------------------------------------------------------------------------------" << endl;
	};

	ss << endl;
	ss << "Raw Script:" << endl;
	outSpacer();
	ss << macro.rawScript << endl;
	outSpacer();

	ss << endl << endl;
	ss << "Text literals:" << endl;
	outSpacer();
	ss << "IDX | ID               | TEXT + HEX" << endl;
	outSpacer();
	if (macro.vars.literals.size())
	{
		for (auto& v : macro.vars.literals)
		{
			ss << padding(v.index, 3, true) << " | ";
			ss << "#" << hex << v.hashValue << " | ";
			ss << "\"" << v.value << "\" hex: ";

			for (auto ch : v.value)
				ss << setfill('0') << setw(2) << 
				hex << abs(cast<int>(ch)) << ' ';

			ss << endl;
		}	
		// stop hexing!
		cout << dec;
	}
	else
		ss << "NONE" << endl;
	outSpacer();
	
	ss << endl << endl;
	ss << "User variables:" << endl;
	outSpacer();
	ss << "IDX | NAME                   | INIT" << endl;
	outSpacer();
	if (macro.vars.userVars.size())
	{
		for (auto& v : macro.vars.userVars)
		{
			ss << padding(v.index, 3, true) << " | ";
			ss << padding(v.actual, 20, false, ' ') << " | " << (v.startingValue == NONE ? "null" : v.startingValue);
			ss << endl;
		}
	}
	else
		ss << "NONE" << endl;
	outSpacer();
	
	ss << endl << endl;
	ss << "Table Column Map (in script or aggregates):" << endl;
	outSpacer();
	ss << "IDX | COLIDX | NAME                 | TYPE      | NOTE" << endl;
	outSpacer();
	if (macro.vars.tableVars.size())
	{
		for (auto& v : macro.vars.tableVars)
		{
			ss << padding(v.index, 3, true) << " | ";
			ss << padding(v.schemaColumn, 6, true) << " | ";
			ss << padding(v.actual, 20, false) << " | ";

			std::string type;
			switch (v.schemaType)
			{
				case columnTypes_e::freeColumn: 
					type = "err(1)";
				break;
				case columnTypes_e::intColumn: 
					type = "int";
				break;
				case columnTypes_e::doubleColumn: 
					type = "double";
				break;
				case columnTypes_e::boolColumn: 
					type = "bool";
				break;
				case columnTypes_e::textColumn: 
					type = "text";
				break;
				default: 
					type = "err(2)";
				;
			}

			ss << padding(type, 9, false) << " | ";

			if (v.actual == "__uuid")
				ss << "actual for 'person' or 'people'";
			else if (v.actual == "__action")
				ss << "actual for 'action'";
			else if (v.actual == "__stamp")
				ss << "actual for 'stamp'";
			else if (v.actual == "__session")
				ss << "actual for 'session'";
			ss << endl;
		}
	}
	else
		ss << "NONE" << endl;
	outSpacer();

	ss << endl << endl;
	ss << "Aggregates:" << endl;
	outSpacer();
	ss << "AGGIDX | TBLIDX | AGG    | NAME                 | ALIAS                | NOTE" << endl;
	outSpacer();
	if (macro.vars.columnVars.size())
	{
		for (auto& v : macro.vars.columnVars)
		{
			ss << padding(v.index, 6, true) << " | ";
			ss << padding(v.column, 6, true) << " | ";
			ss << padding(ModifierDebugStrings.find(v.modifier)->second, 6, false) << " | ";
			if (v.column == -1)
				ss << "  NA  | ";
			else
			ss << padding(v.actual, 20, false) << " | ";
			ss << padding(v.alias, 20, false) << " | ";

			if (v.actual == "__uuid")
				ss << "from 'person' or 'people'  ";
			else if (v.actual == "__action")
				ss << "from 'action'  ";
			else if (v.actual == "__stamp")
				ss << "from 'stamp'  ";
			else if (v.actual == "__session")
				ss << "from 'session'  ";

			if (v.distinctColumnName != v.actual)
				ss << "distinct: " << v.distinctColumnName;

			ss << endl;
		}
	}
	else
		ss << "NONE" << endl;
	outSpacer();

	ss << endl << endl;
	ss << "PyQL Marshals:" << endl;
	outSpacer();
	ss << "FUNC# | MARSHAL" << endl;
	outSpacer();
	if (macro.marshalsReferenced.size())
	{
		auto getMarshalName = [](Marshals_e marshalCode)->std::string
		{
			for (auto &m : Marshals)
				if (m.second == marshalCode)
					return m.first;
			return "__MISSING__";
		};

		for (auto& m : macro.marshalsReferenced)
		{
			ss << padding(static_cast<int64_t>(m), 5, true) << " | ";
			ss << getMarshalName(m) << endl;
		}
	}
	else
		ss << "NONE" << endl;
	outSpacer();

	ss << endl << endl;
	ss << "User Functions:" << endl;
	outSpacer();
	ss << " OFS | NAME" << endl;
	outSpacer();
	if (macro.vars.functions.size())
	{
		for (auto& f : macro.vars.functions)
		{
			ss << padding(f.execPtr, 4, true, '0') << " | ";
			ss << f.name;
			ss << endl;
		}

		ss << endl;
	}
	else
		ss << "NONE" << endl;
	outSpacer();


	for (auto& pair : macro.indexes)
	{
		auto& index = pair.second;

		ss << endl << endl;
		ss << "Index Macros:" << endl;
		outSpacer();
		ss << "OP             | COLUMN               | VALUE" << endl;
		outSpacer();
		if (index.size())
		{
			for (auto& i : index)
			{
				const auto op = HintOperatorsDebug.find(i.op)->second;
				ss << padding(op, 14, false) << " | ";

				switch (i.op)
				{
				case HintOp_e::PUSH_EQ:
				case HintOp_e::PUSH_NEQ:
				case HintOp_e::PUSH_GT:
				case HintOp_e::PUSH_GTE:
				case HintOp_e::PUSH_LT:
				case HintOp_e::PUSH_LTE:
					ss << padding(i.column, 20, false) << " | ";
					if (i.numeric)
						ss << to_string(i.intValue);
					else
						ss << i.textValue;
					break;

				case HintOp_e::PUSH_PRESENT:
					ss << padding(i.column, 20, false) << " | ";
					break;
				default:
					ss << "                     |";
				}

				ss << endl;
			}
		}
		else
			ss << "NONE - EVAL*" << endl;
		outSpacer();
	}

	ss << endl << endl;
	ss << "Assembly:" << endl;
	outSpacer();
	ss << "OFS  | OP           |           VAL |      IDX |      EXT | LINE | CODE" << endl;
	outSpacer();
	auto count = 0;
	for (auto& m: macro.code)
	{
		const auto opString = OpDebugStrings.find(m.op)->second;
		ss << padding(count, 4, true, '0') << " | ";
		ss << padding(opString, 12, false) << " | ";
		ss << ((m.value == 9999999) ? padding("INF", 13, left) : padding(m.value, 13, left)) << " | ";
		ss << padding(m.index, 8, left) << " | ";
		ss << padding(m.extra, 8, left) << " | ";
		ss << ((m.debug.number == -1) ? "    " : padding( "#" + to_string(m.debug.number), 4)) << " | ";
		ss << m.debug.text;
		ss << endl;
		if (m.debug.translation.length())
		{
			std::string spaces = "";
			auto it = m.debug.text.begin();
			while (*it == ' ')
			{
				spaces += ' ';
				++it;
			}
			ss << "     |              |               |          |          | ";
			ss << "   > | " << spaces << m.debug.translation << endl;
		}
		++count;
	}
	outSpacer();
	
	return ss.str();
}

