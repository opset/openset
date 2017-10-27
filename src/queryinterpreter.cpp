#include <algorithm>
#include <cmath>

#include "queryinterpreter.h"
#include "time/epoch.h"
#include "table.h"
#include "columns.h"

const int MAX_EXEC_COUNT = 1'000'000'000;
const int MAX_RECURSE_COUNT = 10;

openset::query::Interpreter::Interpreter(Macro_s& macros, const InterpretMode_e interpretMode):
	macros(macros),
	interpretMode(interpretMode)
{
	stack = new cvar[128];
	stackPtr = stack;
}

openset::query::Interpreter::~Interpreter()
{
	// if we have a result set, delete it
	//if (result)
	//	delete result;
	if (stack)
		delete []stack;
}

void openset::query::Interpreter::setResultObject(result::ResultSet* resultSet)
{
	result = resultSet;
}

void openset::query::Interpreter::configure()
{
	/* Configure the grid (only on first mount)
	 * 
	 * When we get our first mount we have access
	 * to the table and schema objects. With these
	 * we can prepare the grid to only expand
	 * columns used in the query and map those
	 * columns to the schema
	 */
	auto schema = grid->getTable()->getColumns();

	for (auto& cvar : macros.vars.tableVars)
	{
		const auto index = schema->getColumn(cvar.actual);

		if (index)
		{
			if (grid->isFullSchema())
			{
				cvar.column = grid->getGridColumn(cvar.schemaColumn);
				cvar.index = cvar.column;
				if (cvar.column == -1)
				{
					error.set(
						errors::errorClass_e::run_time,
						errors::errorCode_e::column_not_in_table,
						"column_nname: " + cvar.actual);
					return;
				}
			}
		}
		else
		{
			error.set(
				errors::errorClass_e::run_time,
				errors::errorCode_e::column_not_in_table,
				"column_name: " + cvar.actual);
			return;
		}
	}

	isConfigured = true;
}

vector<string> openset::query::Interpreter::getReferencedColumns() const
{
	vector<string> mappedColumns;

	// extract "actual" column names in query and put in
	// vector for mapTable call
	for (auto& c : macros.vars.tableVars)
		mappedColumns.push_back(c.actual);

	return mappedColumns;
}

void openset::query::Interpreter::mount(Person* person)
{
	/** \brief Mount a person object.
	 * 
	 * When the first person object is mounted we 
	 * will have access to the table and schema which 
	 * are referenced in the grid object. At this point
	 * we can call configure which will map the
	 * columns in this query to the grid, allowing for
	 * selective expansion of grid data when 
	 * grid::prepare is called		
	 */

	eventDistinct.clear();

	grid = person->getGrid(); // const
	blob = grid->getAttributeBlob();
	rows = grid->getRows(); // const
	rowCount = rows->size();

	if (person->getMeta())
	{
		uuid = person->getUUID();
		linid = person->getMeta()->linId;
	}

	stackPtr = stack;

	if (!isConfigured && rows->size())
		configure();
}

openset::query::SegmentList* openset::query::Interpreter::getSegmentList() const
{
	return &macros.segments;
}

void openset::query::Interpreter::marshal_tally(const int paramCount, const Col_s* columns, const int currentRow)
{

	if (paramCount <= 0)
		return;

	vector<cvar> params(paramCount);

	for (auto i = paramCount - 1 ; i >= 0; --i)
	{
		--stackPtr;

		// if any of these params are undefined, exit
		if (stackPtr->typeof() != cvar::valueType::STR &&
			*stackPtr == NONE)
			params[i] = NONE;
		else
			params[i] = std::move(*stackPtr);
	}

	if (!params.size())
		return;

	// strings, doubles, and bools are all ints internally,
	// this will ensure non-int types are represented as ints
	// during grouping
	const auto fixToInt = [&](const cvar& value) -> int64_t
	{
		switch (value.typeof())
		{
			case cvar::valueType::INT32:
			case cvar::valueType::INT64:
				return value.getInt64();

			case cvar::valueType::FLT:
			case cvar::valueType::DBL:
				return value.getDouble() * 10000;

			case cvar::valueType::STR:
				{
					auto hash = MakeHash(value.getString());
					result->addLocalText(hash, value.getString()); // cache this text
					return hash;
				}

			case cvar::valueType::BOOL:
				return value.getBool() ? 1 : 0;
			default:
				return NONE;
		}
	};
	

	const auto aggColumns = [&](result::Accumulator* resultColumns)
	{
		for (auto& resCol: macros.vars.columnVars)
		{
			if (!resCol.nonDistinct) // if the 'all' flag was NOT used on an aggregater
			{
				/* Yikes - a big gnarly tuple (hash can colide, tuple won't)
				 * 
				 *  What this is saying is for this column, and this value for
				 *  this branch in the result at this timestamp, have we ever ran
				 *  an aggregation? If not, run it, otherwise move on
				 */ 				
				
				distinctKey = ValuesSeenKey{
					resCol.index,
					resCol.modifier == Modifiers_e::var ? fixToInt(resCol.value) : columns->cols[resCol.distinctColumn],
					recast<int64_t>(resultColumns), // this pointer is unique to the ResultSet row
					resCol.schemaColumn == COL_UUID ? 0 : (resCol.modifier == Modifiers_e::dist_count_person ? 0 : columns->cols[COL_STAMP])
				};

				// emplace try will return false if the value exists
				if (!eventDistinct.emplaceTry(distinctKey, 1))
					continue; // we already tabulated this for this key
			}

			auto resultIndex = resCol.index + segmentColumnShift;

			switch (resCol.modifier)
			{
				case Modifiers_e::sum:
					if (columns->cols[resCol.column] != NONE)
					{
						if (resultColumns->columns[resultIndex].value == NONE)
							resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
						else
							resultColumns->columns[resultIndex].value += columns->cols[resCol.column];
					}
					break;

				case Modifiers_e::min:
					if (columns->cols[resCol.column] != NONE &&
							(resultColumns->columns[resultIndex].value == NONE ||
							 resultColumns->columns[resultIndex].value > columns->cols[resCol.column]))
						resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
					break;

				case Modifiers_e::max:
					if (columns->cols[resCol.column] != NONE &&
							(resultColumns->columns[resultIndex].value == NONE ||
							 resultColumns->columns[resultIndex].value < columns->cols[resCol.column]))
						resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
					break;

				case Modifiers_e::avg:
					if (columns->cols[resCol.column] != NONE)
					{
						if (resultColumns->columns[resultIndex].value == NONE)
						{
							resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
							resultColumns->columns[resultIndex].count = 1;
						}
						else
						{
							resultColumns->columns[resultIndex].value += columns->cols[resCol.column];
							resultColumns->columns[resultIndex].count++;
						}
					}
					break;
				case Modifiers_e::dist_count_person:
				case Modifiers_e::count:
					if (columns->cols[resCol.column] != NONE)
					{
						if (resultColumns->columns[resultIndex].value == NONE)
							resultColumns->columns[resultIndex].value = 1;
						else
							resultColumns->columns[resultIndex].value++;
					}
					break;

				case Modifiers_e::value:
					resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
					break;

				case Modifiers_e::var:
					if (resultColumns->columns[resultIndex].value == NONE)
						resultColumns->columns[resultIndex].value = fixToInt(resCol.value);
					else
						resultColumns->columns[resultIndex].value += fixToInt(resCol.value);
					break;
				default: break;
			}
		}
	};

	rowKey.clear();

	// run column lambdas!
	if (macros.vars.columnLambdas.size())
		for (auto lambdaIndex : macros.vars.columnLambdas)
			opRunner(// call the column lambda
				&macros.code.front() + lambdaIndex,
				currentRow);

	auto depth = 0;

	for (auto item : params)
    {

		if (item.typeof() != cvar::valueType::STR &&
			item == NONE)
			break;

	    rowKey.key[depth] = fixToInt(item);
			
		//result->setAtDepth(rowKey, set_cb);
		auto tPair = result->results.get(rowKey);

		if (!tPair)
		{
			const auto t = new (result->mem.newPtr(sizeof(openset::result::Accumulator))) openset::result::Accumulator();
			tPair = result->results.set(rowKey, t);
		}

		aggColumns(tPair->second);

	    ++depth;
	}

}

void openset::query::Interpreter::marshal_schedule(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"schedule doesn't have the correct number of parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	const auto functionHash = MakeHash(*stackPtr); // pop

	--stackPtr;
	auto scheduleAt = *stackPtr; // pop

	if (schedule_cb)
		schedule_cb(functionHash, scheduleAt);

	*stackPtr = NONE; // push
	++stackPtr;
}

void openset::query::Interpreter::marshal_emit(const int paramCount)
{
	if (paramCount != 1)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"emit doesn't have the correct number of parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	jobState = true;
	loopState = LoopState_e::in_exit;

	--stackPtr;
	auto emitMessage = *stackPtr; // pop

	if (emit_cb)
		emit_cb(emitMessage);

	*stackPtr = NONE; // push
	++stackPtr;
}

void __nestItercvar(const cvar* cvariable, string& result)
{
	if (cvariable->typeof() == cvar::valueType::DICT)
	{
		result += "{";

		auto idx = 0;
		for (auto& v: *cvariable->getDict())
		{
			if (idx)
				result += ", ";

			result += "\"" + v.first.getString() + "\": ";
			__nestItercvar(&v.second, result);

			++idx;
		}
		result += "}";
	}
	else if (cvariable->typeof() == cvar::valueType::LIST)
	{
		result += "[";

		auto idx = 0;
		for (auto& v : *cvariable->getList())
		{
			if (idx)
				result += ", ";
			__nestItercvar(&v, result);

			++idx;
		}
		result += "]";
	}
	else if (cvariable->typeof() == cvar::valueType::SET)
	{
		result += "(";

		auto idx = 0;
		for (auto& v : *cvariable->getSet())
		{
			if (idx)
				result += ", ";
			__nestItercvar(&v, result);
	
			++idx;
		}
		result += ")";
	}
	else if (cvariable->typeof() == cvar::valueType::STR)
	{
		result += "\"" + cvariable->getString() + "\"";
	}
	else
	{
		result += cvariable->getString();
	}
}

void openset::query::Interpreter::marshal_log(const int paramCount)
{
	vector<cvar> params;
	for (auto i = 0; i < paramCount; ++i)
	{
		--stackPtr;
		params.push_back(*stackPtr);
	}

	// print these in reverse order with reverse iterators
	for_each(params.rbegin(), params.rend(), [](auto& item)
         {
	         if (item.typeof() == cvar::valueType::DICT ||
				 item.typeof() == cvar::valueType::SET ||
		         item.typeof() == cvar::valueType::LIST)
	         {
		         string result;
		         __nestItercvar(&item, result);
		         cout << result << " ";
	         }
	         else
		         cout << item << " ";
         });

	cout << endl;

	*stackPtr = NONE;
	++stackPtr;
}

void openset::query::Interpreter::marshal_break(const int paramCount)
{
	if (paramCount > 1)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"break requires: no params, #, 'top' or 'all'");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	if (paramCount)
	{
		--stackPtr;
		auto param = *stackPtr;

		if (param == "all"s)
		{
			breakDepth = nestDepth;
			loopState = LoopState_e::in_break;
		}
		else if (param == "top"s)
		{
			breakDepth = nestDepth - 1;
			loopState = LoopState_e::in_break;
		}
		else
		{
			breakDepth = param;
			if (breakDepth > nestDepth || breakDepth < 0)
			{
				error.set(
					errors::errorClass_e::run_time,
					errors::errorCode_e::break_depth_to_deep,
					"break ## to deep for current nest level");
				return;
			}

			loopState = LoopState_e::in_break;
		}
	}
	else
	{
		breakDepth = 1;
		loopState = LoopState_e::in_break;
	}

	*stackPtr = NONE;
	++stackPtr;
}

void openset::query::Interpreter::marshal_dt_within(const int paramCount, const int64_t rowStamp)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"date within bad parameter count");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto compareStamp = stackPtr->getInt64();
	compareStamp = Epoch::fixMilli(compareStamp);

	--stackPtr;
	const auto milliseconds = stackPtr->getInt64();

	*stackPtr = within(compareStamp, rowStamp, milliseconds);
	++stackPtr;
}

void openset::query::Interpreter::marshal_dt_between(const int paramCount, const int64_t rowStamp)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"between clause requires two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto end_stamp = *stackPtr;

	--stackPtr;
	auto start_stamp = *stackPtr;

	// If it's a strong then convert stamp to unixtime
	if (start_stamp.typeof() == cvar::valueType::STR)
		start_stamp = Epoch::ISO8601ToEpoch(start_stamp);

	// If it's a strong then convert stamp to unixtime
	if (end_stamp.typeof() == cvar::valueType::STR)
		end_stamp = Epoch::ISO8601ToEpoch(end_stamp);

	if (start_stamp < 0 || end_stamp < 0)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"date error in between statement");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// make sure the stamp is in milliseconds
	start_stamp = Epoch::fixMilli(start_stamp);
	end_stamp = Epoch::fixMilli(start_stamp);
	
	*stackPtr = 
		(rowStamp >= start_stamp.getInt64() && 
 		rowStamp < end_stamp.getInt64()) ? 1 : 0;
	++stackPtr;
}

void openset::query::Interpreter::marshal_bucket(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"bucket takes two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	const int64_t bucket = (*stackPtr * 100);

	--stackPtr;
	int64_t value = (*stackPtr * 100);

	if (bucket != 0)
	{
		value = (static_cast<int64_t>(value / bucket) * bucket);
		if (bucket < 100)
			*stackPtr = round(value) / 100.0;
		else
			*stackPtr = value / 100;

	}
	else
		*stackPtr = 0;
	++stackPtr;
}

void openset::query::Interpreter::marshal_round(const int paramCount)
{
	if (paramCount != 1 && paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"round takes one or two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	int64_t places = 0;

	if (paramCount == 2)
	{
		--stackPtr;
		places = *stackPtr;
	}

	const double power = pow(10.0, places);
	*(stackPtr - 1) = round((stackPtr - 1)->getDouble() * power) / power;
}

void openset::query::Interpreter::marshal_fix(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"fix takes two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	const int64_t places = *stackPtr;
	double value = *(stackPtr - 1);

	const auto negative = value < 0;

	if (negative)
		value = std::abs(value);

	const auto power = places ? pow(10.0, places) : 1;
	const int64_t rounded = round(value * power);

	auto str = to_string(rounded);

	while (str.length() <= places)
		str = "0" + str;

	if (places)
		str.insert(str.end() - places, '.');

	if (negative)
		str = "-" + str;

	*(stackPtr - 1) = str;
}


void openset::query::Interpreter::marshal_makeDict(const int paramCount)
{
	cvar output;
	output.dict();

	if (!paramCount)
	{
		*stackPtr = move(output);
		++stackPtr;
		return;
	}

	if (paramCount % 2 == 1)
		throw("incorrect param count in dictionary");

	auto iter = stackPtr - paramCount;
	for (auto i = 0; i < paramCount; i += 2 , iter += 2)
		output[*iter] = move(*(iter + 1)); // same as: output[key] = value

	stackPtr -= paramCount;
	*stackPtr = move(output);
	++stackPtr;
}

void openset::query::Interpreter::marshal_makeList(const int paramCount)
{
	cvar output;
	output.list();

	auto outList = output.getList();

	auto iter = stackPtr - paramCount;
	for (auto i = 0; i < paramCount; ++i , ++iter)
		outList->emplace_back(*iter);

	stackPtr -= paramCount;
	*stackPtr = move(output);
	++stackPtr;
}

void openset::query::Interpreter::marshal_makeSet(const int paramCount)
{
	cvar output;
	output.set();

	auto outList = output.getSet();

	auto iter = stackPtr - paramCount;
	for (auto i = 0; i < paramCount; ++i, ++iter)
		outList->emplace(*iter);

	stackPtr -= paramCount;
	*stackPtr = move(output);
	++stackPtr;
}


void openset::query::Interpreter::marshal_population(const int paramCount)
{
	if (paramCount != 1)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"compliment takes one parameter");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto a = *stackPtr;

	// if we acquired IndexBits from getSegment_cb we must
	// delete them after we are done, or it'll leak
	auto aDelete = false;

	IndexBits* aBits = nullptr;

	if (getSegment_cb)
		aBits = getSegment_cb(a, aDelete);

	if (!aBits)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// copy then AND NOT to get the compliment of these two segments
	bits->opCopy(*aBits);

	if (aDelete)
		delete aBits;
}

void openset::query::Interpreter::marshal_intersection(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"intersection takes two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto b = *stackPtr;

	--stackPtr;
	auto a = *stackPtr;

	// if we acquired IndexBits from getSegment_cb we must
	// delete them after we are done, or it'll leak
	auto aDelete = false;
	auto bDelete = false;

	IndexBits* aBits = nullptr;

	if (getSegment_cb)
		aBits = getSegment_cb(a, aDelete);

	if (!aBits)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"intersection - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	IndexBits* bBits = nullptr;

	if (getSegment_cb)
		bBits = getSegment_cb(b, bDelete);

	if (!bBits)
	{
		if (aDelete)
			delete aBits;
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"intersection - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// copy then AND to get the intersection of these two segments
	bits->opCopy(*aBits);
	bits->opAnd(*bBits);
}

void openset::query::Interpreter::marshal_union(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"union takes two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto b = *stackPtr;

	--stackPtr;
	auto a = *stackPtr;

	// if we acquired IndexBits from getSegment_cb we must
	// delete them after we are done, or it'll leak
	auto aDelete = false;
	auto bDelete = false;

	IndexBits* aBits = nullptr;

	if (getSegment_cb)
		aBits = getSegment_cb(a, aDelete);

	if (!aBits)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	IndexBits* bBits = nullptr;

	if (getSegment_cb)
		bBits = getSegment_cb(b, bDelete);

	if (!bBits)
	{
		if (aDelete)
			delete aBits;

		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// copy then OR to get the union of these two segments
	bits->opCopy(*aBits);
	bits->opOr(*bBits);

	if (aDelete)
		delete aBits;
	if (bDelete)
		delete bBits;
}

void openset::query::Interpreter::marshal_compliment(const int paramCount)
{
	if (paramCount != 1)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"compliment takes one parameter");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto a = *stackPtr;

	// if we acquired IndexBits from getSegment_cb we must
	// delete them after we are done, or it'll leak
	auto aDelete = false;

	IndexBits* aBits = nullptr;

	if (getSegment_cb)
		aBits = getSegment_cb(a, aDelete);

	if (!aBits)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// copy then AND NOT to get the compliment of these two segments
	bits->opCopy(*aBits);
	bits->opNot();

	if (aDelete)
		delete aBits;
}

void openset::query::Interpreter::marshal_difference(const int paramCount)
{
	if (paramCount != 2)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::sdk_param_count,
			"difference takes two parameters");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	--stackPtr;
	auto b = *stackPtr;

	--stackPtr;
	auto a = *stackPtr;

	// if we acquired IndexBits from getSegment_cb we must
	// delete them after we are done, or it'll leak
	auto aDelete = false;
	auto bDelete = false;

	IndexBits* aBits = nullptr;

	if (getSegment_cb)
		aBits = getSegment_cb(a, aDelete);

	if (!aBits)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	IndexBits* bBits = nullptr;

	if (getSegment_cb)
		bBits = getSegment_cb(b, bDelete);

	if (!bBits)
	{
		if (aDelete)
			delete aBits;

		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::set_math_param_invalid,
			"compliment - set could not be found");
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	// copy then AND NOT to get the compliment of these two segments
	bits->opCopy(*aBits);
	bits->opAndNot(*bBits);

	if (aDelete)
		delete aBits;
	if (bDelete)
		delete bBits;
}

void openset::query::Interpreter::marshal_slice(const int paramCount) 
{
	if (paramCount != 3)
		throw std::runtime_error("slice [:] malformed");
	if ((stackPtr - 3)->typeof() != cvar::valueType::REF)
		throw std::runtime_error("slice [:] first parameter must be reference type");

	const auto reference = stackPtr - 3;
	auto startIndex = (stackPtr - 2)->getInt64();
	auto endIndex = (stackPtr - 1)->getInt64();

	stackPtr -= 2; // we will return our result at this position

	// local function to sort our missing, negative, or out of range indexes
	const auto fixIndexes = [](size_t valueLength, int64_t& startIndex, int64_t& endIndex)
	{
		if (startIndex == NONE)
			startIndex = 0;
		else if (startIndex < 0)
			startIndex = valueLength + startIndex;

		if (endIndex == NONE)
			endIndex = valueLength;
		else if (endIndex < 0)
			endIndex = valueLength + endIndex;

		if (endIndex < 0)
			endIndex = 0;
		if (endIndex > valueLength)
			endIndex = valueLength;
		if (startIndex < 0)
			startIndex = 0;
		if (startIndex > valueLength)
			startIndex = valueLength;

		// we will swap for a valid range if they are reversed for whatever reason
		if (endIndex < startIndex)
			std::swap(startIndex, endIndex);

	};

	const auto refType = reference->getReference()->typeof();

	if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET)
		throw std::runtime_error("slice [:] expecting list, string or convertable type");

	// SET and DICT will merge, LIST will append, so you can append objects
	if (refType == cvar::valueType::LIST)
	{
		const auto value = reference->getReference()->getList();
		const auto valueLength = value->size();
		
		fixIndexes(valueLength, startIndex, endIndex);

		cvar result(cvar::valueType::LIST);
		auto resultList = result.getList();

		resultList->insert(resultList->begin(), value->begin() + startIndex, value->begin() + endIndex);
		*(stackPtr - 1) = std::move(result);
	}
	else
	{
		auto value = reference->getReference()->getString(); // convert type of needed
		const auto valueLength = value.length();

		fixIndexes(valueLength, startIndex, endIndex);

		cvar result(cvar::valueType::STR);
		const auto resultString = result.getStringPtr(); // grab the actual pointer to the string in the cvar

		*resultString = value.substr(startIndex, endIndex - startIndex);
		*(stackPtr - 1) = std::move(result);
	}
		
}

void openset::query::Interpreter::marshal_find(const int paramCount, const bool reverse) 
{
	if (paramCount < 2)
		throw std::runtime_error("find malformed");
	if ((stackPtr - paramCount)->typeof() != cvar::valueType::REF)
		throw std::runtime_error(".find first parameter must be reference type");

	auto count = paramCount;

	size_t length = 0;
	if (count == 4)
	{
		length = (stackPtr - 1)->getInt32();
		--stackPtr;
		--count;
	}

	size_t firstPos = 0;
	if (count == 3)
	{
		firstPos = (stackPtr - 1)->getInt32();
		--stackPtr;
	}
	
	const auto reference = stackPtr - 2;
	const auto lookFor = stackPtr - 1;
	const auto refType = reference->getReference()->typeof();

	auto str = reference->getReference()->getString();

	// python allows a second index, C++ takes a length, lets convert
	if (length && !reverse)
		str = str.substr(0, length);
	
	if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
		throw std::runtime_error(".find expecting string or convertable type");

	const auto pos = (reverse) ? 
		str.rfind(lookFor->getString()) : 
	    str.find(lookFor->getString(), firstPos);
	
	*(stackPtr - 1) = (pos == std::string::npos) ? -1 : static_cast<int32_t>(pos);
}

void openset::query::Interpreter::marshal_split(const int paramCount) const
{
	if (paramCount != 2)
		throw std::runtime_error("split malformed");
	if ((stackPtr - paramCount)->typeof() != cvar::valueType::REF)
		throw std::runtime_error(".split first parameter must be reference type");

	const auto reference = stackPtr - 2;
	const auto lookFor = stackPtr - 1;
	const auto refType = reference->getReference()->typeof();

	if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
		throw std::runtime_error(".find expecting string or convertable type");

	const auto source = reference->getReference()->getString();
	const auto search = lookFor->getString();
	const auto searchLen = search.length();

	// make a result list
	cvar result(cvar::valueType::LIST);

	size_t startIdx = 0;
	
	while (true)
	{
		const auto pos = source.find(search, startIdx);

		if (pos == std::string::npos)
			break;		

		result += source.substr(startIdx, pos - startIdx);
		startIdx = pos + searchLen;
	}

	auto cleanup = source.substr(startIdx);
	if (cleanup.length())
		result += cleanup;

	// leave the List on the stack
	*(stackPtr - 1) = std::move(result);
}

void openset::query::Interpreter::marshal_strip(const int paramCount) const
{
	if (paramCount != 1)
		throw std::runtime_error("strip malformed");
	if ((stackPtr - paramCount)->typeof() != cvar::valueType::REF)
		throw std::runtime_error(".strip first parameter must be reference type");

	const auto reference = stackPtr - 1;
	const auto refType = reference->getReference()->typeof();

	if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
		throw std::runtime_error(".strip expecting string or convertable type");

	auto text = reference->getReference()->getString();

	const auto whiteSpace = " \t\n\r"s;

	if (const auto start = text.find_first_not_of(whiteSpace);  
	    start == text.find_last_not_of(whiteSpace) && start == std::string::npos)
		text.clear();
	else
	{
		if (const auto end = text.find_last_not_of(whiteSpace); end != std::string::npos)
			text.resize(end + 1);

		if (start != std::string::npos)
			text = text.substr(start);
	}

	// leave the List on the stack
	*(stackPtr - 1) = text;
}

void openset::query::Interpreter::marshal_url_decode(const int paramCount) const
{
	if (paramCount != 1)
		throw std::runtime_error("url_decode malformed");

	const auto paramType = (stackPtr - 1)->typeof();

	if (paramType == cvar::valueType::DICT || paramType == cvar::valueType::SET || paramType == cvar::valueType::LIST)
		throw std::runtime_error(".url_decode expecting string or convertable type");

	const auto url = (stackPtr - 1)->getString();

	auto& result = *(stackPtr - 1);
	result.dict();

	result["host"] = NONE;
	result["path"] = NONE;
	result["query"] = NONE;
	result["params"] = cvar::Dict{};

	size_t start = 0;

	// look for the // after the protocal, if its here we have a host
	if (auto slashSlash = url.find("//"); slashSlash != std::string::npos)
	{
		slashSlash += 2;
		const auto endPos = url.find("/", slashSlash);
		if (endPos == std::string::npos) // something is ugly
			return;
		result["host"] = url.substr(slashSlash, endPos - (slashSlash));

		start = endPos;
	}

	// we have a question mark
	if (auto qpos = url.find("?", start); qpos != std::string::npos)
	{
		result["path"] = url.substr(start, qpos - start);
		++qpos;
		auto query = url.substr(qpos);
		result["query"] = query;
		
		start = 0;

		while (true)
		{
			auto pos = query.find("&", start);

			if (pos == std::string::npos)
				pos = query.size();

			auto param = query.substr(start, pos - start);

			if (const auto ePos = param.find("="); ePos != std::string::npos)
			{
				const auto key = param.substr(0, ePos);
				const auto value = param.substr(ePos + 1);
				result["params"][key] = value;
			}
			else
				result["params"][param] = true;

			start = pos + 1;  // move past the '&'

			if (start >= query.length()) break;
		}
	}
	else
		result["path"] = url.substr(start);

	*(stackPtr - 1) == std::move(result);
	
}

string openset::query::Interpreter::getLiteral(const int64_t id) const
{
	for (auto& i: macros.vars.literals)
	{
		if (i.hashValue == id)
			return i.value;
	}
	return "";
}

bool openset::query::Interpreter::marshal(Instruction_s* inst, int& currentRow)
{
	// index maps to function in the enumerator marshals_e
	// extra maps to the param count (items on stack)

	// note: param order is reversed, last item on the stack
	// is also last param in function call
	switch (cast<Marshals_e>(inst->index))
	{
	case Marshals_e::marshal_tally:
	{
		if (interpretMode == InterpretMode_e::count)
		{
			if (bits)
				bits->bitSet(linid);
			loopState = LoopState_e::in_exit;
			*stackPtr = 0;
			++stackPtr;
			return true;
		}
		marshal_tally(inst->extra, (*rows)[currentRow], currentRow);
	}
	break;
	case Marshals_e::marshal_now:
		*stackPtr = Now();
		++stackPtr;
		break;
	case Marshals_e::marshal_event_time:
		*stackPtr = (*rows)[currentRow]->cols[COL_STAMP];
		++stackPtr;
		break;
	case Marshals_e::marshal_last_event:
		*stackPtr = rows->back()->cols[COL_STAMP];
		++stackPtr;
		break;
	case Marshals_e::marshal_first_event:
		*stackPtr = rows->front()->cols[COL_STAMP];
		++stackPtr;
		break;
	case Marshals_e::marshal_prev_match:
		*stackPtr = (matchStampPrev.size() > 1) ? *(matchStampPrev.end() - 2) : matchStampTop;
		++stackPtr;
		break;
	case Marshals_e::marshal_first_match:
		*stackPtr = matchStampTop;
		++stackPtr;
		break;
	case Marshals_e::marshal_bucket:
		marshal_bucket(inst->extra);
		break;
	case Marshals_e::marshal_round:
		marshal_round(inst->extra);
		break;
	case Marshals_e::marshal_fix:
		marshal_fix(inst->extra);
		break;
	case Marshals_e::marshal_trunc:
		*(stackPtr - 1) = (stackPtr - 1)->getInt64(); // always trucates a float
		break;
	case Marshals_e::marshal_to_seconds:
		*(stackPtr - 1) /= int64_t(1'000); // in place
		break;
	case Marshals_e::marshal_to_minutes:
		*(stackPtr - 1) /= int64_t(60'000); // in place							
		break;
	case Marshals_e::marshal_to_hours:
		*(stackPtr - 1) /= int64_t(3'600'000); // in place							
		break;
	case Marshals_e::marshal_to_days:
		*(stackPtr - 1) /= int64_t(86'400'000); // in place							
		break;
	case Marshals_e::marshal_get_second:
		*(stackPtr - 1) = Epoch::epochSecondNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_second:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochSecondDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_minute:
		*(stackPtr - 1) = Epoch::epochMinuteNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_minute:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochMinuteDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_hour:
		*(stackPtr - 1) = Epoch::epochHourNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_hour:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochHourDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_round_day:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochDayDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_day_of_week:
		*(stackPtr - 1) = Epoch::epochDayOfWeek(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_get_day_of_month:
		*(stackPtr - 1) = Epoch::epochDayOfMonth(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_get_day_of_year:
		*(stackPtr - 1) = Epoch::epochDayOfYear(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_week:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochWeekDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_month:
		*(stackPtr - 1) = Epoch::epochMonthNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_month:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochMonthDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_quarter:
		*(stackPtr - 1) = Epoch::epochQuarterNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_quarter:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochQuarterDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_get_year:
		*(stackPtr - 1) = Epoch::epochYearNumber(*(stackPtr - 1));
		break;
	case Marshals_e::marshal_round_year:
		*(stackPtr - 1) = Epoch::fixMilli(Epoch::epochYearDate(*(stackPtr - 1)));
		break;
	case Marshals_e::marshal_iter_get:
		*stackPtr = currentRow;
		++stackPtr;
		break;
	case Marshals_e::marshal_iter_set:
		currentRow = (stackPtr - 1)->getInt64();
		if (currentRow < 0 || currentRow >= rows->size())
			throw std::runtime_error("row iterator out of range");
		rows->begin() + currentRow;
		--stackPtr;
		break;
	case Marshals_e::marshal_iter_move_first:
		currentRow = 0;
		rows->begin() + currentRow;
		break;
	case Marshals_e::marshal_iter_move_last:
		currentRow = rows->size() - 1;
		if (currentRow < 0)
			throw std::runtime_error("iter_set_last called on empty set");
		rows->begin() + currentRow;
		break;
	case Marshals_e::marshal_iter_next:
	{
		/* 
		 * Will advance the event if it can, else it will
		 * exit any outerloop that it is in. If it is in the
		 * main body, it will exit the current query silently (without error)
		 */

		const auto currentGrp = HashPair((*rows)[currentRow]->cols[COL_STAMP], (*rows)[currentRow]->cols[COL_ACTION]); // use left to hold lastRowId
											   // an "event" can contian many rows so,
											   // watch the group column and iterate 
											   // until the row group changes
		while (currentRow < rowCount)
		{
			++currentRow;
			if (currentRow == rowCount ||
				currentGrp != HashPair((*rows)[currentRow]->cols[COL_STAMP], (*rows)[currentRow]->cols[COL_ACTION]))
				break;
		}

		if (currentRow == rowCount) // end of set
		{
			if (nestDepth)
			{
				breakDepth = 1;
				loopState = LoopState_e::in_break;
				*stackPtr = 0;
				++stackPtr;
			}
			else
			{
				loopState = LoopState_e::in_exit;
				*stackPtr = 0;
				++stackPtr;
			}
			--recursion;
			return true;
		}
	}
	break;
	case Marshals_e::marshal_event_count:

		if (eventCount == -1)
		{
			int64_t currentGrp = 0;
			auto countIter = rows->begin();
			eventCount = 0;

			while (countIter != rows->end())
			{
				if (currentGrp != HashPair((*countIter)->cols[COL_STAMP], (*countIter)->cols[COL_ACTION]))
				{
					currentGrp = HashPair((*countIter)->cols[COL_STAMP], (*countIter)->cols[COL_ACTION]);
					++eventCount;
				}
				++countIter;
			}
		}
		*stackPtr = eventCount;
		++stackPtr;
		break;
	case Marshals_e::marshal_iter_prev:
		break;
	case Marshals_e::marshal_iter_within:
		marshal_dt_within(inst->extra, (*rows)[currentRow]->cols[COL_STAMP]);
		break;
	case Marshals_e::marshal_iter_between:
		marshal_dt_between(inst->extra, (*rows)[currentRow]->cols[COL_STAMP]);
		break;
	case Marshals_e::marshal_population:
		marshal_population(inst->extra);
		break;
	case Marshals_e::marshal_intersection:
		marshal_intersection(inst->extra);
		break;
	case Marshals_e::marshal_union:
		marshal_union(inst->extra);
		break;
	case Marshals_e::marshal_compliment:
		marshal_compliment(inst->extra);
		break;
	case Marshals_e::marshal_difference:
		marshal_difference(inst->extra);
		break;
	case Marshals_e::marshal_return:
		// return will have its params on the stack,
		// we will just leave these on the stack and
		// break out of this block... magic!
		--recursion;
		return true;
	case Marshals_e::marshal_break:
		marshal_break(inst->extra);
		break;
	case Marshals_e::marshal_continue:
		loopState = LoopState_e::in_continue;
		*stackPtr = 0;
		++stackPtr;
		break;
	case Marshals_e::marshal_log:
		marshal_log(inst->extra);
		break;
	case Marshals_e::marshal_emit:
		marshal_emit(inst->extra);
		break;
	case Marshals_e::marshal_schedule:
		marshal_schedule(inst->extra);
		break;
	case Marshals_e::marshal_debug:
		--stackPtr;
		debugLog.push_back(*stackPtr);
		break;
	case Marshals_e::marshal_exit:
		loopState = LoopState_e::in_exit;
		*stackPtr = 0;
		++stackPtr;
		--recursion;
		return true;
	case Marshals_e::marshal_init_dict:
		(*stackPtr).dict();
		++stackPtr;
		break;
	case Marshals_e::marshal_init_list:
		(*stackPtr).list();
		++stackPtr;
		break;
	case Marshals_e::marshal_make_dict:
		marshal_makeDict(inst->extra);
		break;
	case Marshals_e::marshal_make_list:
		marshal_makeList(inst->extra);
		break;
	case Marshals_e::marshal_set:
		if (!inst->extra) // no params is just init
		{
			(*stackPtr).set();
			++stackPtr;
		}
		else // params means we are making a filled set
		{
			marshal_makeSet(inst->extra);
		}
		break;
	case Marshals_e::marshal_list:
		if (!inst->extra) // no params is just init
		{
			(*stackPtr).list();
			++stackPtr;
		}
		else // params means we are making a filled set
		{
			marshal_makeList(inst->extra);
		}
		break;
	case Marshals_e::marshal_dict:
		if (!inst->extra) // no params is just init
		{
			(*stackPtr).dict();
			++stackPtr;
		}
		else // params means we are making a filled set
		{
			marshal_makeDict(inst->extra);
		}
		break;
	case Marshals_e::marshal_int:
		*(stackPtr - 1) = (stackPtr - 1)->getInt64();
		break;
	case Marshals_e::marshal_float:
		*(stackPtr - 1) = (stackPtr - 1)->getDouble();
		break;
	case Marshals_e::marshal_str:
		*(stackPtr - 1) = (stackPtr - 1)->getString();
		break;
	case Marshals_e::marshal_len:
		*(stackPtr - 1) = (stackPtr - 1)->len();
		break;
	case Marshals_e::marshal_append:
	case Marshals_e::marshal_update:
	case Marshals_e::marshal_add:
		if (inst->extra != 2)
			throw std::runtime_error(".append/.update requires parameters");
		if ((stackPtr - 2)->typeof() != cvar::valueType::REF)
			throw std::runtime_error(".append/.update first parameter must be reference type");

		// SET and DICT will merge, LIST will append, so you can append objects
		if ((stackPtr - 2)->getReference()->typeof() == cvar::valueType::LIST)
			(stackPtr - 2)->getReference()->getList()->push_back(*(stackPtr - 1));
		else
			*(stackPtr - 2)->getReference() += *(stackPtr - 1);
		stackPtr -= 2;
		break;
	case Marshals_e::marshal_remove:
	case Marshals_e::marshal_del:
		if (inst->extra != 2)
			throw std::runtime_error("del requires parameters");
		if ((stackPtr - 2)->typeof() != cvar::valueType::REF)
			throw std::runtime_error("del first parameter must be reference type");
		*(stackPtr - 2)->getReference() -= *(stackPtr - 1);
		stackPtr -= 2;
		break;
	case Marshals_e::marshal_contains:
		if (inst->extra != 2)
			throw std::runtime_error("contain requires parameters (malformed in clause)");
		*(stackPtr - 2) = (stackPtr - 1)->contains(*(stackPtr - 2));
		--stackPtr;
		break;
	case Marshals_e::marshal_not_contains:
		if (inst->extra != 2)
			throw std::runtime_error("not_contains requires parameters (malformed not in clause)");
		*(stackPtr - 2) = !((stackPtr - 1)->contains(*(stackPtr - 2)));
		--stackPtr;
		break;
	case Marshals_e::marshal_pop:
		if (inst->extra != 1)
			throw std::runtime_error("pop requires reference parameter");
		{
			auto var = (stackPtr - 1)->getReference();
			const auto res = (stackPtr - 1);
			if (var->typeof() == cvar::valueType::LIST)
			{
				if (!var->getList() || var->getList()->size() == 0)
				{
					*res = NONE;
					break;
				}
				*res = std::move(var->getList()->back());
				var->getList()->pop_back();
			}
			else if (var->typeof() == cvar::valueType::DICT)
			{
				if (!var->getDict() || var->getDict()->size() == 0)
				{
					*res = NONE;
					break;
				}
				auto value = var->getDict()->begin();
				var->getDict()->erase(value);
				var->dict(); // result is a Dict
				(*res) = std::move(cvar::o{ value->first, value->second });
			}
			else if (var->typeof() == cvar::valueType::SET)
			{
				if (!var->getSet() || var->getSet()->size() == 0)
				{
					*res = NONE;
					break;
				}
				auto value = *var->getSet()->begin();
				var->getSet()->erase(value);
				*res = std::move(value);
			}
			else
				throw std::runtime_error("pop can only be performed on set or list types");
		}
		break;
	case Marshals_e::marshal_clear:
		if (inst->extra != 1)
			throw std::runtime_error("pop requires reference parameter");
		{
			const auto var = (stackPtr - 1)->getReference();
			const auto res = (stackPtr - 1);
			if (var->typeof() == cvar::valueType::LIST)
			{
				auto value = var->getList()->front();
				var->getList()->erase(var->getList()->begin());
				*res = std::move(value);
			}
			else if (var->typeof() == cvar::valueType::SET)
			{
				auto value = *var->getSet()->begin();
				var->getSet()->erase(value);
				*res = std::move(value);
			}
			else
				throw std::runtime_error("pop can only be performed on set or list types");
		}
		break;
	case Marshals_e::marshal_keys:
		if (inst->extra != 1)
			throw std::runtime_error("keys requires reference parameter");
		{
			const auto var = (stackPtr - 1)->getReference();
			auto res = (stackPtr - 1);
			if (var->typeof() == cvar::valueType::DICT)
			{
				res->list(); // result is a Dict
				for (const auto v : *var->getDict())
					(*res->getList()).push_back(v.first);
			}
			else
				throw std::runtime_error("keys can only be performed on dict types");
		}
		break;
	case Marshals_e::marshal_session_count: 
		if (macros.sessionColumn == -1)
			throw std::runtime_error("session column could not be found");
		++stackPtr;
		*(stackPtr - 1) = rows->back()->cols[macros.sessionColumn];
		break;
	case Marshals_e::marshal_str_split: 
		marshal_split(inst->extra);
		break;
	case Marshals_e::marshal_str_find: 
		marshal_find(inst->extra);
		break;
	case Marshals_e::marshal_str_rfind:
		marshal_find(inst->extra, true);
		break;
	case Marshals_e::marshal_str_slice:
		marshal_slice(inst->extra);
		break;
	case Marshals_e::marshal_str_strip:
		marshal_strip(inst->extra);
		break;
	case Marshals_e::marshal_range:
		throw std::runtime_error("range is not implemented");
		break;
	case Marshals_e::marshal_url_decode:
		marshal_url_decode(inst->extra);
		break;

	default:
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::missing_function_definition,
			inst->debug.toStr());
		--recursion;
		break;
	}

	return false; 
}

void openset::query::Interpreter::opRunner(Instruction_s* inst, int currentRow)
{
	// count allows for now row pointer, and no mounted person
	if ((!rows || rows->empty()) && interpretMode != InterpretMode_e::count)
	{
		loopState = LoopState_e::in_exit;
		*stackPtr = NONE;
		++stackPtr;
		return;
	}

	//auto rowIter = rows->begin() + currentRow;

	if (++recursion > MAX_RECURSE_COUNT)
	{
		error.set(
			errors::errorClass_e::run_time,
			errors::errorCode_e::recursion,
			"nesting depth was: " + to_string(recursion),
			lastDebug->toStrShort());
		loopState = LoopState_e::in_exit;

		*stackPtr = NONE;
		++stackPtr;

		--recursion;
		return;
	}

	while (loopState == LoopState_e::run && !error.inError())
	{
		// tracks the last known script line number
		if (inst->debug.number)
			lastDebug = &inst->debug;

		/* TODO LONG RUN CHECK --- we will do this differently
		if (++loopCount > MAX_EXEC_COUNT)
		{
			error.set(
				errors::errorClass_e::run_time,
				errors::errorCode_e::exec_count_exceeded,
				"exec_count: " + to_string(loopCount),
				lastDebug->toStrShort());
			loopState = loopState_e::in_exit;

			*stackPtr = NONE;
			++stackPtr;

			--recursion;
			return;
		}
		*/

		switch (inst->op)
		{
			case OpCode_e::NOP:
				// do nothing... nothing to see here... move on
				break;
			case OpCode_e::PSHTBLCOL:
				// push a column value
				switch (macros.vars.tableVars[inst->index].schemaType)
				{
					case columnTypes_e::freeColumn:
						*stackPtr = NONE;
						break;
					case columnTypes_e::intColumn:
						*stackPtr = (*rows)[currentRow]->cols[macros.vars.tableVars[inst->index].column];
						break;
					case columnTypes_e::doubleColumn:
						*stackPtr = (*rows)[currentRow]->cols[macros.vars.tableVars[inst->index].column] / 10000.0;
						break;
					case columnTypes_e::boolColumn:
						*stackPtr = (*rows)[currentRow]->cols[macros.vars.tableVars[inst->index].column] ? true : false;
						break;
					case columnTypes_e::textColumn:
						{
							const auto text = grid->getAttributes()->blob->getValue(
								macros.vars.tableVars[inst->index].schemaColumn,
								(*rows)[currentRow]->cols[macros.vars.tableVars[inst->index].column]);

							if (text)
								*stackPtr = text;
							else
								*stackPtr = (*rows)[currentRow]->cols[macros.vars.tableVars[inst->index].column];
						}
						break;
					default:
						break;
				}

				++stackPtr;
				break;
			case OpCode_e::VARIDX:
				*stackPtr = inst->index;
				++stackPtr;
				break;
			case OpCode_e::PSHPAIR:
				// we are simply going to pop two items off the stack (the key, then the value)
				// and assign these to a new Dictionary (a dictionary with one pair)
				{
					--stackPtr;
					const auto key = std::move(*stackPtr);
					--stackPtr;
					const auto value = std::move(*stackPtr);
					(*stackPtr).dict();
					(*stackPtr)[key] = std::move(value);
					++stackPtr;
				}
				break;
			case OpCode_e::PSHRESCOL:
				// push a select value onto stack
				{
					if (macros.vars.columnVars[inst->index].modifier != Modifiers_e::var)
					{
						// push mapped column value into 
						// TODO range check
						*stackPtr = (*rows)[currentRow]->cols[macros.vars.columnVars[inst->index].column];
						++stackPtr;
					}
					else
					{
						*stackPtr = macros.vars.columnVars[inst->index].value;
						++stackPtr;
					}
				}
				break;
			case OpCode_e::PSHUSROBJ:
				{
					auto* tcvar = &macros.vars.userVars[inst->index].value;

					for (auto x = 0; x < inst->extra; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					// this is the value
					*stackPtr = std::move(tcvar ? *tcvar : cvar{NONE});
					++stackPtr;
				}
				break;
			case OpCode_e::PSHUSROREF:
			{
				auto* tcvar = &macros.vars.userVars[inst->index].value;

				for (auto x = 0; x < inst->extra; ++x)
				{
					--stackPtr;
					tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
				}

				// this is the value
				stackPtr->setReference(tcvar);
				++stackPtr;
			}
			break;
			case OpCode_e::PSHUSRVAR:
				// push a cvar							
				*stackPtr = macros.vars.userVars[inst->index].value;
				++stackPtr;
				break;
			case OpCode_e::PSHUSRVREF:
				// push a cvar							
				stackPtr->setReference(&macros.vars.userVars[inst->index].value);
				++stackPtr;
				break;
			case OpCode_e::PSHLITTRUE:
				// push boolean true						
				*stackPtr = true;
				++stackPtr;
				break;
			case OpCode_e::PSHLITFALSE:
				// push boolean false
				*stackPtr = false;
				++stackPtr;
				break;
			case OpCode_e::PSHLITSTR:
				// push a string value
				//result->addLocalText(macros.vars.literals[inst->index].value);
				*stackPtr = macros.vars.literals[inst->index].value; // WAS hashValue
				++stackPtr;
				break;
			case OpCode_e::PSHLITINT:
				// push a numeric value
				*stackPtr = inst->value;
				++stackPtr;
				break;
			case OpCode_e::PSHLITFLT:
				// push a floating point value
				*stackPtr = cast<double>(inst->value) / cast<double>(1'000'000);
				++stackPtr;
				break;
			case OpCode_e::PSHLITNUL:
				// push a floating point value
				*stackPtr = NONE;
				++stackPtr;
				break;
			case OpCode_e::POPUSROBJ:
				{
					auto* tcvar = &macros.vars.userVars[inst->index].value;

					for (auto x = 0; x < inst->extra - 1; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					--stackPtr;
					const auto key = std::move(*stackPtr);

					// this is the value
					--stackPtr;
					(*tcvar)[key] = std::move(*stackPtr);
				}
				break;
			case OpCode_e::POPUSRVAR:
				// pop stack into cvar
				--stackPtr;
				macros.vars.userVars[inst->index].value = std::move(*stackPtr);
				break;
			case OpCode_e::POPTBLCOL:
				// pop stack into column value
				// NOTE: we don't actually do this
				break;
			case OpCode_e::POPRESCOL:
				// pop stack into select
				if (macros.vars.columnVars[inst->index].modifier == Modifiers_e::var)
				{
					--stackPtr;
					macros.vars.columnVars[inst->index].value = *stackPtr;
				}
				else
				{
					// TODO - can we do anything to the data? probably not					
				}
				break;
			case OpCode_e::CNDIF:
				// execute lambda, and if not 0 on stack
				// next a block.
				// IF is implemented with a lambda. If it returns true
				// the corresponding code block is run. After this
				// it most proceed to the code line with the 
				// first non-ELSE/ELIF
				opRunner(// call condition lambda
					&macros.code.front() + inst->extra,
					currentRow); // anything not 0 is true

				--stackPtr;

				if (stackPtr->isEvalTrue())
				{
					// PASSED - run the code block (recursive)
					opRunner(
						&macros.code.front() + inst->index,
						currentRow);

					// fast forward passed subsequent elif/else ops
					++inst;

					while (inst->op == OpCode_e::CNDELIF ||
						inst->op == OpCode_e::CNDELSE)
						++inst;

					// we've advanced the instruction pointer
					// loop to top
					continue;
				}
				break;
			case OpCode_e::CNDELIF:
				// ELIF always follows an IF
				// if a match is made the execution pointer after
				// nesting must move to the first non-ELSE/ELIF

				opRunner(// call condition lambda
					&macros.code.front() + inst->extra,
					currentRow); // anything not 0 is true

				--stackPtr;

				//if (stackPtr->getInt64() != NONE && *stackPtr)
				if (stackPtr->isEvalTrue())
				{
					// PASSED - run the code block (recursive)
					opRunner(
						&macros.code.front() + inst->index,
						currentRow);

					// fast forward passed subsequent elif/else ops
					++inst;

					while (inst->op == OpCode_e::CNDELIF ||
						inst->op == OpCode_e::CNDELSE)
						++inst;

					// we've advanced the instruction pointer
					// loop to top
					continue;
				}

				break;
			case OpCode_e::CNDELSE:
				// ELSE block be executed only when all if/elif blocks fail
				// DEFAULTED - run the code block (recursive)
				opRunner(
					&macros.code.front() + inst->index,
					currentRow);
				break;
			case OpCode_e::ITFOR:
				{
					--stackPtr;
					const int64_t keyIdx = *stackPtr;

					int64_t valueIdx = 0;
					if (inst->value == 2)
					{
						--stackPtr;
						valueIdx = *stackPtr;
					}

					--stackPtr;
					auto source = move(*stackPtr);

					if (source.typeof() == cvar::valueType::DICT)
					{
						// enter loop, increment nest 
						++nestDepth;

						const auto from = source.getDict();

						for (auto& x : *from)
						{
							if (loopState == LoopState_e::in_exit || error.inError())
							{
								*stackPtr = 0;
								++stackPtr;

								--nestDepth;

								matchStampPrev.pop_back();
								--recursion;
								return;
							}

							macros.vars.userVars[keyIdx].value = x.first;

							if (inst->value == 2)
								macros.vars.userVars[valueIdx].value = x.second;

							opRunner(
								&macros.code.front() + inst->index,
								currentRow);

							if (loopState == LoopState_e::in_break)
							{
								if (breakDepth == 1 || nestDepth == 1)
								{
									loopState = LoopState_e::run;
								}
								else
								{
									--nestDepth;
									--recursion;
								}

								--breakDepth;

								if (breakDepth == 0)
									break;

								matchStampPrev.pop_back();
								return;
							}

							if (loopState == LoopState_e::in_continue)
								loopState = LoopState_e::run; // no actual action, we are going to loop anyways
						}

						// out of loop, decrement nest 
						--nestDepth;
					}
					else if (source.typeof() == cvar::valueType::LIST)
					{
						const auto from = source.getList();

						for (auto& x: *from)
						{
							if (loopState == LoopState_e::in_exit || error.inError())
							{
								*stackPtr = 0;
								++stackPtr;

								--nestDepth;

								matchStampPrev.pop_back();
								--recursion;
								return;
							}

							macros.vars.userVars[keyIdx].value = x;

							opRunner(
								&macros.code.front() + inst->index,
								currentRow);

							if (loopState == LoopState_e::in_break)
							{
								if (breakDepth == 1 || nestDepth == 1)
								{
									loopState = LoopState_e::run;
								}
								else
								{
									--nestDepth;
									--recursion;
								}

								--breakDepth;

								if (breakDepth == 0)
									break;

								matchStampPrev.pop_back();
								return;
							}

							if (loopState == LoopState_e::in_continue)
								loopState = LoopState_e::run; // no actual action, we are going to loop anyways
						}
					}
					else if (source.typeof() == cvar::valueType::SET)
					{
						const auto from = source.getSet();

						for (auto& x : *from)
						{
							if (loopState == LoopState_e::in_exit || error.inError())
							{
								*stackPtr = 0;
								++stackPtr;

								--nestDepth;

								matchStampPrev.pop_back();
								--recursion;
								return;
							}

							macros.vars.userVars[keyIdx].value = x;

							opRunner(
								&macros.code.front() + inst->index,
								currentRow);

							if (loopState == LoopState_e::in_break)
							{
								if (breakDepth == 1 || nestDepth == 1)
								{
									loopState = LoopState_e::run;
								}
								else
								{
									--nestDepth;
									--recursion;
								}

								--breakDepth;

								if (breakDepth == 0)
									break;

								matchStampPrev.pop_back();
								return;
							}

							if (loopState == LoopState_e::in_continue)
								loopState = LoopState_e::run; // no actual action, we are going to loop anyways
						}
					}
					else
					{
						error.set(
							errors::errorClass_e::run_time,
							errors::errorCode_e::iteration_error,
							inst->debug.toStr());
						loopState = LoopState_e::in_exit;
						--recursion;
						return;
					}
				}
				break;
			case OpCode_e::ITNEXT:
				// fancy and strange stuff happens here					
				{
					auto iterCount = 0;
					cvar lambda;
					auto rowGrp = HashPair((*rows)[currentRow]->cols[COL_STAMP], (*rows)[currentRow]->cols[COL_ACTION]); // use left to hold lastRowId

					// enter loop, increment nest 
					++nestDepth;

					// store the time stamp of the last match
					matchStampPrev.push_back((*rows)[currentRow]->cols[0]);

					// user right for count
					for (const auto rowCount = rows->size();
					     iterCount < inst->value && currentRow < rowCount;
					     ++currentRow)
					{
						//++loopCount;

						if (loopState == LoopState_e::in_exit || error.inError())
						{
							*stackPtr = 0;
							++stackPtr;

							--nestDepth;

							matchStampPrev.pop_back();
							--recursion;
							return;
						}

						if (nestDepth == 1) // 1 is top loop, record the stamp on the match
							matchStampTop = (*rows)[currentRow]->cols[0];

						if (inst->extra)
						{
							opRunner(// call the "where" lambda
								&macros.code.front() + inst->extra,
								currentRow);
							--stackPtr;
							lambda = *stackPtr;
						}
						else
						{
							lambda = 1;
						}

						// call lambda to see if this row passes the test
						if (lambda.isEvalTrue()) // cool, we have row that matches
						{
							matchStampPrev.back() = (*rows)[currentRow]->cols[0];

							// run the inner code block
							if (!inst->index)
							{
								error.set(
									errors::errorClass_e::run_time,
									errors::errorCode_e::iteration_error,
									inst->debug.toStr());
								loopState = LoopState_e::in_exit;
								--recursion;
								return;
							}

							// increment run count
							if (rowGrp != HashPair((*rows)[currentRow]->cols[COL_STAMP], (*rows)[currentRow]->cols[COL_ACTION]))
							{
								++iterCount;
								rowGrp = HashPair((*rows)[currentRow]->cols[COL_STAMP], (*rows)[currentRow]->cols[COL_ACTION]);
							}

							if (iterCount < inst->value)
								opRunner(
									&macros.code.front() + inst->index,
									currentRow);
						}

						if (loopState == LoopState_e::in_break)
						{
							if (breakDepth == 1 || nestDepth == 1)
							{
								loopState = LoopState_e::run;
							}
							else
							{
								--nestDepth;
								--recursion;
							}

							--breakDepth;

							if (breakDepth == 0)
								break;

							matchStampPrev.pop_back();
							return;
						}

						// no actual action, we are going to loop anyways
						if (loopState == LoopState_e::in_continue)
							loopState = LoopState_e::run;
					}

					matchStampPrev.pop_back();

					// out of loop, decrement nest 
					--nestDepth;

					// otherwise we move to the next line, loop is done
				}
				break;
			case OpCode_e::ITPREV:
				// more fancy and strange stuff happens here
				break;
			case OpCode_e::MATHADD:
				// add last two items on stack
				// return product
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr += *(stackPtr + 1);
				++stackPtr;
				break;
			case OpCode_e::MATHSUB:
				// subtract last two items on stack
				// return product
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr -= *(stackPtr + 1);
				++stackPtr;
				break;
			case OpCode_e::MATHMUL:
				// multiply last two items on stack
				// return product
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr *= *(stackPtr + 1);
				++stackPtr;
				break;
			case OpCode_e::MATHDIV:
				// divide last two items on stack
				// return product
				// NOTE: Divide by zero returns 0
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr /= *(stackPtr + 1); // divide by zero handled in `cvar` /= operator
				++stackPtr;
				break;
			case OpCode_e::MATHADDEQ:
			{
				auto* tcvar = &macros.vars.userVars[inst->index].value;

				if (!inst->extra)
				{
					--stackPtr;
					*tcvar += *stackPtr;
				}
				else
				{
					for (auto x = 0; x < inst->extra - 1; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					--stackPtr;
					const auto key = std::move(*stackPtr);

					// this is the value
					--stackPtr;
					(*tcvar)[key] += *stackPtr;
				}
			}
			break;
			case OpCode_e::MATHSUBEQ:
			{
				auto* tcvar = &macros.vars.userVars[inst->index].value;

				if (!inst->extra)
				{
					--stackPtr;
					*tcvar -= *stackPtr;
				}
				else
				{
					for (auto x = 0; x < inst->extra - 1; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					--stackPtr;
					const auto key = std::move(*stackPtr);

					// this is the value
					--stackPtr;
					(*tcvar)[key] -= *stackPtr;
				}
			}
			break;
			case OpCode_e::MATHMULEQ:
			{
				auto* tcvar = &macros.vars.userVars[inst->index].value;

				if (!inst->extra)
				{
					--stackPtr;
					*tcvar *= *stackPtr;
				}
				else
				{
					for (auto x = 0; x < inst->extra - 1; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					--stackPtr;
					const auto key = std::move(*stackPtr);

					// this is the value
					--stackPtr;
					(*tcvar)[key] *= *stackPtr;
				}
			}
			break;
			case OpCode_e::MATHDIVEQ:
			{
				auto* tcvar = &macros.vars.userVars[inst->index].value;

				if (!inst->extra)
				{
					--stackPtr;
					*tcvar *= *stackPtr;
				}
				else
				{
					for (auto x = 0; x < inst->extra - 1; ++x)
					{
						--stackPtr;
						tcvar = tcvar->getMemberPtr(*stackPtr); // *stackPtr is our key
					}

					--stackPtr;
					const auto key = std::move(*stackPtr);

					// this is the value
					--stackPtr;
					(*tcvar)[key] /= *stackPtr;
				}
			}
			break;
			case OpCode_e::OPGT:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr > *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPLT:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr < *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPGTE:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr >= *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPLTE:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr <= *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPEQ:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr == *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPNEQ:
				// compare last two items on stack
				--stackPtr;
				//right = *stackPtr;
				--stackPtr;
				*stackPtr = (*stackPtr != *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::OPWTHN:
				// compare last two items on stack
				// TODO - figure this one out
				break;
			case OpCode_e::OPNOT:
				--stackPtr;
				*stackPtr = ((*stackPtr).typeof() == cvar::valueType::BOOL && *stackPtr && *stackPtr != NONE) ? false : true;
				++stackPtr;
				break;
			case OpCode_e::LGCAND:
				// AND last two items on stack
				--stackPtr;
				//right = *stackPtr;
				if (stackPtr->typeof() != cvar::valueType::BOOL && *stackPtr == NONE)
					*stackPtr = false;
				--stackPtr;
				if ((*stackPtr).typeof() != cvar::valueType::BOOL && *stackPtr == NONE)
					*stackPtr = false;
				*stackPtr = (*stackPtr && *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::LGCOR:
				// AND last two items on stack
				--stackPtr;
				//right = *stackPtr;
				if (stackPtr->typeof() != cvar::valueType::BOOL && *stackPtr == NONE)
					*stackPtr = false;
				--stackPtr;
				if ((*stackPtr).typeof() != cvar::valueType::BOOL && *stackPtr == NONE)
					*stackPtr = false;

				*stackPtr = (*stackPtr || *(stackPtr + 1));
				++stackPtr;
				break;
			case OpCode_e::MARSHAL:
				if (marshal(inst, currentRow))
					return;
				break;
			case OpCode_e::CALL:
				// Call a Script Function
				opRunner(
					&macros.code.front() + inst->index,
					currentRow);
				break;
			case OpCode_e::RETURN:
				if (stack == stackPtr)
				{
					*stackPtr = 0;
					++stackPtr;
				}
				--recursion;
				return;
			case OpCode_e::TERM:
				// script is complete, exit all nested
				// loops
				loopState = LoopState_e::in_exit;
				*stackPtr = 0;
				++stackPtr;
				--recursion;
				return;

			default: break;
		}

		// move to the next instruction
		++inst;
	}
}

void openset::query::Interpreter::setScheduleCB(
	function<bool(int64_t functionHash, int seconds)> cb)
{
	schedule_cb = cb;
}

void openset::query::Interpreter::setEmitCB(function<bool(string emitMessage)> cb)
{
	emit_cb = cb;
}

void openset::query::Interpreter::setGetSegmentCB(function<IndexBits*(string, bool& deleteAfterUsing)> cb)
{
	getSegment_cb = cb;
}

void openset::query::Interpreter::setBits(IndexBits* indexBits, int maxPopulation)
{
	bits = indexBits;
	maxBitPop = maxPopulation;
	bits->lastBit(maxBitPop);
}

void openset::query::Interpreter::setCompareSegments(IndexBits* querySegment, std::vector<IndexBits*> segments)
{
	// first AND the bits calculated by the query code to the segments provided to this function
	// to leave us segments that just contain people that match the query and the segment
	for (auto segment : segments)
	{
		segment->opAnd(*querySegment); // segmentBits will contain the result of the AND
		segmentIndexes.push_back(segment);
	}
		
	querySegment->reset(); // clean querySegment for this query

	// now we replace querySegment with the union of all the segmentBits. 
	for (auto segmentBits : segments)
		querySegment->opOr(*segmentBits);
}

void openset::query::Interpreter::execReset()
{
	// clear the flags
	loopCount = 0;
	recursion = 0;
	eventCount = -1;
	jobState = false;
	loopState = LoopState_e::run;
	stackPtr = stack;
	matchStampPrev.clear();
	eventDistinct.clear();

	if (firstRun)
	{
		// this script references global cvariables, so
		// we will copy them from the table object when this
		// interpretor is run for it's first time (that way
		// we get the most recent values)
		if (macros.useGlobals)
		{
			// find the global in this script!
			for (auto &s: macros.vars.userVars)
			{
				if (s.actual == "globals")
				{
					s.value = this->grid->getTable()->getGlobals();

					if (!s.value.contains("segment"))
					{
						s.value["segment"] = cvar{};
						s.value["segment"].dict();
					}
					break;
				}
			}			
		}
		firstRun = false;
	}
}

void openset::query::Interpreter::exec()
{
	execReset();

	const auto inst = &macros.code.front();

	try
	{
		// if we have segment constraints
		if (segmentIndexes.size())
		{
			segmentColumnShift = 0;
			for (auto seg : segmentIndexes)
			{
				if (seg->bitState(linid)) // if the person is in this segment run the ops
					opRunner(inst, 0);

				// for each segment we offset the results by the number of columns
				segmentColumnShift += macros.vars.columnVars.size();
				execReset();
			}
		}
		else
		{
			segmentColumnShift = 0;
			opRunner(inst, 0);
		}
	}
	catch (const std::runtime_error& ex)
	{
		std::string additional = "";
		if (lastDebug)
			additional = lastDebug->toStrShort();

		error.set(
			openset::errors::errorClass_e::run_time,
			openset::errors::errorCode_e::run_time_exception_triggered,
			std::string{ ex.what() } +" (2)",
			additional
		);
	}
	catch (...)
	{
		std::string additional = "";
		if (lastDebug)
			additional = lastDebug->toStrShort();

		error.set(
			openset::errors::errorClass_e::run_time,
			openset::errors::errorCode_e::run_time_exception_triggered,
			"unknown run-time error (3)",
			additional
		);
	}
}

void openset::query::Interpreter::exec(const string functionName)
{
	execReset();

	for (auto& f:macros.vars.functions)
	{
		if (f.name == functionName)
		{
			const auto inst = &macros.code.front() + f.execPtr;

			try
			{
				// if we have segment constraints
				if (segmentIndexes.size())
				{
					segmentColumnShift = 0;
					for (auto seg : segmentIndexes)
					{
						if (seg->bitState(linid)) // if the person is in this segment run the ops
							opRunner(inst, 0);

						// for each segment we offset the results by the number of columns
						segmentColumnShift += macros.vars.columnVars.size();
						execReset();
					}
				}
				else
				{
					segmentColumnShift = 0;
					opRunner(inst, 0);
				}
			}
			catch (const std::runtime_error& ex)
			{
				std::string additional = "";
				if (lastDebug)
					additional = lastDebug->toStrShort();

				error.set(
					openset::errors::errorClass_e::run_time,
					openset::errors::errorCode_e::run_time_exception_triggered,
					std::string{ ex.what() } +" (2)",
					additional
				);
			}
			catch (...)
			{
				std::string additional = "";
				if (lastDebug)
					additional = lastDebug->toStrShort();

				error.set(
					openset::errors::errorClass_e::run_time,
					openset::errors::errorCode_e::run_time_exception_triggered,
					"unknown run-time error (3)",
					additional
				);
			}

			return;
		}
	}

	error.set(
		errors::errorClass_e::run_time,
		errors::errorCode_e::missing_function_entry_point,
		"function: " + functionName);
}

void openset::query::Interpreter::exec(const int64_t functionHash)
{
	execReset();

	for (auto& f : macros.vars.functions)
	{
		if (f.nameHash == functionHash)
		{
			const auto inst = &macros.code.front() + f.execPtr;

			try
			{
				// if we have segment constraints
				if (segmentIndexes.size())
				{
					segmentColumnShift = 0;
					for (auto seg : segmentIndexes)
					{
						if (seg->bitState(linid)) // if the person is in this segment run the ops
							opRunner(inst, 0);

						// for each segment we offset the results by the number of columns
						segmentColumnShift += macros.vars.columnVars.size();
						execReset();
					}
				}
				else
				{
					segmentColumnShift = 0;
					opRunner(inst, 0);
				}
			}
			catch (const std::runtime_error& ex)
			{
				std::string additional = "";
				if (lastDebug)
					additional = lastDebug->toStrShort();

				error.set(
					openset::errors::errorClass_e::run_time,
					openset::errors::errorCode_e::run_time_exception_triggered,
					std::string{ ex.what() } +" (2)",
					additional
				);
			}
			catch (...)
			{
				std::string additional = "";
				if (lastDebug)
					additional = lastDebug->toStrShort();

				error.set(
					openset::errors::errorClass_e::run_time,
					openset::errors::errorCode_e::run_time_exception_triggered,
					"unknown run-time error (3)",
					additional
				);
			}

			return;
		}
			
	}

	error.set(
		errors::errorClass_e::run_time,
		errors::errorCode_e::missing_function_entry_point,
		"function_id: " + to_string(functionHash));
}

