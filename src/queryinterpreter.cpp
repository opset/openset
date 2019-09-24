#include <algorithm>
#include <cmath>
#include "queryinterpreter.h"
#include "time/epoch.h"
#include "table.h"
#include "columns.h"
#include "grid.h"
//const int MAX_EXEC_COUNT = 1'000'000'000;
const int MAX_RECURSE_COUNT = 10;
const int STACK_DEPTH       = 64;

openset::query::Interpreter::Interpreter(Macro_s& macros, const InterpretMode_e interpretMode)
    : macros(macros),
      interpretMode(interpretMode)
{
    stack    = new cvar[STACK_DEPTH];
    stackPtr = stack;
}

openset::query::Interpreter::~Interpreter()
{
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
                cvar.index  = cvar.column;
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
    vector<string> mappedColumns; // extract "actual" column names in query and put in
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
    grid     = person->getGrid(); // const
    blob     = grid->getAttributeBlob();
    attrs    = grid->getAttributes();
    rows     = grid->getRows(); // const
    rowCount = rows->size();

    if (firstRun)
    {
        // this script references global cvariables, so
        // we will copy them from the table object when this
        // interpretor is run for it's first time (that way
        // we get the most recent values)
        if (macros.useGlobals || macros.useProps)
        {
            // find the global in this script!
            for (auto& s : macros.vars.userVars)
            {
                if (macros.useGlobals && s.actual == "globals")
                {
                    s.value = this->grid->getTable()->getGlobals();
                    if (!s.value.contains("segment"))
                        s.value["segment"] = cvar(cvar::valueType::DICT);
                }

                if (macros.useProps && s.actual == "props")
                {
                    s.value = cvar(cvar::valueType::DICT);
                    propsIndex = s.index;
                }

            }
        }
        firstRun = false;
    }


    if (macros.useProps && propsIndex != -1 && grid)
        macros.vars.userVars[propsIndex].value = grid->getProps();

    if (person->getMeta())
    {
        uuid  = person->getUUID();
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

int64_t openset::query::Interpreter::convertStamp(const cvar& stamp)
{
    if (stamp.typeOf() == cvar::valueType::STR && Epoch::isISO8601(stamp))
    {
        const auto newStamp = Epoch::ISO8601ToEpoch(stamp);
        if (newStamp == -1)
            throw std::runtime_error("'" + stamp + "' is not an ISO8601 timestamp");
        return newStamp;
    }

    return Epoch::fixMilli(stamp.getInt64());
}

void openset::query::Interpreter::extractMarshalParams(const int paramCount)
{
    for (auto i = 1; i <= paramCount; ++i) // PERF
    {
        --stackPtr; // if any of these params are undefined, exit
        if (stackPtr->typeOf() != cvar::valueType::STR && *stackPtr == NONE)
            marshalParams[paramCount - i] = NONE;
        else
            marshalParams[paramCount - i] = std::move(*stackPtr);
    }
}

void openset::query::Interpreter::marshal_tally(const int paramCount, const Col_s* columns, const int currentRow)
{
    if (paramCount <= 0)
        return;                       // pop the stack into a pre-allocated array of cvars in reverse order
    extractMarshalParams(paramCount); // strings, doubles, and bools are all ints internally,
    // this will ensure non-int types are represented as ints
    // during grouping
    const auto fixToInt = [&](const cvar& value) -> int64_t
    {
        switch (value.typeOf())
        {
        case cvar::valueType::INT32: case cvar::valueType::INT64:
            return value.getInt64();
        case cvar::valueType::FLT: case cvar::valueType::DBL:
            return value.getDouble() * 10000;
        case cvar::valueType::STR:
        {
            const auto tString = value.getString();
            const auto hash    = MakeHash(tString);
            result->addLocalText(hash, tString); // cache this text
            return hash;
        }
        case cvar::valueType::BOOL:
            return value.getBool()
                       ? 1
                       : 0;
        default:
            return NONE;
        }
    };
    const auto getType = [&](const cvar& value) -> result::ResultTypes_e
    {
        switch (value.typeOf())
        {
        case cvar::valueType::INT32: case cvar::valueType::INT64:
            return result::ResultTypes_e::Int;
        case cvar::valueType::FLT: case cvar::valueType::DBL:
            return result::ResultTypes_e::Double;
        case cvar::valueType::STR:
            return result::ResultTypes_e::Text;
        case cvar::valueType::BOOL:
            return result::ResultTypes_e::Bool;
        default:
            return result::ResultTypes_e::None;
        }
    };
    const auto aggColumns = [&](result::Accumulator* resultColumns)
    {
        for (auto& resCol : macros.vars.columnVars)
        {
            if (!resCol.nonDistinct) // if the 'all' flag was NOT used on an aggregator
            {
                /*
                 * This is where we make the "counting key" for our aggregator. If we have already seen
                 * a key we will never aggregate again using that key (the keys cache/hash "eventDistinct" is reset
                 * upon when we switch to another person record before we re-execute the query script on that person.
                 *
                 * The key is a compound key made by taking in the following parameters:
                 * 
                 *   - index: the index of the column being counted in this iteratior of "columnVars"
                 *   - distinct: usually the value of the column in an event row, or an alternate column to count distinctly
                 *               in the event row... or when it is a variable (rather than an event column) the 
                 *               value of that variable.
                 *   - countKey: when counting people, stamp is zero (because we don't want to count a person more than once),
                 *               otherwise it is set to the row-number so we don't count a value for a row twice... or 
                 *               when we are counting with the special flag "useStampedRowIds" we use the timestamp of the row
                 *               allowing multiple rows with the same stamp to be counted as if they were part of one larger
                 *               row. 
                 *   - column:   integer version of the pointer to the result set ("resultColumns"). This is because each
                 *               result grouping has it's own "resultColumns" and we must distiguish whether we have counted
                 *               for a specific group or not before (i.e. the keys above could potentially be met on another group)
                 *               
                 */
                distinctKey.set(
                    resCol.index,
                    (resCol.modifier == Modifiers_e::var)
                        ? fixToInt(resCol.value)
                        : columns->cols[resCol.distinctColumn],
                    ((resCol.schemaColumn == COL_UUID) || (resCol.modifier == Modifiers_e::dist_count_person))
                        ? 0
                        : (macros.useStampedRowIds
                               ? columns->cols[COL_STAMP]
                               : currentRow),
                    reinterpret_cast<int64_t>(resultColumns));
                if (eventDistinct.hasKey(distinctKey))
                    continue;
                eventDistinct.set(distinctKey, 1);
            }
            const auto resultIndex = resCol.index + segmentColumnShift;
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
                if (columns->cols[resCol.column] != NONE && (resultColumns->columns[resultIndex].value == NONE ||
                    resultColumns->columns[resultIndex].value > columns->cols[resCol.column]))
                    resultColumns->columns[resultIndex].value = columns->cols[resCol.column];
                break;
            case Modifiers_e::max:
                if (columns->cols[resCol.column] != NONE && (resultColumns->columns[resultIndex].value == NONE ||
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
            case Modifiers_e::dist_count_person: case Modifiers_e::count:
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
                    resultColumns->columns[resultIndex].value = 1; //fixToInt(resCol.value);
                else
                    resultColumns->columns[resultIndex].value++; //+= fixToInt(resCol.value);
                break;
            default:
                break;
            }
        }
    };
    rowKey.clear(); // run column lambdas!
    if (macros.vars.columnLambdas.size())
        for (auto lambdaIndex : macros.vars.columnLambdas)
            opRunner(
                // call the column lambda
                &macros.code.front() + lambdaIndex,
                currentRow);
    auto depth = 0;
    for (const auto& item : marshalParams)
    {
        if (depth == paramCount || (item.typeOf() != cvar::valueType::STR && item == NONE))
            break;
        rowKey.key[depth]   = fixToInt(item);
        rowKey.types[depth] = getType(item); //result->setAtDepth(rowKey, set_cb);
        const auto aggs     = result->getMakeAccumulator(rowKey);
        aggColumns(aggs);
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
    jobState  = true;
    loopState = LoopState_e::in_exit;
    --stackPtr;
    const auto emitMessage = *stackPtr; // pop
    if (emit_cb)
        emit_cb(emitMessage);
    *stackPtr = NONE; // push
    ++stackPtr;
}

void __nestItercvar(const cvar* cvariable, string& result)
{
    if (cvariable->typeOf() == cvar::valueType::DICT)
    {
        result += "{";
        auto idx = 0;
        for (auto& v : *cvariable->getDict())
        {
            if (idx)
                result += ", ";
            result += "\"" + v.first.getString() + "\": ";
            __nestItercvar(&v.second, result);
            ++idx;
        }
        result += "}";
    }
    else if (cvariable->typeOf() == cvar::valueType::LIST)
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
    else if (cvariable->typeOf() == cvar::valueType::SET)
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
    else if (cvariable->typeOf() == cvar::valueType::STR)
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
    } // print these in reverse order with reverse iterators

    for (auto& item : params)
    {
        if (item.typeOf() == cvar::valueType::DICT || item.typeOf() == cvar::valueType::SET || item.typeOf() == cvar
            ::valueType::LIST)
        {
            string result;
            __nestItercvar(&item, result);
            cout << result << " ";
        }
        else
            cout << item << " ";
     };

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
            loopState  = LoopState_e::in_break;
        }
        else if (param == "top"s)
        {
            breakDepth = nestDepth - 1;
            loopState  = LoopState_e::in_break;
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
        loopState  = LoopState_e::in_break;
    }
    *stackPtr = NONE;
    ++stackPtr;
}

void openset::query::Interpreter::marshal_ISO8601_to_stamp(const int paramCount, const int64_t rowStamp)
{
    if (paramCount != 1)
    {
        error.set(
            errors::errorClass_e::run_time,
            errors::errorCode_e::sdk_param_count,
            "between clause requires two parameters");
        *stackPtr = NONE;
        ++stackPtr;
        return;
    }
    auto stamp = *(stackPtr - 1);
    if (stamp.typeOf() == cvar::valueType::STR)
        stamp = Epoch::ISO8601ToEpoch(stamp);
    *stackPtr = stamp;
}

void openset::query::Interpreter::marshal_bucket(const int paramCount)
{
    if (paramCount != 2)
    {
        error.set(errors::errorClass_e::run_time, errors::errorCode_e::sdk_param_count, "bucket takes two parameters");
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
    *(stackPtr - 1)    = round((stackPtr - 1)->getDouble() * power) / power;
}

void openset::query::Interpreter::marshal_fix(const int paramCount)
{
    if (paramCount != 2)
    {
        error.set(errors::errorClass_e::run_time, errors::errorCode_e::sdk_param_count, "fix takes two parameters");
        *stackPtr = NONE;
        ++stackPtr;
        return;
    }
    const auto zeros = "0000000000"s;
    --stackPtr;
    int64_t places = *stackPtr;
    if (places > 10)
        places          = 10;
    double value        = *(stackPtr - 1);
    const auto negative = value < 0;
    if (negative)
        value        = std::abs(value);
    const auto power = places
                           ? pow(10.0, places)
                           : 1;
    const int64_t rounded = round(value * power);
    auto str              = to_string(rounded);
    if (str.length() <= places)
    {
        const auto missingPreZeros = (places - str.length()) + 1;
        str                        = zeros.substr(0, missingPreZeros) + str;
    }
    if (places)
        str.insert(str.end() - places, '.');
    if (negative)
        str         = "-" + str;
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
        throw std::runtime_error("incorrect param count in dictionary");
    auto iter         = stackPtr - paramCount;
    for (auto i       = 0; i < paramCount; i += 2, iter += 2)
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
    for (auto i  = 0; i < paramCount; ++i)
    {
        --stackPtr;
        outList->emplace_back(std::move(*stackPtr));
    }
    *stackPtr = move(output);
    ++stackPtr;
}

void openset::query::Interpreter::marshal_makeSet(const int paramCount)
{
    cvar output;
    output.set();
    auto outList = output.getSet();
    auto iter    = stackPtr - paramCount;
    for (auto i  = 0; i < paramCount; ++i, ++iter)
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
    const auto a = *stackPtr; // if we acquired IndexBits from getSegment_cb we must
    // delete them after we are done, or it'll leak
    auto aDelete     = false;
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
    } // copy then AND NOT to get the compliment of these two segments
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
    const auto b = *stackPtr;
    --stackPtr;
    const auto a = *stackPtr; // if we acquired IndexBits from getSegment_cb we must
    // delete them after we are done, or it'll leak
    auto aDelete     = false;
    auto bDelete     = false;
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
    } // copy then AND to get the intersection of these two segments
    bits->opCopy(*aBits);
    bits->opAnd(*bBits);
}

void openset::query::Interpreter::marshal_union(const int paramCount)
{
    if (paramCount != 2)
    {
        error.set(errors::errorClass_e::run_time, errors::errorCode_e::sdk_param_count, "union takes two parameters");
        *stackPtr = NONE;
        ++stackPtr;
        return;
    }
    --stackPtr;
    const auto b = *stackPtr;
    --stackPtr;
    const auto a = *stackPtr; 
    
    // if we acquired IndexBits from getSegment_cb we may have to
    // delete them after we are done
    auto aDelete     = false;
    auto bDelete     = false;
    IndexBits* aBits = nullptr;
    IndexBits* bBits = nullptr;

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
    const auto a = *stackPtr; // if we acquired IndexBits from getSegment_cb we must
    // delete them after we are done, or it'll leak
    auto aDelete     = false;
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
    } // copy then AND NOT to get the compliment of these two segments
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
    const auto b = *stackPtr;
    --stackPtr;
    const auto a = *stackPtr; // if we acquired IndexBits from getSegment_cb we must
    // delete them after we are done, or it'll leak
    auto aDelete     = false;
    auto bDelete     = false;
    IndexBits* aBits = nullptr;
    if (getSegment_cb)
        aBits = getSegment_cb(a, aDelete);
    if (!aBits)
    {
        error.set(
            errors::errorClass_e::run_time,
            errors::errorCode_e::set_math_param_invalid,
            "difference - set could not be found");
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
            "difference - set could not be found");
        *stackPtr = NONE;
        ++stackPtr;
        return;
    } // copy then AND NOT to get the compliment of these two segments
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
    if ((stackPtr - 3)->typeOf() != cvar::valueType::REF)
        throw std::runtime_error("slice [:] first parameter must be reference type");
    const auto reference = stackPtr - 3;
    auto startIndex      = (stackPtr - 2)->getInt64();
    auto endIndex        = (stackPtr - 1)->getInt64();
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
            startIndex = valueLength; // we will swap for a valid range if they are reversed for whatever reason
        if (endIndex < startIndex)
            std::swap(startIndex, endIndex);
    };
    const auto refType = reference->getReference()->typeOf();
    if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET)
        throw std::runtime_error("slice [:] expecting list, string or convertable type");
    // SET and DICT will merge, LIST will append, so you can append objects
    if (refType == cvar::valueType::LIST)
    {
        const auto value       = reference->getReference()->getList();
        const auto valueLength = value->size();
        fixIndexes(valueLength, startIndex, endIndex);
        cvar result(cvar::valueType::LIST);
        auto resultList = result.getList();
        resultList->insert(resultList->begin(), value->begin() + startIndex, value->begin() + endIndex);
        *(stackPtr - 1) = std::move(result);
    }
    else
    {
        auto value             = reference->getReference()->getString(); // convert type of needed
        const auto valueLength = value.length();
        fixIndexes(valueLength, startIndex, endIndex);
        cvar result(cvar::valueType::STR);
        const auto resultString = result.getStringPtr(); // grab the actual pointer to the string in the cvar
        *resultString           = value.substr(startIndex, endIndex - startIndex);
        *(stackPtr - 1)         = std::move(result);
    }
}

void openset::query::Interpreter::marshal_find(const int paramCount, const bool reverse)
{
    if (paramCount < 2)
        throw std::runtime_error("find malformed");
    if ((stackPtr - paramCount)->typeOf() != cvar::valueType::REF)
        throw std::runtime_error(".find first parameter must be reference type");
    auto count    = paramCount;
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
    const auto lookFor   = stackPtr - 1;
    const auto refType   = reference->getReference()->typeOf();
    auto str             = reference->getReference()->getString();
    // python allows a second index, C++ takes a length, lets convert
    if (length && !reverse)
        str = str.substr(0, length);
    if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
        throw std::runtime_error(".find expecting string or convertable type");
    const auto pos = (reverse)
                         ? str.rfind(lookFor->getString())
                         : str.find(lookFor->getString(), firstPos);
    *(stackPtr - 1) = (pos == std::string::npos)
                          ? -1
                          : static_cast<int32_t>(pos);
}

void openset::query::Interpreter::marshal_split(const int paramCount) const
{
    if (paramCount != 2)
        throw std::runtime_error("split malformed");
    if ((stackPtr - paramCount)->typeOf() != cvar::valueType::REF)
        throw std::runtime_error(".split first parameter must be reference type");
    const auto reference = stackPtr - 2;
    const auto lookFor   = stackPtr - 1;
    const auto refType   = reference->getReference()->typeOf();
    if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
        throw std::runtime_error(".find expecting string or convertable type");
    const auto source    = reference->getReference()->getString();
    const auto search    = lookFor->getString();
    const auto searchLen = search.length(); // make a result list
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
        result += cleanup; // leave the List on the stack
    *(stackPtr - 1) = std::move(result);
}

void openset::query::Interpreter::marshal_strip(const int paramCount) const
{
    if (paramCount != 1)
        throw std::runtime_error("strip malformed");
    if ((stackPtr - paramCount)->typeOf() != cvar::valueType::REF)
        throw std::runtime_error(".strip first parameter must be reference type");
    const auto reference = stackPtr - 1;
    const auto refType   = reference->getReference()->typeOf();
    if (refType == cvar::valueType::DICT || refType == cvar::valueType::SET || refType == cvar::valueType::LIST)
        throw std::runtime_error(".strip expecting string or convertable type");
    auto text             = reference->getReference()->getString();
    const auto whiteSpace = " \t\n\r"s;
    if (const auto start  = text.find_first_not_of(whiteSpace); start == text.find_last_not_of(whiteSpace) && start ==
        std::string::npos)
        text.clear();
    else
    {
        if (const auto end = text.find_last_not_of(whiteSpace); end != std::string::npos)
            text.resize(end + 1);
        if (start != std::string::npos)
            text = text.substr(start);
    } // leave the List on the stack
    *(stackPtr - 1) = text;
}

void openset::query::Interpreter::marshal_url_decode(const int paramCount) const
{
    if (paramCount != 1)
        throw std::runtime_error("url_decode malformed");
    const auto paramType = (stackPtr - 1)->typeOf();
    if (paramType == cvar::valueType::DICT || paramType == cvar::valueType::SET || paramType == cvar::valueType::LIST)
        throw std::runtime_error(".url_decode expecting string or convertable type");
    const auto url = (stackPtr - 1)->getString();
    auto& result   = *(stackPtr - 1);
    result.dict();
    result["host"]      = NONE;
    result["path"]      = NONE;
    result["query"]     = NONE;
    result["params"]    = cvar::Dict {};
    size_t start        = 0; // look for the // after the protocal, if its here we have a host
    if (auto slashSlash = url.find("//"); slashSlash != std::string::npos)
    {
        slashSlash += 2;
        const auto endPos = url.find('/', slashSlash);
        if (endPos == std::string::npos) // something is ugly
            return;
        result["host"] = url.substr(slashSlash, endPos - (slashSlash));
        start          = endPos;
    } // we have a question mark
    if (auto qpos = url.find('?', start); qpos != std::string::npos)
    {
        result["path"] = url.substr(start, qpos - start);
        ++qpos;
        auto query      = url.substr(qpos);
        result["query"] = query;
        start           = 0;
        while (true)
        {
            auto pos = query.find('&', start);
            if (pos == std::string::npos)
                pos             = query.size();
            auto param          = query.substr(start, pos - start);
            if (const auto ePos = param.find('='); ePos != std::string::npos)
            {
                const auto key        = param.substr(0, ePos);
                const auto value      = param.substr(ePos + 1);
                result["params"][key] = value;
            }
            else
                result["params"][param] = true;
            start                       = pos + 1; // move past the '&'
            if (start >= query.length())
                break;
        }
    }
    else
        result["path"] = url.substr(start);
}

void openset::query::Interpreter::marshal_get_row(const int paramCount) const
{
    if (paramCount != 1)
        throw std::runtime_error("get_row requires a row iterator");

    if (*(stackPtr - 1) == NONE) // leave None on the stack
        return;

    const auto currentRow = (stackPtr - 1)->getInt32();

    cvar result(cvar::valueType::DICT);

    for (const auto& tableVar : macros.vars.tableVars)
    {
        auto key = tableVar.actual; // we pop the actual user id in this case
        if (tableVar.schemaColumn == COL_UUID)
        {
            result[key] = this->grid->getUUIDString();
            continue;
        }
        auto colValue = NONE; // extract property value from grid->propRow
        if (tableVar.isProp)
            colValue = propRow->cols[tableVar.column];
        else
            colValue = (*rows)[currentRow]->cols[tableVar.column];
        if (colValue == NONE)
            continue;
        switch (tableVar.schemaType)
        {
        case columnTypes_e::freeColumn:
            break;
        case columnTypes_e::intColumn:
            if (tableVar.isSet)
            {
                auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                const auto setData = grid->getSetData();
                result[key]        = cvar(cvar::valueType::SET);
                const auto end     = info.offset + info.length;
                for (auto idx      = info.offset; idx < end; ++idx)
                    result[key].getSet()->emplace(setData[idx]);
            }
            else
                result[key] = colValue;
            break;
        case columnTypes_e::doubleColumn:
            if (tableVar.isSet)
            {
                auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                const auto setData = grid->getSetData();
                result[key]        = cvar(cvar::valueType::SET);
                const auto end     = info.offset + info.length;
                for (auto idx      = info.offset; idx < end; ++idx)
                    result[key].getSet()->emplace(setData[idx] / 10000.0);
            }
            else
                result[key] = colValue / 10000.0;
            break;
        case columnTypes_e::boolColumn:
            if (tableVar.isSet)
            {
                auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                const auto setData = grid->getSetData();
                result[key]        = cvar(cvar::valueType::SET);
                const auto end     = info.offset + info.length;
                for (auto idx      = info.offset; idx < end; ++idx)
                    result[key].getSet()->emplace(
                        setData[idx]
                            ? true
                            : false);
            }
            else
                result[key] = colValue
                                  ? true
                                  : false;
            break;
        case columnTypes_e::textColumn:
            if (tableVar.isSet)
            {
                auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                const auto setData = grid->getSetData();
                result[key]        = cvar(cvar::valueType::SET);
                const auto end     = info.offset + info.length;
                for (auto idx      = info.offset; idx < end; ++idx)
                {
                    const auto attr = attrs->get(tableVar.schemaColumn, setData[idx]);
                    if (attr && attr->text)
                        result[key].getSet()->emplace(std::string(attr->text));
                }
            }
            else
            {
                const auto attr = attrs->get(tableVar.schemaColumn, colValue);
                if (attr && attr->text)
                    result[key] = attr->text;
                else
                    result[key] = colValue;
            }
            break;
        default:
            break;
        }
    }
    *(stackPtr - 1) = result;
}

string openset::query::Interpreter::getLiteral(const int64_t id) const
{
    for (auto& i : macros.vars.literals)
    {
        if (i.hashValue == id)
            return i.value;
    }
    return "";
}

bool openset::query::Interpreter::marshal(Instruction_s* inst, int64_t& currentRow)
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
            return true;
        /*{
            if (bits)
                bits->bitSet(linid);
            loopState = LoopState_e::in_exit;
            *stackPtr = 0;
            ++stackPtr;
            return true;
        }*/
        marshal_tally(inst->extra, (*rows)[currentRow], currentRow);
    }
    break;
    case Marshals_e::marshal_now:
        *stackPtr = Now();
        ++stackPtr;
        break; /*case Marshals_e::marshal_event_time:
                *stackPtr = (*rows)[currentRow]->cols[COL_STAMP];
                ++stackPtr;
                break;*/
    case Marshals_e::marshal_last_event:
        *stackPtr = rows->back()->cols[COL_STAMP];
        ++stackPtr;
        break;
    case Marshals_e::marshal_first_event:
        *stackPtr = rows->front()->cols[COL_STAMP];
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
    case Marshals_e::marshal_row_count:
        if (eventCount == -1)
        {
            int64_t currentGrp = 0;
            auto countIter     = rows->begin();
            eventCount         = 0;
            while (countIter != rows->end())
            {
                if (currentGrp != HashPair((*countIter)->cols[COL_STAMP], (*countIter)->cols[COL_EVENT]))
                {
                    currentGrp = HashPair((*countIter)->cols[COL_STAMP], (*countIter)->cols[COL_EVENT]);
                    ++eventCount;
                }
                ++countIter;
            }
        }
        *stackPtr = eventCount;
        ++stackPtr;
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
    case Marshals_e::marshal_return: // return will have its params on the stack,
        // we will just leave these on the stack and
        // break out of this block... magic!
        if (stackPtr == stack) // push None if no return
        {
            *stackPtr = NONE;
            ++stackPtr;
        }
        inReturn = true;
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
    case Marshals_e::marshal_push_subscript:
        {
        marshal_makeList(inst->extra);
        }
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
    case Marshals_e::marshal_append: case Marshals_e::marshal_update: case Marshals_e::marshal_add:
        if (inst->extra != 2)
            throw std::runtime_error(".append/.update requires parameters");
        if ((stackPtr - 2)->typeOf() != cvar::valueType::REF)
            throw std::runtime_error(".append/.update first parameter must be reference type");
        // SET and DICT will merge, LIST will append, so you can append objects
        if ((stackPtr - 2)->getReference()->typeOf() == cvar::valueType::LIST)
            (stackPtr - 2)->getReference()->getList()->push_back(*(stackPtr - 1));
        else
            *(stackPtr - 2)->getReference() += *(stackPtr - 1);
        stackPtr -= 2;
        break;
    case Marshals_e::marshal_remove: case Marshals_e::marshal_del:
        if (inst->extra != 2)
            throw std::runtime_error("del requires parameters");
        if ((stackPtr - 2)->typeOf() != cvar::valueType::REF)
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
            auto var       = (stackPtr - 1)->getReference();
            const auto res = (stackPtr - 1);
            if (var->typeOf() == cvar::valueType::LIST)
            {
                if (!var->getList() || var->getList()->size() == 0)
                {
                    *res = NONE;
                    break;
                }
                *res = std::move(var->getList()->back());
                var->getList()->pop_back();
            }
            else if (var->typeOf() == cvar::valueType::DICT)
            {
                if (!var->getDict() || var->getDict()->size() == 0)
                {
                    *res = NONE;
                    break;
                }
                auto value = var->getDict()->begin();
                var->getDict()->erase(value);
                var->dict(); // result is a Dict
                (*res) = cvar::o { value->first, value->second };
            }
            else if (var->typeOf() == cvar::valueType::SET)
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
            if (var->typeOf() == cvar::valueType::LIST)
            {
                auto value = var->getList()->front();
                var->getList()->erase(var->getList()->begin());
                *res = std::move(value);
            }
            else if (var->typeOf() == cvar::valueType::SET)
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
            auto res       = (stackPtr - 1);
            if (var->typeOf() == cvar::valueType::DICT)
            {
                res->list(); // result is a Dict
                for (const auto& v : *var->getDict())
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
    case Marshals_e::marshal_url_decode:
        marshal_url_decode(inst->extra);
        break;
    case Marshals_e::marshal_get_row:
        marshal_get_row(inst->extra);
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

cvar* openset::query::Interpreter::lambda(int lambdaId, int currentRow)
{
    const auto beforePtr = stackPtr;
    opRunner(
        // call condition lambda
        &macros.code.front() + macros.lambdas[lambdaId],
        currentRow);

    if (stackPtr < beforePtr)
    {
            error.set(
                errors::errorClass_e::run_time,
                errors::errorCode_e::exec_count_exceeded,
                "stack under-run",
                lastDebug ? lastDebug->toStrShort() : "stack under-run in lambda" );
            loopState = LoopState_e::in_exit;
    }
    else if (stackPtr == beforePtr) // lambda left nothing on the stack, so add a default result
    {
        *stackPtr = NONE;
        ++stackPtr;
    }

    --stackPtr;
    return stackPtr;
}

void openset::query::Interpreter::opRunner(Instruction_s* inst, int64_t currentRow)
{
    // count allows for now row pointer, and no mounted person
    if ((!rows || rows->empty()) && interpretMode != InterpretMode_e::count)
    {
        loopState = LoopState_e::in_exit;
        *stackPtr = NONE;
        ++stackPtr;
        return;
    } //auto rowIter = rows->begin() + currentRow;
    if (++recursion > MAX_RECURSE_COUNT)
    {
        error.set(
            errors::errorClass_e::run_time,
            errors::errorCode_e::recursion,
            "nesting depth was: " + to_string(recursion),
            lastDebug ? lastDebug->toStrShort() : "");
        loopState = LoopState_e::in_exit;
        *stackPtr = NONE;
        ++stackPtr;
        --recursion;
        return;
    }
    while (loopState == LoopState_e::run && !error.inError() && !inReturn)
    {
        // tracks the last known script line number
        lastDebug = &inst->debug; /* TODO LONG RUN CHECK --- we will do this differently
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

        // underrun test
        if (stackPtr < stack)
        {
            error.set(
                errors::errorClass_e::run_time,
                errors::errorCode_e::exec_count_exceeded,
                "stack under-run",
                lastDebug->toStrShort());
            loopState = LoopState_e::in_exit;
            // reset stack
            stackPtr = stack;
            *stackPtr = NONE;
            ++stackPtr;
            --recursion;
            return;            
        }

        switch (inst->op)
        {
        case OpCode_e::NOP: // do nothing... nothing to see here... move on
            break;
        case OpCode_e::PSHTBLCOL: // push a column value
        {
            // if it's row iterator variable, we get its value, otherwise we use the current row
            const int64_t readRow = inst->extra != NONE
                ? macros.vars.userVars[inst->extra].value.getInt64()
                : currentRow; // we pop the actual user id in this case

            if (macros.vars.tableVars[inst->index].schemaColumn == COL_UUID)
            {
                *stackPtr = this->grid->getUUIDString();
            }
            else
            {
                auto colValue = NONE; // extract property value from grid->propRow
                
                //if (macros.vars.tableVars[inst->index].isProp)
                //{
                //    colValue = propRow->cols[macros.vars.tableVars[inst->index].column];
                // }
                //else
                //{
                colValue = (*rows)[readRow]->cols[macros.vars.tableVars[inst->index].column];
                //}

                switch (macros.vars.tableVars[inst->index].schemaType)
                {
                case columnTypes_e::freeColumn:
                    *stackPtr = NONE;
                    break;
                case columnTypes_e::intColumn:
                    if (colValue == NONE)
                    {
                        if (macros.vars.tableVars[inst->index].isSet)
                            stackPtr->set();
                        else
                            *stackPtr = NONE;
                    }
                    else if (macros.vars.tableVars[inst->index].isSet)
                    {
                        auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                        const auto setData = grid->getSetData();
                        stackPtr->set();
                        const auto end = info.offset + info.length;
                        for (auto idx  = info.offset; idx < end; ++idx)
                            stackPtr->getSet()->emplace(setData[idx]);
                    }
                    else
                        *stackPtr = colValue;
                    break;
                case columnTypes_e::doubleColumn:
                    if (colValue == NONE)
                    {
                        if (macros.vars.tableVars[inst->index].isSet)
                            stackPtr->set();
                        else
                            *stackPtr = NONE;
                    }
                    else if (macros.vars.tableVars[inst->index].isSet)
                    {
                        auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                        const auto setData = grid->getSetData();
                        stackPtr->set();
                        const auto end = info.offset + info.length;
                        for (auto idx  = info.offset; idx < end; ++idx)
                            stackPtr->getSet()->emplace(setData[idx] / 10000.0);
                    }
                    else
                        *stackPtr = colValue / 10000.0;
                    break;
                case columnTypes_e::boolColumn:
                    if (colValue == NONE)
                    {
                        if (macros.vars.tableVars[inst->index].isSet)
                            stackPtr->set();
                        else
                            *stackPtr = NONE;
                    }
                    else if (macros.vars.tableVars[inst->index].isSet)
                    {
                        auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                        const auto setData = grid->getSetData();
                        stackPtr->set();
                        const auto end = info.offset + info.length;
                        for (auto idx  = info.offset; idx < end; ++idx)
                            stackPtr->getSet()->emplace(
                                setData[idx] ? true : false);
                    }
                    else
                        *stackPtr = colValue ? true : false;
                    break;
                case columnTypes_e::textColumn:
                    if (colValue == NONE)
                    {
                        if (macros.vars.tableVars[inst->index].isSet)
                            stackPtr->set();
                        else
                            *stackPtr = NONE;
                    }
                    else if (macros.vars.tableVars[inst->index].isSet)
                    {
                        auto& info         = *reinterpret_cast<SetInfo_s*>(&colValue);
                        const auto setData = grid->getSetData();
                        stackPtr->set();
                        const auto end = info.offset + info.length;
                        for (auto idx  = info.offset; idx < end; ++idx)
                        {
                            const auto attr = attrs->get(macros.vars.tableVars[inst->index].schemaColumn, setData[idx]);
                            if (attr && attr->text)
                                stackPtr->getSet()->emplace(std::string(attr->text));
                        }
                    }
                    else
                    {
                        const auto attr = attrs->get(macros.vars.tableVars[inst->index].schemaColumn, colValue);
                        if (attr && attr->text)
                            *stackPtr = attr->text;
                        else
                            *stackPtr = colValue;
                    }
                    break;
                default:
                    break;
                }
            }
            ++stackPtr;
        }
        break;
        case OpCode_e::VARIDX:
            *stackPtr = inst->index;
            ++stackPtr;
            break;
        case OpCode_e::COLIDX:
            *stackPtr = inst->index;
            ++stackPtr;
            break;
        case OpCode_e::PSHPAIR: // we are simply going to pop two items off the stack (the key, then the value)
            // and assign these to a new Dictionary (a dictionary with one pair)
        {
            --stackPtr;
            const auto key = std::move(*stackPtr);
            --stackPtr;
            auto value = std::move(*stackPtr);
            (*stackPtr).dict();
            (*stackPtr)[key] = std::move(value);
            ++stackPtr;
        }
        break;
        case OpCode_e::PSHRESCOL: // push a select value onto stack
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
            }                                           // this is the value
            if (tcvar)
                *stackPtr = *tcvar; // copy, not move
            else
                *stackPtr = NONE;
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
            }                                           // this is the value
            stackPtr->setReference(tcvar);
            ++stackPtr;
        }
        break;
        case OpCode_e::PSHUSRVAR: // push a cvar							
            *stackPtr = macros.vars.userVars[inst->index].value;
            ++stackPtr;
            break;
        case OpCode_e::PSHUSRVREF: // push a cvar							
            stackPtr->setReference(&macros.vars.userVars[inst->index].value);
            ++stackPtr;
            break;
        case OpCode_e::PSHLITTRUE: // push boolean true						
            *stackPtr = true;
            ++stackPtr;
            break;
        case OpCode_e::PSHLITFALSE: // push boolean false
            *stackPtr = false;
            ++stackPtr;
            break;
        case OpCode_e::PSHLITSTR: // push a string value
            //result->addLocalText(macros.vars.literals[inst->index].value);
            *stackPtr = macros.vars.literals[inst->index].value; // WAS hashValue
            ++stackPtr;
            break;
        case OpCode_e::PSHLITINT: // push a numeric value
            *stackPtr = inst->value;
            ++stackPtr;
            break;
        case OpCode_e::PSHLITFLT: // push a floating point value
            *stackPtr = cast<double>(inst->value) / cast<double>(1'000'000);
            ++stackPtr;
            break;
        case OpCode_e::PSHLITNUL: // push a floating point value
            *stackPtr = NONE;
            ++stackPtr;
            break;
        case OpCode_e::POPUSROBJ:
        {
            auto* tcvar = &macros.vars.userVars[inst->index].value;
            for (auto x = 0; x < inst->extra - 1; ++x)
            {
                --stackPtr;
                // throw if missing key while popping
                tcvar = tcvar->getMemberPtr(*stackPtr, true); // *stackPtr is our key
            }
            --stackPtr;
            const auto key = std::move(*stackPtr); // TODO - use ref?
            // this is the value
            --stackPtr;
            (*tcvar)[key] = std::move(*stackPtr);
        }
        break;
        case OpCode_e::POPUSRVAR: // pop stack into cvar
            --stackPtr;
            macros.vars.userVars[inst->index].value = std::move(*stackPtr);
            break;
        case OpCode_e::POPTBLCOL: // pop stack into column value
            // NOTE: we don't actually do this
            break;
        case OpCode_e::POPRESCOL: // pop stack into select
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
        case OpCode_e::CNDIF: // execute lambda, and if not 0 on stack
            // next a block.
            // IF is implemented with a lambda. If it returns true
            // the corresponding code block is run. After this
            // it most proceed to the code line with the 
            // first non-ELSE/ELIF
            opRunner(
                // call condition lambda
                &macros.code.front() + inst->extra,
                currentRow); // anything not 0 is true
            --stackPtr;
            if (stackPtr->isEvalTrue())
            {
                // PASSED - run the code block (recursive)
                opRunner(&macros.code.front() + inst->index, currentRow);
                if (inReturn)
                    return; // fast forward passed subsequent elif/else ops
                ++inst;
                while (inst->op == OpCode_e::CNDELIF || inst->op == OpCode_e::CNDELSE)
                    ++inst; // we've advanced the instruction pointer
                // loop to top
                continue;
            }
            break;
        case OpCode_e::CNDELIF: // ELIF always follows an IF
            // if a match is made the execution pointer after
            // nesting must move to the first non-ELSE/ELIF
            opRunner(
                // call condition lambda
                &macros.code.front() + inst->extra,
                currentRow); // anything not 0 is true
            --stackPtr;      //if (stackPtr->getInt64() != NONE && *stackPtr)
            if (stackPtr->isEvalTrue())
            {
                // PASSED - run the code block (recursive)
                opRunner(&macros.code.front() + inst->index, currentRow);
                if (inReturn)
                    return; // fast forward passed subsequent elif/else ops
                ++inst;
                while (inst->op == OpCode_e::CNDELIF || inst->op == OpCode_e::CNDELSE)
                    ++inst; // we've advanced the instruction pointer
                // loop to top
                continue;
            }
            break;
        case OpCode_e::CNDELSE: // ELSE block be executed only when all if/elif blocks fail
            // DEFAULTED - run the code block (recursive)
            opRunner(&macros.code.front() + inst->index, currentRow);
            if (inReturn)
                return;
            break;
        case OpCode_e::ITFOR:
        {
            --stackPtr;

            const int64_t keyIdx = *stackPtr; // we are going to look back one instruction to see if we 
            // are putting values into a user variable or a column variable
            const auto isColumn = (inst - 1)->op == OpCode_e::COLIDX;

            int64_t valueIdx    = 0;

            if (inst->value == 2)
            {
                --stackPtr;
                valueIdx = *stackPtr;
            }

            --stackPtr;
            auto source = move(*stackPtr);

            if (source.typeOf() == cvar::valueType::DICT)
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
                        --recursion;
                        return;
                    }
                    if (isColumn)
                        macros.vars.columnVars[keyIdx].value = x.first;
                    else
                        macros.vars.userVars[keyIdx].value = x.first;
                    if (inst->value == 2)
                        macros.vars.userVars[valueIdx].value = x.second;
                    opRunner(&macros.code.front() + inst->index, currentRow);
                    if (loopState == LoopState_e::in_break || inReturn)
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
                        return;
                    }
                    if (loopState == LoopState_e::in_continue)
                        loopState = LoopState_e::run; // no actual action, we are going to loop anyways
                }                                     // out of loop, decrement nest 
                --nestDepth;
            }
            else if (source.typeOf() == cvar::valueType::LIST)
            {
                const auto from = source.getList();
                for (auto& x : *from)
                {
                    if (loopState == LoopState_e::in_exit || error.inError())
                    {
                        *stackPtr = 0;
                        ++stackPtr;
                        --nestDepth;
                        --recursion;
                        return;
                    }
                    if (isColumn)
                        macros.vars.columnVars[keyIdx].value = x;
                    else
                        macros.vars.userVars[keyIdx].value = x;
                    opRunner(&macros.code.front() + inst->index, currentRow);
                    if (loopState == LoopState_e::in_break || inReturn)
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
                        return;
                    }
                    if (loopState == LoopState_e::in_continue)
                        loopState = LoopState_e::run; // no actual action, we are going to loop anyways
                }
            }
            else if (source.typeOf() == cvar::valueType::SET)
            {
                const auto from = source.getSet();

                for (auto& x : *from)
                {
                    if (loopState == LoopState_e::in_exit || error.inError())
                    {
                        *stackPtr = 0;
                        ++stackPtr;
                        --nestDepth;
                        --recursion;
                        return;
                    }

                    if (isColumn)
                        macros.vars.columnVars[keyIdx].value = x;
                    else
                        macros.vars.userVars[keyIdx].value = x;

                    opRunner(&macros.code.front() + inst->index, currentRow);

                    if (loopState == LoopState_e::in_break || inReturn)
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
                        return;
                    }
                    if (loopState == LoopState_e::in_continue)
                        loopState = LoopState_e::run; // no actual action, we are going to loop anyways
                }
            }
            else
            {
                error.set(errors::errorClass_e::run_time, errors::errorCode_e::iteration_error, inst->debug.toStr());
                loopState = LoopState_e::in_exit;
                --recursion;
                return;
            }
        }
        break; /*			case OpCode_e::ITNEXT:
                                // fancy and strange stuff happens here					
                                {
                                    if (currentRow >= static_cast<int>(rows->size()))
                                        break;
                
                                    const auto savedRow = currentRow;
                                    auto iterCount = 0;
                                    cvar lambda;
                
                                    // enter loop, increment nest 
                                    ++nestDepth;
                
                                    // store the time stamp of the last match
                                    matchStampPrev.push_back((*rows)[currentRow]->cols[0]);
                
                                    // user right for count
                                    for (const auto rowCount = rows->size();
                                         iterCount < inst->value && currentRow < static_cast<int>(rowCount);
                                         ++currentRow)
                                    {
                
                                        if (loopState == LoopState_e::in_exit || error.inError())
                                        {
                                            *stackPtr = 0;
                                            ++stackPtr;
                
                                            matchStampPrev.pop_back();
                                            --nestDepth;
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
                
                                            if (iterCount < inst->value)
                                                opRunner(
                                                    &macros.code.front() + inst->index,
                                                    currentRow);
                
                                            // increment run count
                                            ++iterCount;
                
                                        }
                
                                        if (loopState == LoopState_e::in_break || inReturn)
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
                
                                    currentRow = savedRow;
                
                                    // out of loop, decrement nest 
                                    --nestDepth;
                
                                    // otherwise we move to the next line, loop is done
                                }
                                break;
                            case OpCode_e::ITPREV:
                                // more fancy and strange stuff happens here
                                break;*/
        case OpCode_e::ITFORR: case OpCode_e::ITFORRC: case OpCode_e::ITFORRCF:
        {
            --stackPtr;
            const auto reference = stackPtr->getReference();
            const auto rowCount  = static_cast<int>(rows->size());
            if (currentRow >= rowCount || currentRow < 0)
                break;
            const auto savedRow = currentRow; // reset row position if using ITFORR, ITFORRC, ITFORRCF
            if (inst->op == OpCode_e::ITFORRCF)
            {
                --stackPtr;
                currentRow = *stackPtr; // ITFORRCF (Iterate-for-row-continue-from) starts from stack value
            }
            else if (inst->op == OpCode_e::ITFORR)
                currentRow = 0;
            else
                ++currentRow; // ITFORCC starts on next row
            // bad math check
            //if (currentRow < 0)
            //  currentRow = 0;
            // number of matches when using match limiting on `for` loop
            auto iterCount    = 0; // result of lambda call
            cvar lambdaResult = 0; // enter loop, increment nest 
            ++nestDepth;           // user right for count
            for (; iterCount < inst->value && currentRow >= 0 && currentRow < static_cast<int>(rowCount); ++currentRow)
            {
                if (loopState == LoopState_e::in_exit || error.inError())
                {
                    *stackPtr = 0;
                    ++stackPtr;
                    --nestDepth;
                    --recursion;
                    return;
                }                        // set the value of referenced `for variable` to the current row number
                *reference = currentRow; // run the `if` conditional lambda if present, or
                // default to passing (True)
                if (inst->extra) // Lambda for `if` conditional
                {
                    opRunner(
                        // call the "where" lambda
                        &macros.code.front() + inst->extra,
                        currentRow);
                    --stackPtr;
                    lambdaResult = *stackPtr; // return value from `if` evaluation
                }
                else
                {
                    lambdaResult = 1;
                }                              // call lambda to see if this row passes the test
                if (lambdaResult.isEvalTrue()) // cool, we have row that matches
                {
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
                    if (iterCount < inst->value)
                        opRunner(&macros.code.front() + inst->index, currentRow); // increment run count
                    ++iterCount;
                }
                if (loopState == LoopState_e::in_break || inReturn)
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
                    return;
                } // no actual action, we are going to loop anyways
                if (loopState == LoopState_e::in_continue)
                    loopState = LoopState_e::run;
            }
            currentRow = savedRow; // out of loop, decrement nest 
            --nestDepth;
        }
            break;
        case OpCode_e::ITRFORR: case OpCode_e::ITRFORRC: case OpCode_e::ITRFORRCF:
        {
            --stackPtr;
            const auto reference = stackPtr->getReference();
            const auto rowCount  = static_cast<int>(rows->size());
            if (currentRow >= rowCount || currentRow < 0)
                break;
            const auto savedRow = currentRow; // reset row position if using ITFORR, ITFORRC, ITFORRCF
            if (inst->op == OpCode_e::ITRFORRCF)
            {
                --stackPtr;
                currentRow = *stackPtr; // ITFORRCF (Iterate-for-row-continue-from) starts from stack value
            }
            else if (inst->op == OpCode_e::ITRFORR)
                currentRow = rowCount - 1;
            else
                --currentRow; // ITFORCC starts on next row
            // bad math check
            //if (currentRow > rowCount)
            //    currentRow = rowCount - 1;
            // number of matches when using match limiting on `for` loop
            auto iterCount    = 0; // result of lambda call
            cvar lambdaResult = 0; // enter loop, increment nest 
            ++nestDepth;           // user right for count
            for (; iterCount < inst->value && currentRow >= 0 && currentRow < static_cast<int>(rowCount); --currentRow)
            {
                if (loopState == LoopState_e::in_exit || error.inError())
                {
                    *stackPtr = 0;
                    ++stackPtr;
                    --nestDepth;
                    --recursion;
                    return;
                }                        // set the value of referenced `for variable` to the current row number
                *reference = currentRow; // run the `if` conditional lambda if present, or
                // default to passing (True)
                if (inst->extra) // Lambda for `if` conditional
                {
                    opRunner(
                        // call the "where" lambda
                        &macros.code.front() + inst->extra,
                        currentRow);
                    --stackPtr;
                    lambdaResult = *stackPtr; // return value from `if` evaluation
                }
                else
                {
                    lambdaResult = 1;
                }                              // call lambda to see if this row passes the test
                if (lambdaResult.isEvalTrue()) // cool, we have row that matches
                {
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
                    if (iterCount < inst->value)
                        opRunner(&macros.code.front() + inst->index, currentRow); // increment run count
                    ++iterCount;
                }
                if (loopState == LoopState_e::in_break || inReturn)
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
                    return;
                } // no actual action, we are going to loop anyways
                if (loopState == LoopState_e::in_continue)
                    loopState = LoopState_e::run;
            }
            currentRow = savedRow; // out of loop, decrement nest 
            --nestDepth;
        }
            break;
        case OpCode_e::SETROW:
            currentRow = macros.vars.userVars[inst->index].value;
            break;
        case OpCode_e::MATHADD: // add last two items on stack
            // return product
            stackPtr -= 2;
            *stackPtr += *(stackPtr + 1);
            ++stackPtr;
            break;
        case OpCode_e::MATHSUB: // subtract last two items on stack
            // return product
            stackPtr -= 2;
            *stackPtr -= *(stackPtr + 1);
            ++stackPtr;
            break;
        case OpCode_e::MATHMUL: // multiply last two items on stack
            // return product
            stackPtr -= 2;
            *stackPtr *= *(stackPtr + 1);
            ++stackPtr;
            break;
        case OpCode_e::MATHDIV: // divide last two items on stack
            // return product
            // NOTE: Divide by zero returns 0
            stackPtr -= 2;
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
                const auto key = std::move(*stackPtr); // this is the value
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
                const auto key = std::move(*stackPtr); // this is the value
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
                const auto key = std::move(*stackPtr); // this is the value
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
                const auto key = std::move(*stackPtr); // this is the value
                --stackPtr;
                (*tcvar)[key] /= *stackPtr;
            }
        }
        break;
        case OpCode_e::OPGT: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr > *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPLT: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr < *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPGTE: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr >= *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPLTE: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr <= *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPEQ: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr == *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPNEQ: // compare last two items on stack
            stackPtr -= 2;
            *stackPtr = (*stackPtr != *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::OPWTHN: // compare last two items on stack
            // TODO - figure this one out
            break;
        case OpCode_e::OPNOT:
            --stackPtr;
            *stackPtr = ((*stackPtr).typeOf() == cvar::valueType::BOOL && *stackPtr && *stackPtr != NONE)
                            ? false
                            : true;
            ++stackPtr;
            break;
        case OpCode_e::LGCAND: // AND last two items on stack
            --stackPtr;
            if (stackPtr->typeOf() != cvar::valueType::BOOL && *stackPtr == NONE)
                *stackPtr = false;
            --stackPtr;
            if ((*stackPtr).typeOf() != cvar::valueType::BOOL && *stackPtr == NONE)
                *stackPtr = false;
            *stackPtr     = (*stackPtr && *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::LGCOR: // AND last two items on stack
            --stackPtr;
            if (stackPtr->typeOf() != cvar::valueType::BOOL && *stackPtr == NONE)
                *stackPtr = false;
            --stackPtr;
            if ((*stackPtr).typeOf() != cvar::valueType::BOOL && *stackPtr == NONE)
                *stackPtr = false;
            *stackPtr     = (*stackPtr || *(stackPtr + 1));
            ++stackPtr;
            break;
        case OpCode_e::MARSHAL:
            if (marshal(inst, currentRow))
                return;
            break;
        case OpCode_e::CALL: // Call a Script Function
            inReturn = false;
            opRunner(&macros.code.front() + inst->index, currentRow);
            inReturn = false;
            break;
        case OpCode_e::RETURN: // this is a soft return like END-OF-BLOCK, not like explicit return call
            //inReturn = true;
            
            if (stack == stackPtr)
            {
                *stackPtr = NONE;
                ++stackPtr;
            }
            --recursion;
            return;
        case OpCode_e::TERM: // script is complete, exit all nested
            // loops
            loopState = LoopState_e::in_exit;
            *stackPtr = NONE;
            ++stackPtr;
            --recursion;
            return;
        case OpCode_e::OPIN:
            {
                stackPtr -= 2;
                const auto& rightSide = *(stackPtr + 1);
                const auto& leftSide = *stackPtr;

                if (leftSide.isContainer())
                    *stackPtr = false; // THROW
                else
                    *stackPtr = rightSide.contains(leftSide); // `in` is `contains` in reverse
                ++stackPtr;
            }
            break;
        case OpCode_e::OPCONT:
            {
                stackPtr -= 2;
                const auto& rightSide = *(stackPtr + 1);
                const auto& leftSide = *stackPtr;

                if (!leftSide.isContainer())
                    *stackPtr = false;
                else
                    *stackPtr = leftSide.containsAllOf(rightSide);
                ++stackPtr;
            }
            break;
        case OpCode_e::OPANY:
            {
                stackPtr -= 2;
                const auto& rightSide = *(stackPtr + 1);
                const auto& leftSide = *stackPtr;

                if (!leftSide.isContainer())
                    *stackPtr = false;
                else
                    *stackPtr = leftSide.containsAnyOf(rightSide);
                ++stackPtr;
            }
            break;
        case OpCode_e::CALL_FOR:
        {
            --stackPtr;
            const auto reference = stackPtr->getReference();
            --stackPtr;
            const auto source = *stackPtr;

            ++nestDepth;

            if (source != NONE)
            {
                if (!source.isContainer())
                {
                    error.set(
                        errors::errorClass_e::run_time,
                        errors::errorCode_e::iteration_error,
                        inst->debug.toStr());
                    loopState = LoopState_e::in_exit;
                    return;
                }

                switch (source.typeOf())
                {
                case cvar::valueType::LIST:
                    for (const auto& i : *source.getList())
                    {
                        *reference = i;
                        lambda(inst->index, currentRow);

                        if (loopState == LoopState_e::in_break || inReturn)
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
                            return;
                        } 
                        
                        // no actual action, we are going to loop anyways
                        if (loopState == LoopState_e::in_continue)
                            loopState = LoopState_e::run;
                    }
                    break;
                case cvar::valueType::DICT:
                    for (const auto& i : *source.getDict())
                    {
                        *reference = i.first;
                        lambda(inst->index, currentRow);

                        if (loopState == LoopState_e::in_break || inReturn)
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
                            return;
                        } 
                        
                        // no actual action, we are going to loop anyways
                        if (loopState == LoopState_e::in_continue)
                            loopState = LoopState_e::run;
                    }
                    break;
                case cvar::valueType::SET:
                    for (const auto& i : *source.getSet())
                    {
                        *reference = i;
                        lambda(inst->index, currentRow);

                        if (loopState == LoopState_e::in_break || inReturn)
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
                            return;
                        } 
                        
                        // no actual action, we are going to loop anyways
                        if (loopState == LoopState_e::in_continue)
                            loopState = LoopState_e::run;
                    }
                    break;
                }

            }           
        }
            break;
        case OpCode_e::CALL_IF: // execute lambda, and if not 0 on stack
        {
            // anything not 0 is true
            if (lambda(inst->extra, currentRow)->isEvalTrue())
            {
                lambda(inst->index, currentRow);
                if (inReturn) // error state?
                    return; 
                
                // fast forward passed subsequent elif/else ops
                ++inst;
                //while (inst->op == OpCode_e::CNDELIF || inst->op == OpCode_e::CNDELSE)
                //    ++inst; // we've advanced the instruction pointer
                // loop to top
                continue;
            }
        }
            break;
        case OpCode_e::CALL_EACH:
        {
            auto startRow = currentRow;
            const auto codeBlock = inst->index;
            const auto logicLambda = inst->extra;
            const auto filter = macros.filters[inst->value];

            const auto rowCount  = static_cast<int>(rows->size());
            const auto savedRow = currentRow; // reset row position if using ITFORR, ITFORRC, ITFORRCF

            // .continue - are we continuing from a specific row 
            if (filter.isContinue)
                currentRow = lambda(filter.continueBlock, currentRow)->getInt64();
            else
                filter.isReverse ? currentRow = rowCount - 1 : currentRow = 0;
            
            // .next - are we advancing the cursor
            if (filter.isNext)
               filter.isReverse ? --currentRow : ++currentRow;

            // .limit - set match limit
            int64_t matches = 0;
            auto matchLimit = LLONG_MAX;
            if (filter.isLimit)
                matchLimit = lambda(filter.limitBlock, currentRow)->getInt64();

            // .range - set date limiters
            auto startStamp = LLONG_MIN;
            auto endStamp = LLONG_MAX;

            if (filter.isRange)
            {
                startStamp = convertStamp(*lambda(filter.rangeStartBlock, currentRow));
                endStamp = convertStamp(*lambda(filter.rangeEndBlock, currentRow));
            }

            // .within - within is constrained to limits defined in .range
            if (filter.isWithin || filter.isLookAhead || filter.isLookBack)
            {
                const auto withinStart = convertStamp(*lambda(filter.withinStartBlock, currentRow));
                const auto withinWindow = lambda(filter.withinWindowBlock, currentRow)->getInt64();

                int64_t rangeStart, rangeEnd;

                if (filter.isLookAhead)
                {
                    rangeStart = withinStart;
                    rangeEnd = withinStart + withinWindow;
                }
                else if (filter.isLookBack)
                {
                    rangeStart = withinStart - withinWindow;
                    rangeEnd = withinStart;
                }
                else
                {
                    rangeStart = withinStart - withinWindow;
                    rangeEnd = withinStart + withinWindow;                    
                }

                if (rangeStart > startStamp)
                    startStamp = rangeStart;

                if (rangeEnd < endStamp)
                    endStamp = rangeEnd;
            }

            filterRangeStack.emplace_back(startStamp, endStamp);

            ++nestDepth;

            // Iterate
            while (matches < matchLimit && currentRow < rowCount && currentRow >= 0)
            {
                if (loopState == LoopState_e::in_exit || error.inError())
                {
                    *stackPtr = 0;
                    ++stackPtr;
                    --nestDepth;
                    --recursion;
                    return;
                }                        // set the value of referenced `for variable` to the current row number

                if ((*rows)[currentRow]->cols[COL_STAMP] < startStamp)
                {
                    if (filter.isReverse)
                        break;
                    ++currentRow;
                    continue;
                }

                if ((*rows)[currentRow]->cols[COL_STAMP] > endStamp)
                {
                    if (filter.isReverse)
                    {
                        --currentRow;
                        continue;
                    }
                    break;
                }
                
                if (logicLambda == -1 || lambda(logicLambda, currentRow)->isEvalTrue())
                {
                    lambda(codeBlock, currentRow);
                    ++matches;
                }

                if (loopState == LoopState_e::in_break || inReturn)
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
                    return;
                } 
                
                // no actual action, we are going to loop anyways
                if (loopState == LoopState_e::in_continue)
                    loopState = LoopState_e::run;

                filter.isReverse ? --currentRow : ++currentRow;
            }

            --nestDepth;

            filterRangeStack.pop_back();
            currentRow = savedRow;
        }
            break;

        case OpCode_e::PSHTBLFLT:
        {
            auto startRow = currentRow;
            const auto filter = macros.filters[inst->value];
            const auto rowCount  = static_cast<int>(rows->size());
            const auto savedRow = currentRow; // reset row position if using ITFORR, ITFORRC, ITFORRCF

            // THROW (in compiler?) isNext but not isLookAhead or isLookBack
            
            // .next - are we advancing the cursor
            if (!filter.isRow && filter.isNext)
               filter.isReverse ? --currentRow : ++currentRow;

            // .range - set date limiters
            auto startStamp = LLONG_MIN;
            auto endStamp = LLONG_MAX;

            // column filters inherit date ranges carry from `each` or `if` filters
            if (filterRangeStack.size() && filterRangeStack.back().first != LLONG_MIN)
            {
                startStamp = filterRangeStack.back().first;
                endStamp = filterRangeStack.back().second;
            }

            if (filter.isRange)
            {
                startStamp = convertStamp(*lambda(filter.rangeStartBlock, currentRow));
                endStamp = convertStamp(*lambda(filter.rangeEndBlock, currentRow));
            }

            // .within - within is constrained to limits defined in .range
            if (filter.isWithin || filter.isLookAhead || filter.isLookBack)
            {
                const auto withinStart = convertStamp(*lambda(filter.withinStartBlock, currentRow));
                const auto withinWindow = lambda(filter.withinWindowBlock, currentRow)->getInt64();

                int64_t rangeStart, rangeEnd;

                if (filter.isLookAhead)
                {
                    rangeStart = withinStart;
                    rangeEnd = withinStart + withinWindow;
                }
                else if (filter.isLookBack)
                {
                    rangeStart = withinStart - withinWindow;
                    rangeEnd = withinStart;
                }
                else
                {
                    rangeStart = withinStart - withinWindow;
                    rangeEnd = withinStart + withinWindow;                    
                }

                if (rangeStart > startStamp)
                    startStamp = rangeStart;

                if (rangeEnd < endStamp)
                    endStamp = rangeEnd;
            }

            auto pass = false;
            
            if (filter.isRow)
            {
                pass = lambda(filter.evalBlock, currentRow)->isEvalTrue();
            }
            else if (filter.isEver)
            {
                currentRow = 0;
                while (currentRow < rowCount && currentRow >= 0)
                {
                    if ((*rows)[currentRow]->cols[COL_STAMP] < startStamp)
                    {
                        if (filter.isReverse)
                            break;
                        ++currentRow;
                        continue;
                    }

                    if ((*rows)[currentRow]->cols[COL_STAMP] > endStamp)
                    {
                        if (filter.isReverse)
                        {
                            --currentRow;
                            continue;
                        }
                        break;
                    }
                    
                    if (lambda(filter.evalBlock, currentRow)->isEvalTrue())
                    {
                        pass = true;
                        break;
                    }

                    filter.isReverse ? --currentRow : ++currentRow;
                }                          
            }

            *stackPtr = filter.isNegated ? !pass : pass;
            ++stackPtr;
            currentRow = savedRow;
        }
            break;

        default:
            break;
        } // move to the next instruction
        ++inst;    
    }
    --recursion;
}

void openset::query::Interpreter::setScheduleCB(const function<void(int64_t functionHash, int seconds)>& cb)
{
    schedule_cb = cb;
}

void openset::query::Interpreter::setEmitCB(const function<void(string emitMessage)>& cb)
{
    emit_cb = cb;
}

void openset::query::Interpreter::setGetSegmentCB(const function<IndexBits*(const string&, bool&)>& cb)
{
    getSegment_cb = cb;
}

void openset::query::Interpreter::setBits(IndexBits* indexBits, const int maxPopulation)
{
    bits      = indexBits;
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
    loopCount  = 0;
    recursion  = 0;
    nestDepth  = 0;
    breakDepth = 0;
    eventCount = -1;
    inReturn   = false;
    jobState   = false;
    loopState  = LoopState_e::run;
    stackPtr   = stack; //matchStampPrev.clear();
    eventDistinct.clear();
    for (auto i = 0; i < STACK_DEPTH; ++i)
        stack[i].clear();   
}

void openset::query::Interpreter::exec()
{
    returns.clear(); // cannot be cleared in segment loop
    const auto inst = &macros.code.front();
    //try
    {
        // if we have segment constraints
        if (segmentIndexes.size())
        {
            segmentColumnShift = 0;
            for (auto seg : segmentIndexes)
            {
                if (seg->bitState(linid)) // if the person is in this segment run the ops
                {
                    execReset();
                    opRunner(inst, 0);
                }
                if (stackPtr <= stack)
                    returns.push_back(NONE); // return NONE if stack is unwound
                else
                    returns.push_back(*(stackPtr - 1)); // capture last value on stack
                // for each segment we offset the results by the number of columns
                segmentColumnShift += macros.vars.columnVars.size();
            }
        }
        else
        {
            segmentColumnShift = 0;
            execReset();
            opRunner(inst, 0);
            if (stackPtr <= stack)
                returns.push_back(NONE); // return NONE if stack is unwound
            else
                returns.push_back(*(stackPtr - 1)); // capture last value on stack
        }

        // write back props (checks for change by hashing)
        if (macros.useProps && propsIndex != -1)
            grid->setProps(macros.vars.userVars[propsIndex].value);

    }
   /*catch (const std::runtime_error& ex)
    {
        std::string additional = "";
        if (lastDebug)
            additional = lastDebug->toStrShort();
        error.set(
            errors::errorClass_e::run_time,
            errors::errorCode_e::run_time_exception_triggered,
            std::string { ex.what() } + " (2)",
            additional);
    } catch (...)
    {
        std::string additional = "";
        if (lastDebug)
            additional = lastDebug->toStrShort();
        error.set(
            errors::errorClass_e::run_time,
            errors::errorCode_e::run_time_exception_triggered,
            "unknown run-time error (3)",
            additional);
    }
    */

}

void openset::query::Interpreter::exec(const string& functionName)
{
    exec(MakeHash(functionName));
}

void openset::query::Interpreter::exec(const int64_t functionHash)
{
    returns.clear(); // cannot be cleared in segment loop
    for (auto& f : macros.vars.functions)
    {
        if (f.nameHash == functionHash)
        {
            calledFunction  = f.name;
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
                        {
                            execReset();
                            opRunner(inst, 0);
                        }
                        if (stackPtr <= stack)
                            returns.push_back(NONE); // return NONE if stack is unwound
                        else
                            returns.push_back(*(stackPtr - 1)); // capture last value on stack
                        // for each segment we offset the results by the number of columns
                        segmentColumnShift += macros.vars.columnVars.size();
                    }
                }
                else
                {
                    segmentColumnShift = 0;
                    execReset();
                    opRunner(inst, 0);
                    if (stackPtr <= stack)
                        returns.push_back(NONE); // return NONE if stack is unwound
                    else
                        returns.push_back(*(stackPtr - 1)); // capture last value on stack
                }
            }
            catch (const std::runtime_error& ex)
            {
                std::string additional = "";
                if (lastDebug)
                    additional = lastDebug->toStrShort();
                error.set(
                    errors::errorClass_e::run_time,
                    errors::errorCode_e::run_time_exception_triggered,
                    std::string { ex.what() } + " (2)",
                    additional);
            } catch (...)
            {
                std::string additional = "";
                if (lastDebug)
                    additional = lastDebug->toStrShort();
                error.set(
                    errors::errorClass_e::run_time,
                    errors::errorCode_e::run_time_exception_triggered,
                    "unknown run-time error (3)",
                    additional);
            }
            // write back props (checks for change by hashing)
            if (macros.useProps && propsIndex != -1)
                grid->setProps(macros.vars.userVars[propsIndex].value);

            return;
        }
    }
    error.set(
        errors::errorClass_e::run_time,
        errors::errorCode_e::missing_function_entry_point,
        "function_id: " + to_string(functionHash));
}

openset::query::Interpreter::Returns& openset::query::Interpreter::getLastReturn()
{
    return returns;
}
