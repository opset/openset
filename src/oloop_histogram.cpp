#include "oloop_histogram.h"
#include "indexbits.h"
#include "asyncpool.h"
#include "tablepartitioned.h"
#include "internoderouter.h"

using namespace openset::async;
using namespace openset::query;
using namespace openset::result;

// yes, we are passing queryMacros by value to get a copy
OpenLoopHistogram::OpenLoopHistogram(
    ShuttleLambda<CellQueryResult_s>* shuttle,
    openset::db::Database::TablePtr table,
    Macro_s macros,
    std::string groupName,
    std::string eachProperty,
    const int64_t bucket,
    openset::result::ResultSet* result,
    const int instance)
    : OpenLoop(table->getName(), oloopPriority_e::realtime),
      // queries are high priority and will preempt other running cells
      macros(std::move(macros)),
      shuttle(shuttle),
      groupName(std::move(groupName)),
      eachColumn(std::move(eachProperty)),
      table(table),
      bucket(bucket),
      parts(nullptr),
      maxLinearId(0),
      currentLinId(-1),
      interpreter(nullptr),
      instance(instance),
      runCount(0),
      startTime(0),
      population(0),
      index(nullptr),
      result(result)
{}

OpenLoopHistogram::~OpenLoopHistogram()
{
    if (interpreter)
    {
        // free up any segment bits we may have made
        //for (auto bits : interpreter->segmentIndexes)
          //  delete bits;

        delete interpreter;
    }
}

void OpenLoopHistogram::prepare()
{
    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    maxLinearId = parts->people.customerCount();

    // generate the index for this query
    indexing.mount(table.get(), macros, loop->partition, maxLinearId);
    bool countable;
    index = indexing.getIndex("_", countable);
    population = index->population(maxLinearId);

    interpreter = new Interpreter(macros);
    interpreter->setResultObject(result);

    if (eachColumn.length())
    {
        propInfo = table->getProperties()->getProperty(eachColumn);

        if (!propInfo)
        {
            shuttle->reply(
                0,
                result::CellQueryResult_s {
                    instance,
                    {},
                    openset::errors::Error {
                        openset::errors::errorClass_e::run_time,
                        openset::errors::errorCode_e::item_not_found,
                        "missing foreach column '" + eachColumn + "'"
                    }
                }
            );
            suicide();
            return;
        }

        valueList = parts->attributes.getPropertyValues(propInfo->idx);

        for (auto &v : macros.vars.userVars)
            if (v.actual == "each_value")
                eachVarIdx = v.index;

        if (eachVarIdx == -1)
        {
            shuttle->reply(
                0,
                result::CellQueryResult_s {
                    instance,
                    {},
                    openset::errors::Error {
                        openset::errors::errorClass_e::run_time,
                        openset::errors::errorCode_e::item_not_found,
                        "'foreach' specified in query, but the 'each_value' variable was not found in the script."
                    }
                }
            );
            suicide();
            return;
        }
    }

    // if we are in segment compare mode:
    if (macros.segments.size())
    {
        std::vector<IndexBits*> segments;

        for (const auto& segmentName : macros.segments)
        {
            if (segmentName == "*")
            {
                auto tBits = new IndexBits();
                tBits->makeBits(maxLinearId, 1);
                segments.push_back(tBits);
            }
            else
            {
                /*auto attr = parts->attributes.get(PROP_SEGMENT, MakeHash(segmentName));
                if (attr)
                {
                    segments.push_back(attr->getBits());
                }
                else
                {
                    shuttle->reply(
                        0,
                        result::CellQueryResult_s {
                            instance,
                            {},
                            openset::errors::Error {
                                openset::errors::errorClass_e::run_time,
                                openset::errors::errorCode_e::item_not_found,
                                "missing segment '" + segmentName + "'"
                            }
                        }
                    );
                    suicide();
                    return;
                }*/
                if (!parts->segments.count(segmentName))
                {
                    shuttle->reply(
                        0,
                        result::CellQueryResult_s{
                            instance,
                        {},
                        openset::errors::Error{
                            openset::errors::errorClass_e::run_time,
                            openset::errors::errorCode_e::item_not_found,
                            "missing segment '" + segmentName + "'"
                        }
                        }
                    );
                    suicide();
                    return;
                }

                segments.push_back(parts->segments[segmentName].getBits(parts->attributes));

            }
        }
        interpreter->setCompareSegments(index, segments);
    }

    auto mappedColumns = interpreter->getReferencedColumns();

    // map table, partition and select schema properties to the Customer object
    if (!person.mapTable(table.get(), loop->partition, mappedColumns))
    {
        partitionRemoved();
        suicide();
        return;
    }

    person.setSessionTime(macros.sessionTime);

    rowKey.clear();
    rowKey.key[0] = MakeHash(groupName);
    result->addLocalText(rowKey.key[0], groupName);

    rowKey.types[0] = ResultTypes_e::Text;

    if (valueList.size()) // if we are foreach mode
    {
        switch (propInfo->type)
        {
        case PropertyTypes_e::intProp:
            rowKey.types[1] = ResultTypes_e::Int;
            break;
        case PropertyTypes_e::doubleProp:
            rowKey.types[1] = ResultTypes_e::Double;
            break;
        case PropertyTypes_e::boolProp:
            rowKey.types[1] = ResultTypes_e::Bool;
            break;
        case PropertyTypes_e::textProp:
            rowKey.types[1] = ResultTypes_e::Text;
            break;
        case PropertyTypes_e::freeProp:
        default: ;
        }

        rowKey.types[2] = ResultTypes_e::Double;
    }
    else
    {
        rowKey.types[1] = ResultTypes_e::Double;
    }

    startTime = Now();
}

bool OpenLoopHistogram::run()
{
    while (true)
    {
        if (sliceComplete())
            return true;

        // are we done? This will return the index of the
        // next set bit until there are no more, or maxLinId is met
        if (interpreter->error.inError() || !index->linearIter(currentLinId, maxLinearId))
        {
            shuttle->reply(
                0,
                CellQueryResult_s {
                    instance,
                    {},
                    interpreter->error,
                });

            suicide();
            return false;
        }

        if (const auto personData = parts->people.getCustomerByLIN(currentLinId); personData != nullptr)
        {
            ++runCount;

            person.mount(personData);
            person.prepare();
            interpreter->mount(&person);

            if (valueList.size())
            {
                int64_t key1Value;

                for (auto& itemValue : valueList)
                {
                    switch (propInfo->type)
                    {
                    case PropertyTypes_e::intProp:
                        key1Value = itemValue.first;
                        interpreter->macros.vars.userVars[eachVarIdx].value = itemValue.first;
                        break;
                    case PropertyTypes_e::doubleProp:
                        key1Value = itemValue.first;
                        interpreter->macros.vars.userVars[eachVarIdx].value =
                            static_cast<double>(itemValue.first) / 10000.0;
                        break;
                    case PropertyTypes_e::boolProp:
                        key1Value = itemValue.first;
                        interpreter->macros.vars.userVars[eachVarIdx].value = (itemValue.first != 0);
                        break;
                    case PropertyTypes_e::textProp:
                        if (itemValue.second->text)
                        {
                            result->addLocalText(itemValue.first, itemValue.second->text);
                            key1Value = itemValue.first;
                            interpreter->macros.vars.userVars[eachVarIdx].value = itemValue.second->text;
                        }
                        else
                            continue;
                        break;
                    case PropertyTypes_e::freeProp:
                    default:
                        continue;
                    }

                    interpreter->exec(); // run the script on this customer - do some magic
                    auto returns = interpreter->getLastReturn();

                    auto idx = -1;
                    for (auto& r : returns)
                    {
                        ++idx;

                        if (r == NONE)
                            continue;

                        auto value = static_cast<int64_t>(r.getDouble() * 10000.0);

                        // bucket the key if it's non-zero
                        if (bucket)
                            value = (value / bucket) * bucket;

                        rowKey.key[1] = NONE;
                        rowKey.key[2] = NONE;

                        auto aggs = result->getMakeAccumulator(rowKey);

                        if (aggs->columns[idx].value == NONE)
                            aggs->columns[idx].value = 1;
                        else
                            ++aggs->columns[idx].value;

                        rowKey.key[1] = key1Value;
                        rowKey.key[2] = NONE;

                        aggs = result->getMakeAccumulator(rowKey);

                        if (aggs->columns[idx].value == NONE)
                            aggs->columns[idx].value = 1;
                        else
                            ++aggs->columns[idx].value;

                        // set the key
                        rowKey.key[2] = value;

                        aggs = result->getMakeAccumulator(rowKey);

                        if (aggs->columns[idx].value == NONE)
                            aggs->columns[idx].value = 1;
                        else
                            ++aggs->columns[idx].value;
                    }
                }
            }
            else
            {
                interpreter->exec(); // run the script on this customer - do some magic
                auto returns = interpreter->getLastReturn();

                auto idx = -1;
                for (auto& r : returns)
                {
                    ++idx;

                    if (r == NONE)
                        continue;

                    auto value = static_cast<int64_t>(r.getDouble() * 10000.0);

                    // bucket the key if it's non-zero
                    if (bucket)
                        value = (value / bucket) * bucket;

                    rowKey.key[1] = NONE;

                    auto aggs = result->getMakeAccumulator(rowKey);
                    if (aggs->columns[idx].value == NONE)
                        aggs->columns[idx].value = 1;
                    else
                        ++aggs->columns[idx].value;

                    // set the key
                    rowKey.key[1] = value;

                    aggs = result->getMakeAccumulator(rowKey);
                    if (aggs->columns[idx].value == NONE)
                        aggs->columns[idx].value = 1;
                    else
                        ++aggs->columns[idx].value;
                }
            }
        }
    }
}

void OpenLoopHistogram::partitionRemoved()
{
    shuttle->reply(
        0,
        CellQueryResult_s {
            instance,
            {},
            openset::errors::Error {
                openset::errors::errorClass_e::run_time,
                openset::errors::errorCode_e::partition_migrated,
                "please retry query"
            }
        });
}
