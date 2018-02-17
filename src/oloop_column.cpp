#include "oloop_column.h"

#include "table.h"
#include "tablepartitioned.h"
#include "indexbits.h"
#include "errors.h"
#include "result.h"
#include "attributes.h"

using namespace openset::async;
using namespace openset::result;


OpenLoopColumn::OpenLoopColumn(
    ShuttleLambda<CellQueryResult_s>* shuttle,
    openset::db::Database::TablePtr table,
    ColumnQueryConfig_s config,
    openset::result::ResultSet* result,
    const int64_t instance):
        OpenLoop(table->getName(), oloopPriority_e::realtime),
        shuttle(shuttle),
        config(std::move(config)),
        table(table),
        result(result),
        instance(instance)
{}

OpenLoopColumn::~OpenLoopColumn()
{
    for (auto s: segments)
        delete s;
}

void OpenLoopColumn::prepare()
{  
    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    stopBit = parts->people.peopleCount();

    // if we are in segment compare mode:
    if (config.segments.size())
    {
        for (const auto& segmentName : config.segments)
        {
            if (segmentName == "*")
            {
                auto bits = new db::IndexBits();
                bits->makeBits(stopBit, 1); // make an index of all ones.
                segments.push_back(bits);
            }
            else
            {
                auto attr = parts->attributes.get(db::COL_SEGMENT, MakeHash(segmentName));
                if (attr)
                {
                    segments.push_back(attr->getBits());
                }
                else
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
            }
        }
    }

    // get the root value
    const auto all = parts->attributes.get(config.columnIndex, NONE);

    if (!all)
    {
        shuttle->reply(
            0,
            result::CellQueryResult_s{
            instance,
            {},
            errors::Error{},
        });

        suicide();
        return;
    }

    rowKey.clear();

    const auto hash = MakeHash(config.columnName);
    result->addLocalText(MakeHash(config.columnName), config.columnName);

    rowKey.key[0] = hash;
    rowKey.types[0] = ResultTypes_e::Text;

    // assign the type for the value to the key
    switch (config.columnType)
    {
        case db::columnTypes_e::intColumn: 
            rowKey.types[1] = ResultTypes_e::Int;
        break;
        case db::columnTypes_e::doubleColumn: 
            rowKey.types[1] = ResultTypes_e::Double;
        break;
        case db::columnTypes_e::boolColumn: 
            rowKey.types[1] = ResultTypes_e::Bool;
        break;
        case db::columnTypes_e::textColumn: 
            rowKey.types[1] = ResultTypes_e::Text;
        break;
        default: ;
    }
  
    const auto aggs = result->getMakeAccumulator(rowKey);

    auto idx = 0;
    for (auto s : segments)
    {
        auto bits = all->getBits();
        bits->opAnd(*s);
        aggs->columns[idx].value = bits->population(stopBit);
        delete bits;

        ++idx;
    }

    // turn ints and doubles into their bucketed name
    auto toBucket = [&](const int64_t value)->int64_t
    {
        if (config.bucket == 0)
            return value;

        const auto intBucket = config.bucket.getInt64();
        return (value / intBucket) * intBucket;
    };

    /*
     *  Here we convert a list of values (for the column) into 
     *  a group of buckets. 
     *  
     *  Buckets are groups created by grouping numeric values by nearest value.
     *  
     *  Each bucket contains a list of the column `values` that match the bucket.
     *  
     *  Later the index bits for each `value` in the bucket are `OR`ed together
     *  then `AND`ed to the index.
     */

    for (auto &v : parts->attributes.getColumnValues(config.columnIndex))
    {

        auto groupKey = toBucket(v.first); // we only call that lambda once... hmmm...

        if (groups.find(groupKey) == groups.end())
            groups.emplace(groupKey, Ids{});

        auto& bucketList = groups[groupKey];
        
        switch (config.mode)
        {
        case ColumnQueryMode_e::all:
            bucketList.push_back(v.first); // value hash (or value)
            break;
        case ColumnQueryMode_e::rx:
        {
            if (v.second->text)
            {
                std::smatch matches;
                const std::string tString{ v.second->text };
                if (regex_match(tString, matches, config.rx))
                    bucketList.push_back(v.first);
            }
        }
            break;
        case ColumnQueryMode_e::sub:
            if (v.second->text &&
                string(v.second->text).find(config.filterLow.getString()) != string::npos)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::gt:
            if (v.first > config.filterLow)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::gte:
            if (v.first >= config.filterLow)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::lt:
            if (v.first < config.filterLow)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::lte:
            if (v.first <= config.filterLow)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::eq:
            if (v.first == config.filterLow)
                bucketList.push_back(v.first);
            break;
        case ColumnQueryMode_e::between:
            if (v.first >= config.filterLow &&
                v.first < config.filterHigh)
                bucketList.push_back(v.first);
            break;
        default:;
        }
    }

    groupsIter = groups.begin();
}

void OpenLoopColumn::run()
{

    while (true)
    {
        // are we done? This will return the index of the 
        // next set bit until there are no more, or maxLinId is met

        while (groupsIter != groups.end())
        {
            if (sliceComplete())
                return;

            const auto bucket = groupsIter->first;

            // we skip empty groups (these are empty due to filtering)
            if (!groupsIter->second.size())
            {
                ++groupsIter;
                continue;
            }

            auto columnIndex = 0;
            for (auto s : segments)
            {

                // here we are setting the key for the bucket,
                // this is under our root which is the column name
                rowKey.key[1] = bucket; // value hash (or value)


                const auto aggs = result->getMakeAccumulator(rowKey);

                auto sumBits = new db::IndexBits();

                for (auto value : groupsIter->second)
                {

                    auto attr = parts->attributes.get(config.columnIndex, value);

                    if (!attr)
                        continue;

                    const auto bits = attr->getBits();
                    sumBits->opOr(*bits);
                    delete bits;
                }

                // remove bits not in the segment
                sumBits->opAnd(*s);

                aggs->columns[columnIndex].value = sumBits->population(stopBit);
                delete sumBits;

                // we are going to handle text a little different here
                // text isn't bucketed (at the moment, rx capture may allow us to 
                // do this in the future), so, bucket will always be value
                if (config.columnType == db::columnTypes_e::textColumn)
                {
                    const auto attr = parts->attributes.get(config.columnIndex, bucket);
                    if (attr && attr->text)
                        result->addLocalText(bucket, attr->text);
                }

                ++columnIndex;
            }

            ++groupsIter;
        }

        if (groupsIter == groups.end())
        {
            shuttle->reply(
                0,
                result::CellQueryResult_s{
                instance,
                {},
                errors::Error{}
            }
            );
            suicide();
            return;
        }

    }
}

void OpenLoopColumn::partitionRemoved()
{
    shuttle->reply(
        0,
        result::CellQueryResult_s{
            instance,
            {},
            openset::errors::Error{
                openset::errors::errorClass_e::run_time,
                openset::errors::errorCode_e::partition_migrated,
                "please retry query"
            }
        }
    );
}
