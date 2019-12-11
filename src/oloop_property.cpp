#include "oloop_property.h"

#include "table.h"
#include "tablepartitioned.h"
#include "indexbits.h"
#include "errors.h"
#include "result.h"
#include "attributes.h"

using namespace openset::async;
using namespace openset::result;


OpenLoopProperty::OpenLoopProperty(
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
{
}

OpenLoopProperty::~OpenLoopProperty()
{
    //for (auto s: segments)
      //  delete s;
}

void OpenLoopProperty::prepare()
{
    parts = table->getPartitionObjects(loop->partition, false);

    if (!parts)
    {
        suicide();
        return;
    }

    stopBit = parts->people.customerCount();

    // if we are in segment compare mode:
    if (config.segments.size())
    {
        for (const auto& segmentName : config.segments)
        {
            if (segmentName == "*")
            {
                all.makeBits(stopBit, 1); // make an index of all ones.
                segments.push_back(segmentName);
            }
            else
            {
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

                segments.push_back(segmentName);
            }
        }
    }

    // get the root value
    const auto allBits = parts->attributes.getBits(config.propIndex, NONE);

    if (!allBits)
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

    createRootNode();

    // turn ints and doubles into their bucketed name
    const auto toBucket = [&](const int64_t value)->int64_t
    {
        if (config.bucket == 0)
            return value;

        const auto intBucket = config.bucket.getInt64();
        return (value / intBucket) * intBucket;
    };

    /*
     *  Here we convert a list of values (for the property) into
     *  a group of buckets.
     *
     *  Buckets are groups created by grouping numeric values by nearest value.
     *
     *  Each bucket contains a list of the property `values` that match the bucket.
     *
     *  Later the index bits for each `value` in the bucket are `OR`ed together
     *  then `AND`ed to the index.
     */

    for (auto &v : parts->attributes.getPropertyValues(config.propIndex))
    {

        auto groupKey = toBucket(v.first); // we only call that lambda once... hmmm...

        if (groups.find(groupKey) == groups.end())
            groups.emplace(groupKey, Ids{});

        auto& bucketList = groups[groupKey];

        switch (config.mode)
        {
        case PropertyQueryMode_e::all:
            bucketList.push_back(v.first); // value hash (or value)
            break;
        case PropertyQueryMode_e::rx:
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
        case PropertyQueryMode_e::sub:
            if (v.second->text &&
                string(v.second->text).find(config.filterLow.getString()) != string::npos)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::gt:
            if (v.first > config.filterLow)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::gte:
            if (v.first >= config.filterLow)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::lt:
            if (v.first < config.filterLow)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::lte:
            if (v.first <= config.filterLow)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::eq:
            if (v.first == config.filterLow)
                bucketList.push_back(v.first);
            break;
        case PropertyQueryMode_e::between:
            if (v.first >= config.filterLow &&
                v.first < config.filterHigh)
                bucketList.push_back(v.first);
            break;
        default:;
        }
    }

    groupsIter = groups.begin();
}

void OpenLoopProperty::createRootNode()
{
    rowKey.clear();

    rowKey.key[0] = result->addLocalTextAndHash(config.propName);
    rowKey.types[0] = ResultTypes_e::Text;

    // assign the type for the value to the key
    switch (config.propType)
    {
        case db::PropertyTypes_e::intProp:
            rowKey.types[1] = ResultTypes_e::Int;
        break;
        case db::PropertyTypes_e::doubleProp:
            rowKey.types[1] = ResultTypes_e::Double;
        break;
        case db::PropertyTypes_e::boolProp:
            rowKey.types[1] = ResultTypes_e::Bool;
        break;
        case db::PropertyTypes_e::textProp:
            rowKey.types[1] = ResultTypes_e::Text;
        break;
        default: ;
    }

    result->getMakeAccumulator(rowKey);
}

void OpenLoopProperty::addRootTotal()
{
    rowKey.clear();

    rowKey.key[0] = result->addLocalTextAndHash(config.propName);
    rowKey.types[0] = ResultTypes_e::Text;

    const auto aggs = result->getMakeAccumulator(rowKey);

    auto idx = 0;
    for (auto &segmentName : segments)
    {
        db::IndexBits* segmentBits = segmentName == "*" ? &all : parts->getSegmentBits(segmentName);
        db::IndexBits bits;
        bits.opCopy(rootCount);
        bits.opAnd(*segmentBits);
        aggs->columns[idx].value = bits.population(stopBit);
        ++idx;
    }
}

bool OpenLoopProperty::run()
{

    while (true)
    {
        // are we done? This will return the index of the
        // next set bit until there are no more, or maxLinId is met

        while (groupsIter != groups.end())
        {
            if (sliceComplete())
                return true;

            const auto bucket = groupsIter->first;

            // we skip empty groups (these are empty due to filtering)
            if (!groupsIter->second.size())
            {
                ++groupsIter;
                continue;
            }

            auto columnIndex = 0;
            for (const auto& segmentName : segments)
            {
                // here we are setting the key for the bucket,
                // this is under our root which is the property name
                rowKey.key[1] = bucket; // value hash (or value)

                const auto segmentBits = segmentName == "*" ? &all : parts->getSegmentBits(segmentName);
                const auto aggs = result->getMakeAccumulator(rowKey);

                auto sumBits = new db::IndexBits();

                for (auto value : groupsIter->second)
                {
                    const auto bits = parts->attributes.getBits(config.propIndex, value);

                    if (!bits)
                        continue;

                    sumBits->opOr(*bits);
                }

                rootCount.opOr(*sumBits);

                // remove bits not in the segment
                sumBits->opAnd(*segmentBits);

                aggs->columns[columnIndex].value = sumBits->population(stopBit);
                delete sumBits;

                // we are going to handle text a little different here
                // text isn't bucketed (at the moment, rx capture may allow us to
                // do this in the future), so, bucket will always be value
                if (config.propType == db::PropertyTypes_e::textProp)
                {
                    const auto attr = parts->attributes.get(config.propIndex, bucket);
                    if (attr && attr->text)
                        result->addLocalText(bucket, attr->text);
                }

                ++columnIndex;
            }

            ++groupsIter;
        }

        if (groupsIter == groups.end())
        {
            addRootTotal();

            shuttle->reply(
                0,
                result::CellQueryResult_s{
                instance,
                {},
                errors::Error{}
            }
            );

            suicide();
            return false;
        }

    }
}

void OpenLoopProperty::partitionRemoved()
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
