#include "config.h"
#include "table.h"
//#include "trigger.h"
#include "errors.h"
#include "tablepartitioned.h"
#include "file/file.h"
#include "file/directory.h"
#include "database.h"
#include "asyncpool.h"
#include "internoderouter.h"
#include "queryinterpreter.h"

using namespace openset::db;

Table::Table(const string &name, const bool numericIds, Database* database):
    name(name),
    database(database),
    loadVersion(Now()),
    numericCustomerIds(numericIds),
    tableHash(MakeHash(name))
{}

Table::~Table()
{
    for (auto &part : partitions)
    {
        delete part.second;
        part.second = nullptr;
    }

    Logger::get().info("table dropped '" + name + "'.");
}

Table::TablePtr openset::db::Table::getSharedPtr() const
{
    return this->database->getTable(name);
}

void openset::db::Table::initialize()
{
    // initialize the var object as a dictionary
    globalVars.dict();

    // set the default required properties
    properties.setProperty(PROP_STAMP, "stamp", PropertyTypes_e::intProp, false);
    properties.setProperty(PROP_EVENT, "event", PropertyTypes_e::textProp, false);
    properties.setProperty(PROP_UUID, "id", PropertyTypes_e::intProp, false);
    properties.setProperty(PROP_SEGMENT, "__segment", PropertyTypes_e::textProp, false);
    properties.setProperty(PROP_SESSION, "session", PropertyTypes_e::intProp, false);

    createMissingPartitionObjects();
    Logger::get().info("table created '" + name + "'.");
}

void Table::createMissingPartitionObjects()
{
    globals::async->assertAsyncLock();

    auto myPartitions = globals::mapper->partitionMap.getPartitionsByNodeId(globals::running->nodeId);

    for (auto p: myPartitions)
        getPartitionObjects(p, true);
}

TablePartitioned* Table::getPartitionObjects(const int32_t partition, const bool create)
{
    {
    csLock lock(cs); // scoped lock

    clearZombies();

    if (auto const part = partitions.find(partition); part != partitions.end())
        return part->second;
    }

    if (!create)
        return nullptr;

    const auto part = new TablePartitioned(
        this,
        partition,
        &attributeBlob,
        &properties);

    {
        csLock lock(cs); // scoped lock
        partitions[partition] = part;
        return part;
    }
}

void Table::releasePartitionObjects(const int32_t partition)
{
    csLock lock(cs); // lock for read

    if (const auto part = partitions.find(partition); part != partitions.end())
    {
        part->second->markForDeletion();
        zombies.push(part->second);
        partitions.erase(partition);
    }
}

void Table::propagateCustomerIndexes()
{
    for (auto& part : partitions)
        part.second->attributes.createCustomerPropIndexes();
}

void Table::setSegmentRefresh(
    const std::string& segmentName,
    const openset::query::Macro_s& macros,
    const int64_t refreshTime,
    const int zIndex,
    const bool onInsert)
{
    csLock lock(segmentCS);

    const auto scriptHash = MakeHash(macros.rawScript);

    // lets see if it exists and if the script has a different hash we will replace it.
    if (segmentRefresh.count(segmentName))
    {
        if (segmentRefresh[segmentName].lastHash == scriptHash &&
            segmentRefresh[segmentName].zIndex == zIndex &&
            segmentRefresh[segmentName].refreshTime == refreshTime &&
            segmentRefresh[segmentName].onInsert == onInsert)
            return;
    }

    segmentRefresh.erase(segmentName);
    segmentRefresh.emplace(segmentName, SegmentRefresh_s { segmentName, macros, refreshTime, zIndex, onInsert });
}

void Table::removeSegmentRefresh(const std::string& segmentName)
{
    csLock lock(segmentCS);

    if (segmentRefresh.count(segmentName))
        segmentRefresh.erase(segmentName);
}

void Table::serializeTable(cjson* doc)
{
    auto pkNode = doc->setArray("z_order");

    std::vector<std::string> zList(eventOrderStrings.size());

    // convert to list by stuffing into list at index in .second
    for (auto &i : eventOrderStrings)
        zList[i.second] = i.first;

    // push the strings into the list in order
    for (auto &i : zList)
        pkNode->push(i);

    auto settings = doc->setObject("settings");
    settings->set("event_ttl", eventTtl);
    settings->set("event_max", eventMax);
    settings->set("session_time", sessionTime);
    settings->set("tz_offset", tzOffset);

    auto columnNodes = doc->setArray("properties");

    for (auto &c : properties.properties)
        if (c.deleted == 0 && c.name.size() && c.type != PropertyTypes_e::freeProp)
        {
            auto columnRecord = columnNodes->pushObject();

            std::string type;

            switch (c.type)
            {
            case PropertyTypes_e::intProp:
                type = "int";
                break;
            case PropertyTypes_e::doubleProp:
                type = "double";
                break;
            case PropertyTypes_e::boolProp:
                type = "bool";
                break;
            case PropertyTypes_e::textProp:
                type = "text";
                break;
            default:
                // TODO - this should never happen
                break;
            }

            columnRecord->set("name", c.name);
            columnRecord->set("index", cast<int64_t>(c.idx));
            columnRecord->set("type", type);
            columnRecord->set("deleted", c.deleted);
            columnRecord->set("is_set", c.isSet);
            columnRecord->set("is_prop", c.isCustomerProperty);
        }
}

void Table::serializeSettings(cjson* doc) const
{
    doc->set("event_ttl", eventTtl);
    doc->set("event_max", eventMax);
    doc->set("session_time", sessionTime);
    doc->set("tz_offset", tzOffset);
    doc->set("maint_interval", maintInterval);
    doc->set("segment_interval", segmentInterval);
    doc->set("index_compression", indexCompression);
    doc->set("person_compression", personCompression);
}

void Table::serializeTriggers(cjson* doc)
{
    // push the trigger names
    /*
    doc->setType(cjson::Types_e::OBJECT);

    for (auto &t : triggerConf)
    {
        auto trigNode = doc->setObject(t.second->name);

        trigNode->set("name", t.second->name);
        trigNode->set("entry", t.second->entryFunction);
        trigNode->set("script", t.second->script);
    }
    */
}

void Table::deserializeTable(const cjson* doc)
{
    auto count = 0;

    // load the properties
    const auto addToSchema = [&](cjson* item)
    {
        auto colName = item->xPathString("/name", "");
        auto type = item->xPathString("/type", "");
        auto index = item->xPathInt("/index", -1);
        auto isSet = item->xPathBool("/is_set", false);
        auto isProp = item->xPathBool("/is_prop", false);
        // was it deleted? > 0 = deleted, value is epoch time of deletion
        auto deleted = item->xPathInt("/deleted", 0);

        if (!type.length() || !colName.length() || index == -1)
            return;

        PropertyTypes_e colType;

        if (type == "text")
            colType = PropertyTypes_e::textProp;
        else if (type == "int")
            colType = PropertyTypes_e::intProp;
        else if (type == "double")
            colType = PropertyTypes_e::doubleProp;
        else if (type == "bool")
            colType = PropertyTypes_e::boolProp;
        else
            return; // skip

        properties.setProperty(index, colName, colType, isSet, isProp, deleted);
        count++;
    };

    // load the PK
    eventOrderStrings.clear();
    eventOrderInts.clear();
    const auto pkNode = doc->xPath("/z_order");

    // list of keys
    if (pkNode && pkNode->type() == cjson::Types_e::ARRAY)
    {

        auto nodes = pkNode->getNodes();

        auto idx = 0;
        for (auto n : nodes)
        {
            if (n->type() == cjson::Types_e::STR)
            {
                eventOrderStrings.emplace(n->getString(), idx);
                eventOrderInts.emplace(MakeHash(n->getString()), idx);
                ++idx;
            }
        }
    }

    // read in any settings
    const auto sourceNode = doc->xPath("/settings");
    if (sourceNode)
    {
        if (const auto node = sourceNode->find("event_ttl"); node)
            eventTtl = node->getInt();

        if (const auto node = sourceNode->find("event_max"); node)
            eventMax = node->getInt();

        if (const auto node = sourceNode->find("session_time"); node)
            sessionTime = node->getInt();

        if (const auto node = sourceNode->find("tz_offset"); node)
            tzOffset = node->getInt();
    }


    // set the default required properties
    properties.setProperty(PROP_STAMP, "stamp", PropertyTypes_e::intProp, false);
    properties.setProperty(PROP_EVENT, "event", PropertyTypes_e::textProp, false);
    properties.setProperty(PROP_UUID, "id", PropertyTypes_e::intProp, false);
    properties.setProperty(PROP_SEGMENT, "__segment", PropertyTypes_e::textProp, false);
    properties.setProperty(PROP_SESSION, "session", PropertyTypes_e::intProp, false);

    // load the properties
    const auto columnNode = doc->xPath("/properties");

    if (columnNode)
    {
        auto columns = columnNode->getNodes();
        for (auto n : columns)
            addToSchema(n);
    }
}

void Table::deserializeSettings(const cjson* doc)
{
    if (const auto node = doc->find("event_ttl"); node)
    {
        eventTtl = node->getInt();
        if (eventTtl < 60'000)
            eventTtl = 60'000;
    }

    if (const auto node = doc->find("event_max"); node)
    {
        eventMax = node->getInt();
        if (eventMax < 1)
            eventMax = 1;
    }

    if (const auto node = doc->find("session_time"); node)
    {
        sessionTime = node->getInt();
        if (sessionTime < 1000)
            sessionTime = 1000;
    }

    if (const auto node = doc->find("tz_offset"); node)
    {
        tzOffset = node->getInt();
        if (tzOffset < 0)
            tzOffset = 0;
    }

    if (const auto node = doc->find("maint_interval"); node)
    {
        maintInterval = node->getInt();
        if (maintInterval < 60'000)
            maintInterval = 60'000;
    }

    if (const auto node = doc->find("segment_interval"); node)
    {
        segmentInterval = node->getInt();
        if (segmentInterval < 60'000)
            segmentInterval = 60'000;
    }

    if (const auto node = doc->find("index_compression"); node)
    {
        indexCompression = node->getInt();
        if (indexCompression < 1)
            indexCompression = 1;
        else if (indexCompression > 20)
            indexCompression = 20;
    }

    if (const auto node = doc->find("person_compression"); node)
    {
        personCompression = node->getInt();
        if (personCompression < 1)
            personCompression = 1;
        else if (personCompression > 20)
            personCompression = 20;
    }

}

void Table::clearZombies()
{
    // Note: this private member must be called from within a lock
    if (!zombies.size())
        return;

    // zombie partitions will linger for 30 seconds
    const auto expireStamp = Now() - 30'000;

     while (zombies.size() && zombies.front()->getMarkedForDeletionStamp() < expireStamp)
     {
         const auto part = zombies.front();
         zombies.pop();
         delete part;
     }
}

void Table::deserializeTriggers(const cjson* doc)
{
    /*
    auto triggerList = doc->getNodes();
    for (auto &t : triggerList)
    {
        auto trigInfo = new openset::revent::reventSettings_s;

        trigInfo->name = t->xPathString("/name", "");
        trigInfo->script = t->xPathString("/script", "");
        trigInfo->id = MakeHash(trigInfo->name);
        trigInfo->entryFunction = t->xPathString("/entry", "on_insert");
        trigInfo->entryFunctionHash = MakeHash(trigInfo->entryFunction);
        trigInfo->configVersion = 0;

        auto error = openset::revent::Revent::compileTriggers(
            this,
            trigInfo->script,
            trigInfo->macros);

        triggerConf[trigInfo->name] = trigInfo;

        Logger::get().info("initialized trigger '" + trigInfo->name + "' on table '" + this->name + ".");
    }
    */
}
