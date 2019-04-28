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

Table::Table(const string &name, Database* database):
	name(name),
	database(database),
	loadVersion(Now()),
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

	// set the default required columns
	columns.setColumn(COL_STAMP, "stamp", columnTypes_e::intColumn, false);
	columns.setColumn(COL_EVENT, "event", columnTypes_e::textColumn, false);
	columns.setColumn(COL_UUID, "id", columnTypes_e::intColumn, false);
	columns.setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
	columns.setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SESSION, "session", columnTypes_e::intColumn, false);

	createMissingPartitionObjects();
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
		&columns);

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

    segmentRefresh.emplace(segmentName, SegmentRefresh_s { segmentName, macros, refreshTime, zIndex, onInsert });
}

void Table::serializeTable(cjson* doc)
{
	auto pkNode = doc->setArray("z_order");

	std::vector<std::string> zList(zOrderStrings.size());

	// convert to list by stuffing into list at index in .second
	for (auto &i : zOrderStrings)
		zList[i.second] = i.first;

	// push the strings into the list in order
	for (auto &i : zList)
		pkNode->push(i);

    auto settings = doc->setObject("settings");
    settings->set("event_ttl", eventTtl);
    settings->set("event_max", eventMax);
    settings->set("session_time", sessionTime);
    settings->set("tz_offset", tzOffset);
    
	auto columnNodes = doc->setArray("columns");

	for (auto &c : columns.columns)
		if (c.deleted == 0 && c.name.size() && c.type != columnTypes_e::freeColumn)
		{
			auto columnRecord = columnNodes->pushObject();

			std::string type;

			switch (c.type)
			{
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
				// TODO - this should never happen
				break;
			}

			columnRecord->set("name", c.name);
			columnRecord->set("index", cast<int64_t>(c.idx));
			columnRecord->set("type", type);
			columnRecord->set("deleted", c.deleted);
			columnRecord->set("is_set", c.isSet);
            columnRecord->set("is_prop", c.isProp);
		}
}

void Table::serializeSettings(cjson* doc) const
{
    doc->set("event_ttl", eventTtl);
    doc->set("event_max", eventMax);
    doc->set("session_time", sessionTime);
    doc->set("tz_offset", tzOffset);
    doc->set("maint_interval", maintInterval);
    doc->set("revent_interval", reventInterval);
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

	// load the columns
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

		columnTypes_e colType;

		if (type == "text")
			colType = columnTypes_e::textColumn;
		else if (type == "int")
			colType = columnTypes_e::intColumn;
		else if (type == "double")
			colType = columnTypes_e::doubleColumn;
		else if (type == "bool")
			colType = columnTypes_e::boolColumn;
		else
			return; // skip 

		columns.setColumn(index, colName, colType, isSet, isProp, deleted);
		count++;
	};

	// load the PK
	zOrderStrings.clear();
	zOrderInts.clear();
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
				zOrderStrings.emplace(n->getString(), idx);
				zOrderInts.emplace(MakeHash(n->getString()), idx);
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


	// set the default required columns
	columns.setColumn(COL_STAMP, "stamp", columnTypes_e::intColumn, false);
	columns.setColumn(COL_EVENT, "event", columnTypes_e::textColumn, false);
	columns.setColumn(COL_UUID, "id", columnTypes_e::intColumn, false);
	columns.setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
	columns.setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SESSION, "session", columnTypes_e::intColumn, false);

	// load the columns
	const auto columnNode = doc->xPath("/columns");

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

    if (const auto node = doc->find("revent_interval"); node)
    {
        reventInterval = node->getInt();
        if (maintInterval < 1'000)
            maintInterval = 1'000;
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
