#include "config.h"
#include "table.h"
#include "trigger.h"
#include "errors.h"
#include "tablepartitioned.h"
#include "file/file.h"
#include "file/directory.h"
#include "database.h"
#include "asyncpool.h"
#include "internoderouter.h"

using namespace openset::db;

Table::Table(const string name, Database* database):
	name(name),
	database(database),
	loadVersion(Now())
{
	// initialize the var object as a dictionary
	globalVars.dict(); 

	// set the default required columns
	columns.setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false);
	columns.setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false);
	columns.setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false);
	columns.setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
	columns.setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SESSION, "__session", columnTypes_e::intColumn, false);

	createMissingPartitionObjects();
}

Table::~Table()
{
    for (auto &part : partitions)
    {
        delete part.second;
        part.second = nullptr;
    }
}

void Table::createMissingPartitionObjects()
{
	globals::async->assertAsyncLock();
	
	auto myPartitions = globals::mapper->partitionMap.getPartitionsByNodeId(globals::running->nodeId);

	for (auto p: myPartitions)
		getPartitionObjects(p);
}

TablePartitioned* Table::getPartitionObjects(const int32_t partition)
{
	csLock lock(cs); // scoped lock		
	if (auto const part = partitions.find(partition); part != partitions.end())
		return part->second;

    const auto part = new TablePartitioned(
		this, 
		partition, 
		&attributeBlob, 
		&columns);

	partitions[partition] = part;
	return part;
}

void Table::releasePartitionObjects(const int32_t partition)
{
	csLock lock(cs); // lock for read		

	if (!partitions[partition])
		return;

	// delete the table objects for this partition
	delete partitions[partition];
	partitions[partition] = nullptr;

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
			columnRecord->set("prop", c.isSet);
		}
}

void Table::serializeTriggers(cjson* doc)
{
	// push the trigger names
	doc->setType(cjson::Types_e::OBJECT);

	for (auto &t : triggerConf)
	{
		auto trigNode = doc->setObject(t.second->name);

		trigNode->set("name", t.second->name);
		trigNode->set("entry", t.second->entryFunction);
		trigNode->set("script", t.second->script);
	}	
}

void Table::deserializeTable(cjson* doc)
{
	auto count = 0;

	// load the columns
	const auto addToSchema = [&](cjson* item)
	{
		auto colName = item->xPathString("/name", "");
		auto type = item->xPathString("/type", "");
		auto index = item->xPathInt("/index", -1);
		auto isProp = item->xPathBool("/prop", false);
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
			return; // TODO hmmm...

		columns.setColumn(index, colName, colType, isProp, deleted);
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

	// set the default required columns
	columns.setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false);
	columns.setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false);
	columns.setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false);
	columns.setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
	columns.setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
	columns.setColumn(COL_SESSION, "__session", columnTypes_e::intColumn, false);

	// load the columns
	const auto columnNode = doc->xPath("/columns");

	if (columnNode)
	{
		auto columns = columnNode->getNodes();
		for (auto n : columns)
			addToSchema(n);
	}
}

void Table::deserializeTriggers(cjson* doc)
{
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
}

