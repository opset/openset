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

Table::Table(string name, Database* database):
	name(name),
	database(database),
	loadVersion(Now())
{
	memset(partitions, 0, sizeof(partitions));

	// initialize the var object as a dictionary
	globalVars.dict(); 

	// load Config
	// change - we no longer load last commit
	//loadConfig();

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
{}

void Table::createMissingPartitionObjects()
{
	globals::async->assertAsyncLock();
	
	auto myPartitions = globals::mapper->partitionMap.getPartitionsByNodeId(globals::running->nodeId);

	for (auto p: myPartitions)
		getPartitionObjects(p);
}

TablePartitioned* Table::getPartitionObjects(int32_t partition)
{
	csLock lock(cs); // scoped lock		
	if (partitions[partition])
		return partitions[partition];

	partitions[partition] = new TablePartitioned(
		this, 
		partition, 
		&attributeBlob, 
		&columns);

	return partitions[partition];
}

void Table::releasePartitionObjects(int32_t partition)
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
			columnRecord->set("prop", c.isProp);
		}

}

void Table::serializeTriggers(cjson* doc)
{
	// push the trigger names
	doc->setType(cjsonType::OBJECT);

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
	auto addToSchema = [&](cjson* item)
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
	auto pkNode = doc->xPath("/pk");

	// list of keys
	if (pkNode->type() == cjsonType::ARRAY)
	{
		
		auto nodes = pkNode->getNodes();

		auto idx = 0;
		for (auto n : nodes)
		{
			if (n->type() == cjsonType::STR)
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
	auto columnNode = doc->xPath("/columns");

	if (columnNode)
	{
		auto columns = columnNode->getNodes();
		for (auto n : columns)
			addToSchema(n);
	}
}

void Table::deserializeTriggers(cjson* doc)
{
	auto count = 0;

	auto triggerList = doc->getNodes();
	for (auto &t : triggerList)
	{
		auto trigInfo = new openset::trigger::triggerSettings_s;

		trigInfo->name = t->xPathString("/name", "");
		trigInfo->script = t->xPathString("/script", "");
		trigInfo->id = MakeHash(trigInfo->name);
		trigInfo->entryFunction = t->xPathString("/entry", "on_insert");
		trigInfo->entryFunctionHash = MakeHash(trigInfo->entryFunction);
		trigInfo->configVersion = 0;

		auto error = openset::trigger::Trigger::compileTrigger(
			this,
			trigInfo->name,
			trigInfo->script,
			trigInfo->macros);

		triggerConf[trigInfo->name] = trigInfo;

		Logger::get().info("initialized trigger '" + trigInfo->name + "' on table '" + this->name + ".");
	}

	Logger::get().info("added " + to_string(count) + " columns to table '" + name + "'");

}

void Table::loadConfig()
{

	if (globals::running->testMode)
	{
		// set default columns for test mode
		columns.setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false);
		columns.setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false);
		columns.setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false);
		columns.setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
		columns.setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
		columns.setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
		columns.setColumn(COL_SESSION, "__session", columnTypes_e::intColumn, false);
		return;
	}

	// see if it exists
	if (!openset::IO::File::FileExists(globals::running->path + "tables/" + name + "/table.json"))
	{
		saveConfig();
		return;
	}

	// global config lock
	csLock lock(globals::running->cs);

	// ensure a subdirectory in tables exists for this table
	openset::IO::Directory::mkdir(globals::running->path + "tables/" + name);

	// open table.json in the table subdirectory
	cjson tableDoc(globals::running->path + "tables/" + name + "/table.json");
	// deserialize document
	deserializeTable(&tableDoc);

	// open triggers.json in the table subdirectory
	cjson triggerDoc(globals::running->path + "tables/" + name + "/triggers.json");
	// deserialize triggers
	deserializeTriggers(&triggerDoc);
	
}

void Table::saveConfig()
{
	if (globals::running->testMode)
		return;

	csLock lock(globals::running->cs);

	// ensure a subdirectory in tables exists for this table
	openset::IO::Directory::mkdir(globals::running->path + "tables/" + name);

	// open columns.conf in the table subdirectory
	cjson tableDoc;
	serializeTable(&tableDoc);	
	// save it out
	Logger::get().info("config saved for table '" + name + "'");
	cjson::toFile(globals::running->path + "tables/" + name + "/table.json", &tableDoc, true);

	cjson triggerDoc;
	serializeTriggers(&triggerDoc);
	// save it out
	Logger::get().info("triggers saved for table '" + name + "'");
	cjson::toFile(globals::running->path + "tables/" + name + "/triggers.json", &triggerDoc, true);

}
