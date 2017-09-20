#include "database.h"
#include "config.h"
#include "file/file.h"
#include "file/directory.h"

namespace openset
{
	namespace globals
	{
		openset::db::Database* database;
	}
}

using namespace openset::db;

Database::Database()
{
	openset::globals::database = this;
}

Database::~Database()
{}

openset::db::Table* Database::getTable(string TableName)
{
	csLock lock(cs);
	auto iter = tables.find(TableName);
	if (iter == tables.end())
		return nullptr;

	return iter->second;
}

openset::db::Table* Database::newTable(string TableName)
{
	auto table = getTable(TableName);
	if (table)
		return table;

	csLock lock(cs);
	table = new Table(TableName, this);
	tables[TableName] = table;

	// update the config files
	// change - save is performed on commit
	// saveConfig();

	return table;
}

void Database::initializeTables()
{
	// read our config files in
	// change - load is only performed on resume
	//loadConfig();
}

void Database::serialize(cjson* doc)
{

	doc->setType(cjsonType::ARRAY);

	for (auto n : tables)
		doc->push(n.first);
	
}

void Database::loadConfig()
{
	// we are going to make a table dir
	openset::IO::Directory::mkdir(globals::running->path + "tables");

	// see if it exists
	if (!openset::IO::File::FileExists(globals::running->path + "tables.json"))
	{
		saveConfig();
		return;
	}

	cjson tableDoc(globals::running->path + "tables.json");

	if (tableDoc.empty())
	{
		Logger::get().info(' ', "no tables configured.");
		return;
	}

	auto nodes = tableDoc.getNodes();
	auto count = 0;

	for (auto n : nodes) // array of simple table names, nothing fancy
	{
		auto name = n->getString(); 

		if (!name.length())
			continue;

		++count;
		newTable(name);
	}

	Logger::get().info('+', "loaded " + to_string(count) + " tables.");

}

void Database::saveConfig()
{
	// global config lock
	csLock lock(globals::running->cs);

	cjson tableDoc;

	serialize(&tableDoc);

	cjson::toFile(globals::running->path + "tables.json", &tableDoc, true);
}
