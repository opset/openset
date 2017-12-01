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

using namespace std;
using namespace openset::db;

Database::Database()
{
	openset::globals::database = this;
}

Database::~Database()
{}

openset::db::Table* Database::getTable(const string& tableName)
{
	csLock lock(cs);
	auto iter = tables.find(tableName);
	if (iter == tables.end())
		return nullptr;

	return iter->second;
}

openset::db::Table* Database::newTable(const string& tableName)
{
	auto table = getTable(tableName);
	if (table)
		return table;

	csLock lock(cs);
	table = new Table(tableName, this);
	tables[tableName] = table;

	// update the config files
	// change - save is performed on commit
	// saveConfig();

	return table;
}

void Database::serialize(cjson* doc)
{

	doc->setType(cjsonType::ARRAY);

	for (const auto n : tables)
		doc->push(n.first);
	
}

