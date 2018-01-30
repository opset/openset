#include "database.h"
#include "config.h"
#include "asyncpool.h"

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

openset::db::Table* Database::getTable(const string& tableName)
{
	csLock lock(cs);	
	if (const auto iter = tables.find(tableName); iter == tables.end())
		return nullptr;
    else
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

	return table;
}

void Database::dropTable(const std::string& tableName)
{
	const auto table = getTable(tableName);
	if (!table)
		return;

    openset::globals::async->suspendAsync();
    openset::globals::async->purgeByTable(tableName);
	csLock lock(cs);
    tables.erase(tableName);  
    delete table;
    openset::globals::async->resumeAsync();
}

void Database::serialize(cjson* doc)
{
	doc->setType(cjsonType::ARRAY);

	for (const auto& n : tables)
		doc->push(n.first);	
}

