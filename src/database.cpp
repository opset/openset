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

Database::TablePtr Database::getTable(const string& tableName)
{
	csLock lock(cs);	
	if (const auto iter = tables.find(tableName); iter == tables.end())
		return nullptr;
    else
    	return iter->second;
}

Database::TablePtr Database::newTable(const string& tableName)
{
	auto table = getTable(tableName);
	if (table)
		return table;

    table = make_shared<Table>(tableName, this);

    {
	    csLock lock(cs);	
	    tables[tableName] = table;
    }

    // call this outside the lock, or we will have
    // a nested lock deadlock.
    table->initialize();

	return table;
}

void Database::dropTable(const std::string& tableName)
{
	const auto table = getTable(tableName);
	if (!table)
		return;

    table->deleted = true;

    openset::globals::async->suspendAsync();
    openset::globals::async->purgeByTable(tableName);
	csLock lock(cs);
    tables.erase(tableName);  
    openset::globals::async->resumeAsync();
}

void Database::serialize(cjson* doc)
{
	doc->setType(cjson::Types_e::ARRAY);

	for (const auto& n : tables)
		doc->push(n.first);	
}

