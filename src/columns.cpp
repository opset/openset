#include "columns.h"
#include "config.h"
#include "cjson/cjson.h"

using namespace openset::db;

Columns::Columns()
{
	// so when addressed by pointer we get the index
	for (auto i = 0; i < MAXCOLUMNS; i++)
		columns[i].idx = i;
}

Columns::~Columns()
{}


Columns::columns_s* Columns::getColumn(int column)
{
	return &columns[column];
}

Columns::columns_s* Columns::getColumn(string name)
{
	csLock _lck(lock);

	const auto iter = nameMap.find(name);

	if (iter != nameMap.end())
		return iter->second;
	else
		return nullptr;
}

void Columns::deleteColumn(columns_s* columnInfo)
{
	columnInfo->deleted = Now();
	nameMap.erase(columnInfo->name);
	columnInfo->name = "___deleted";
}

int Columns::getColumnCount() const
{
	return columnCount;
}

void Columns::setColumn(int index, string name, columnTypes_e type, bool isSet, bool deleted)
{
	csLock _lck(lock);

	if (columns[index].name.size())
		nameMap.erase(columns[index].name);

	if (nameMap.find(name) != nameMap.end())
	{
		auto oldRecord = nameMap[name];
		oldRecord->name.clear();
		oldRecord->type = columnTypes_e::freeColumn;
	}

	// update the map
	nameMap[name] = &columns[index];

	// update the column
	columns[index].name = name;
	columns[index].type = type;
	columns[index].isProp = isSet;
	columns[index].deleted = deleted;

	columnCount = 0;
	for (auto c : columns)
		if (c.type != columnTypes_e::freeColumn)
			++columnCount;
}
