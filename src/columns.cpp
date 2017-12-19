#include <regex>

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


Columns::Columns_s* Columns::getColumn(int column)
{
	return &columns[column];
}

Columns::Columns_s* Columns::getColumn(string name)
{
	csLock _lck(lock);

	const auto iter = nameMap.find(name);

	if (iter != nameMap.end())
		return iter->second;
	else
		return nullptr;
}

void Columns::deleteColumn(Columns_s* columnInfo)
{
	columnInfo->deleted = Now();
	nameMap.erase(columnInfo->name);
	columnInfo->name = "___deleted";
}

int Columns::getColumnCount() const
{
	return columnCount;
}

void Columns::setColumn(
    const int index, 
    const string name, 
    const columnTypes_e type, 
    const bool isSet, 
    const bool deleted)
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
	columns[index].isSet = isSet;
	columns[index].deleted = deleted;

	columnCount = 0;
	for (const auto c : columns)
		if (c.type != columnTypes_e::freeColumn)
			++columnCount;
}

bool Columns::validColumnName(const std::string name)
{
    static std::regex nameCapture(R"(^([^ 0-9][a-z0-9_]+)$)");

    std::smatch matches;
    regex_match(name, matches, nameCapture);

    if (!matches.size() == 2)
        return false;

    return matches[1] == name;
}
