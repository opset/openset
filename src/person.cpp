#include "person.h"
#include "table.h"
#include "logger.h"
#include "tablepartitioned.h"

using namespace openset::db;

Person::Person():
	table(nullptr),
	attributes(nullptr),
	blob(nullptr),
	people(nullptr),
	partition(0)
{}

Person::~Person()
{}

void Person::reinit()
{
	table = nullptr;
	attributes = nullptr;
	blob = nullptr;
	people = nullptr;
	partition = 0;
	grid.reinit();
}

bool Person::mapTable(Table* tablePtr, int Partition)
{
	if (table && tablePtr && table == tablePtr)
		return false;

	table = tablePtr;
	partition = Partition;

	// attributes are partitioned, like users so that their bit indexes
	// remain consistent if partitions migrate. 
	// this will acquire the correct attribute data for this Person
	const auto parts = table->getPartitionObjects(partition, false);

    if (!parts)
        return false;

	attributes = &parts->attributes;
	people = &parts->people;
	blob = attributes->getBlob();
	
	mapSchemaAll();

    return true;
}

bool Person::mapTable(Table* tablePtr, int Partition, vector<string> &columnNames)
{
	if (table && tablePtr && table == tablePtr)
		return false;

	table = tablePtr;
	partition = Partition;

	// attributes are partitioned, like users so that their bit indexes
	// remain consistent if partitions migrate. 
	// this will acquire the correct attribute data for this Person
	auto parts = table->getPartitionObjects(partition, false);

    if (!parts)
        return false;

	attributes = &parts->attributes;
	people = &parts->people;
	blob = attributes->getBlob();

	mapSchemaList(columnNames);

    return true;	
}

bool Person::mapSchemaAll()
{
	return grid.mapSchema(table, attributes);
}

bool Person::mapSchemaList(const vector<string>& columnNames)
{
	return grid.mapSchema(table, attributes, columnNames);
}

void Person::mount(PersonData_s* personData)
{
#if defined(_DEBUG) || defined(NDEBUG)
	Logger::get().fatal((table), "mapTable must be called before mount");
#endif
	grid.mount(personData);
}

void Person::prepare()
{
	grid.prepare();
}

void Person::insert(cjson* rowData)
{
	grid.insert(rowData);
}

PersonData_s* Person::commit()
{
	auto data = grid.commit();
	people->replacePersonRecord(data);
	return data;    
}
