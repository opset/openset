#include "customer.h"
#include "table.h"
#include "logger.h"
#include "tablepartitioned.h"

using namespace openset::db;

Customer::Customer():
    table(nullptr),
    attributes(nullptr),
    blob(nullptr),
    people(nullptr),
    partition(0)
{}

void Customer::reinit()
{
    table = nullptr;
    attributes = nullptr;
    blob = nullptr;
    people = nullptr;
    partition = 0;
    grid.reinit();
}

bool Customer::mapTable(Table* tablePtr, int Partition)
{
    if (table && tablePtr && table == tablePtr)
        return false;

    table = tablePtr;
    partition = Partition;

    // attributes are partitioned, like users so that their bit indexes
    // remain consistent if partitions migrate.
    // this will acquire the correct attribute data for this Customer
    const auto parts = table->getPartitionObjects(partition, false);

    if (!parts)
        return false;

    attributes = &parts->attributes;
    people = &parts->people;
    blob = attributes->getBlob();

    mapSchemaAll();

    return true;
}

bool Customer::mapTable(Table* tablePtr, int Partition, vector<string> &columnNames)
{
    if (table && tablePtr && table == tablePtr)
        return false;

    table = tablePtr;
    partition = Partition;

    // attributes are partitioned, like users so that their bit indexes
    // remain consistent if partitions migrate.
    // this will acquire the correct attribute data for this Customer
    auto parts = table->getPartitionObjects(partition, false);

    if (!parts)
        return false;

    attributes = &parts->attributes;
    people = &parts->people;
    blob = attributes->getBlob();

    mapSchemaList(columnNames);

    return true;
}

bool Customer::mapSchemaAll()
{
    return grid.mapSchema(table, attributes);
}

bool Customer::mapSchemaList(const vector<string>& columnNames)
{
    return grid.mapSchema(table, attributes, columnNames);
}

void Customer::mount(PersonData_s* personData)
{
#if defined(_DEBUG) || defined(NDEBUG)
    Logger::get().fatal((table), "mapTable must be called before mount");
#endif
    grid.mount(personData);
}

void Customer::prepare()
{
    grid.prepare();
}

void Customer::insert(cjson* rowData)
{
    grid.insertEvent(rowData);
}

PersonData_s* Customer::commit()
{
    const auto data = grid.commit();
    people->replaceCustomerRecord(data);
    return data;
}
