#include "columnmapping.h"
#include "table.h"
#include "attributes.h"
#include "threads/locks.h"

openset::db::ColumnMap_s* openset::db::ColumnMapping::mapSchema(
    Table* table,
    Attributes* attributes,
    const vector<string>& columnNames)
{
    csLock lock(cs);

    const auto schema = table->getColumns();
    int64_t hash = 0;

    // make a hash value for the requested columns
    for (const auto& colName : columnNames)
    {
        const auto s = schema->getColumn(colName);
        if (!s)
            return nullptr;
        hash = AppendHash(s->idx, hash);
    }

    const auto iter = map.find(hash);

    if (iter != map.end())
    {
        ++iter->second->refCount;
        return iter->second;
    }

    auto cm = new ColumnMap_s;

    cm->hash = hash;

    // negative one fill these as -1 means no mapping
    for (auto& i : cm->columnMap)
        i = -1;

    for (auto& i : cm->reverseMap)
        i = -1;

    cm->columnCount = 0;

    cm->insertMap.clear();

    for (const auto& colName : columnNames)
    {
        const auto s = schema->getColumn(colName);

        if (!s)
        {
            delete cm;
            return nullptr;;
        }

        if (s->idx == COL_UUID)
            cm->uuidColumn = cm->columnCount;
        else if (s->idx == COL_SESSION)
            cm->sessionColumn = cm->columnCount;

        cm->columnMap[cm->columnCount] = static_cast<int32_t>(s->idx); // maps local column to schema
        cm->reverseMap[s->idx] = static_cast<int32_t>(cm->columnCount); // maps schema to local column
        cm->insertMap[MakeHash(s->name)] = cm->columnCount; // maps to local column
        //isSet[columnCount] = true;

        ++cm->columnCount;
    }

    cm->refCount = 1;
    cm->rowBytes = cm->columnCount * 8LL;

    map[hash] = cm;
    return cm;
}

openset::db::ColumnMap_s* openset::db::ColumnMapping::mapSchema(Table* table, Attributes* attributes)
{
    if (allMapping) // TODO check for schema version
        return allMapping;

    csLock lock(cs);

    auto cm = new ColumnMap_s;

    // negative one fill these as -1 means no mapping
    for (auto& i : cm->columnMap)
        i = -1;

    for (auto& i : cm->reverseMap)
        i = -1;

    cm->columnCount = 0;

    cm->insertMap.clear();

    for (auto& s : table->getColumns()->columns)
        if (s.type != columnTypes_e::freeColumn)
        {
            if (s.idx == COL_UUID)
                cm->uuidColumn = cm->columnCount;
            else if (s.idx == COL_SESSION)
                cm->sessionColumn = cm->columnCount;

            cm->columnMap[cm->columnCount] = static_cast<int32_t>(s.idx); // maps local column to schema
            cm->reverseMap[s.idx] = static_cast<int32_t>(cm->columnCount); // maps schema to local column
            cm->insertMap[MakeHash(s.name)] = cm->columnCount; // maps to local column
            //isSet[columnCount] = true;

            ++cm->columnCount;
        }

    cm->rowBytes = cm->columnCount * 8LL;

    allMapping = cm;
    return cm;
}

void openset::db::ColumnMapping::releaseMap(ColumnMap_s* cm)
{
    if (cm == allMapping)
        return;

    csLock lock(cs);

    --cm->refCount;

    if (cm->refCount == 0)
    {
        map.erase(cm->hash);
        delete cm;
    }
}
