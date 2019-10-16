#include "property_mapping.h"
#include "table.h"
#include "attributes.h"
#include "threads/locks.h"

openset::db::PropertyMap_s* openset::db::PropertyMapping::mapSchema(
    Table* table,
    Attributes* attributes,
    const vector<string>& propertyNames)
{
    csLock lock(cs);

    const auto schema = table->getProperties();
    int64_t hash = 0;

    // make a hash value for the requested properties
    for (const auto& colName : propertyNames)
    {
        const auto s = schema->getProperty(colName);
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

    auto cm = new PropertyMap_s;

    cm->hash = hash;

    // negative one fill these as -1 means no mapping
    for (auto& i : cm->propertyMap)
        i = -1;

    for (auto& i : cm->reverseMap)
        i = -1;

    cm->propertyCount = 0;

    cm->insertMap.clear();

    for (const auto& colName : propertyNames)
    {
        const auto s = schema->getProperty(colName);

        if (!s)
        {
            delete cm;
            return nullptr;;
        }

        if (s->idx == PROP_UUID)
            cm->uuidPropIndex = cm->propertyCount;
        else if (s->idx == PROP_SESSION)
            cm->sessionPropIndex = cm->propertyCount;

        cm->propertyMap[cm->propertyCount] = static_cast<int32_t>(s->idx); // maps local property to schema
        cm->reverseMap[s->idx] = static_cast<int32_t>(cm->propertyCount); // maps schema to local property
        cm->insertMap[MakeHash(s->name)] = cm->propertyCount; // maps to local property
        //isSet[propertyCount] = true;

        ++cm->propertyCount;
    }

    cm->refCount = 1;
    cm->rowBytes = cm->propertyCount * 8LL;

    map[hash] = cm;
    return cm;
}

openset::db::PropertyMap_s* openset::db::PropertyMapping::mapSchema(Table* table, Attributes* attributes)
{
    if (allMapping) // TODO check for schema version
        return allMapping;

    csLock lock(cs);

    auto cm = new PropertyMap_s;

    // negative one fill these as -1 means no mapping
    for (auto& i : cm->propertyMap)
        i = -1;

    for (auto& i : cm->reverseMap)
        i = -1;

    cm->propertyCount = 0;

    cm->insertMap.clear();

    for (auto& s : table->getProperties()->properties)
        if (s.type != PropertyTypes_e::freeProp)
        {
            if (s.idx == PROP_UUID)
                cm->uuidPropIndex = cm->propertyCount;
            else if (s.idx == PROP_SESSION)
                cm->sessionPropIndex = cm->propertyCount;

            cm->propertyMap[cm->propertyCount] = static_cast<int32_t>(s.idx); // maps local property to schema
            cm->reverseMap[s.idx] = static_cast<int32_t>(cm->propertyCount); // maps schema to local property
            cm->insertMap[MakeHash(s.name)] = cm->propertyCount; // maps to local property
            //isSet[propertyCount] = true;

            ++cm->propertyCount;
        }

    cm->rowBytes = cm->propertyCount * 8LL;

    allMapping = cm;
    return cm;
}

void openset::db::PropertyMapping::releaseMap(PropertyMap_s* cm)
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
