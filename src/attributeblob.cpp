#include "attributeblob.h"
#include "sba/sba.h"

bool openset::db::AttributeBlob::isAttribute(const int32_t propIndex, const int64_t valueHash)
{
    csLock lock(cs);
    return attributesBlob.count(attr_key_s::makeKey(propIndex, valueHash));
}

bool openset::db::AttributeBlob::isAttribute(const int32_t propIndex, const string& value)
{
    // TODO iterate for collisions
    const auto valueHash = MakeHash(value);

    csLock lock(cs);
    return attributesBlob.count(attr_key_s::makeKey(propIndex, valueHash));
}

char* openset::db::AttributeBlob::storeValue(const int32_t propIndex, const string& value)
{
    const auto valueHash = MakeHash(value);
    char* blob = nullptr;

    const auto key = attr_key_s::makeKey(propIndex, valueHash);

    csLock lock(cs);

    if (auto attr = attributesBlob.find(key); attr != attributesBlob.end())
        return attr->second;

    const auto len = value.length();
    blob = mem.newPtr(len + 1);//cast<char*>(PoolMem::getPool().getPtr(len + 1));
    strcpy(blob, value.c_str());
    attributesBlob.insert({key, blob});

    return blob;
}

char* openset::db::AttributeBlob::getValue(const int32_t propIndex, const int64_t valueHash)
{
    char* blob = nullptr;
    const auto key = attr_key_s::makeKey(propIndex, valueHash);

    csLock lock(cs);
    if (auto attr = attributesBlob.find(key); attr != attributesBlob.end())
        return attr->second;
    else
        return nullptr;
}
