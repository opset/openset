#include "attributes.h"
#include "sba/sba.h"
#include "table.h"
#include "properties.h"
#include "attributeblob.h"

using namespace openset::db;

Attributes::Attributes(const int partition, Table* table, AttributeBlob* attributeBlob, Properties* properties) :
    table(table),
    blob(attributeBlob),
    properties(properties),
    partition(partition),
    indexCache(128)
{}

Attributes::~Attributes()
{
    for (auto &attr: propertyIndex)
    {
        PoolMem::getPool().freePtr(attr.second);
        attr.second = nullptr;
    }
}

IndexBits* Attributes::getBits(const int32_t propIndex, int64_t value)
{
    // apply bucketing to double values
    if (const auto propInfo = properties->getProperty(propIndex); propInfo && propInfo->type == PropertyTypes_e::doubleProp)
        value = static_cast<int64_t>(value / propInfo->bucket) * propInfo->bucket;

    if (const auto bits = indexCache.get(propIndex, value); bits)
        return bits;

    const auto attribute = Attributes::getMake(propIndex, value);

    auto bits = new IndexBits();
    bits->mount(attribute->data);

    // cache these bits
    const auto [evictPropIndex, evictValue, evictBits] = indexCache.set(propIndex, value, bits);

    // if anything got squeezed out compress it
    if (evictBits)
    {
        if (evictBits->data.isDirty())
        {
            const auto evictAttribute = Attributes::getMake(static_cast<int>(evictPropIndex), evictValue);
            evictAttribute->data = evictBits->store();
        }
        delete evictBits;
    }

    return bits;
}

void Attributes::addChange(const int64_t customerId, const int32_t propIndex, const int64_t value, const int32_t linearId, const bool state)
{
    if (propIndex == PROP_STAMP || propIndex == PROP_UUID || propIndex == PROP_SESSION)
        return;

    const auto key = attr_key_s( propIndex, value );

    if (state)
        customerIndexing.insert(propIndex, customerId, linearId, value);
    else
        customerIndexing.erase(propIndex, customerId, value);

    if (auto changeRecord = changeIndex.find(key); changeRecord != changeIndex.end())
    {
        changeRecord->second.emplace_back(Attr_changes_s{linearId, state});
        return;
    }

    changeIndex.emplace(key, std::vector<Attr_changes_s>{Attr_changes_s{linearId, state}});
}

Attr_s* Attributes::getMake(const int32_t propIndex, int64_t value)
{
    if (const auto propInfo = properties->getProperty(propIndex); propInfo && propInfo->type == PropertyTypes_e::doubleProp)
        value = static_cast<int64_t>(value / propInfo->bucket) * propInfo->bucket;

    auto key = attr_key_s( propIndex, value );

    if (const auto& res = propertyIndex.emplace(key, nullptr); res.second == true)
    {
        const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
        attr->data = nullptr;
        attr->text = nullptr;
        res.first->second = attr;
        return attr;
    }
    else
    {
        return res.first->second;
    }
}

Attr_s* Attributes::getMake(const int32_t propIndex, const string& value)
{
    auto key = attr_key_s( propIndex,  MakeHash(value) );

    if (const auto& res = propertyIndex.emplace(key, nullptr); res.second == true)
    {
        const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
        attr->data = nullptr;
        attr->text = blob->storeValue(propIndex, value);
        res.first->second = attr;
        return attr;
    }
    else
    {
        return res.first->second;
    }
}

Attr_s* Attributes::get(const int32_t propIndex, const int64_t value) const
{
    if (const auto attrPair = propertyIndex.find({ propIndex, value }); attrPair != propertyIndex.end())
        return attrPair->second;

    return nullptr;
}

Attr_s* Attributes::get(const int32_t propIndex, const string& value) const
{
    if (const auto attrPair = propertyIndex.find({ propIndex, MakeHash(value) }); attrPair != propertyIndex.end())
        return attrPair->second;

    return nullptr;
}

void Attributes::drop(const int32_t propIndex, const int64_t value)
{
    propertyIndex.erase({ propIndex, value });
}

void Attributes::setDirty(const int64_t customerId, const int32_t linId, const int32_t propIndex, const int64_t value, const bool on)
{
    addChange(customerId, propIndex, value, linId, on);
}

void Attributes::clearDirty()
{
    for (auto& change : changeIndex)
    {
        const auto bits = getBits(change.first.index, change.first.value);

        for (const auto t : change.second)
        {
            if (t.state)
                bits->bitSet(t.linId);
            else
                bits->bitClear(t.linId);
        }
    }

    changeIndex.clear();
}

AttributeBlob* Attributes::getBlob() const
{
    return blob;
}

Attributes::AttrListExpanded Attributes::getPropertyValues(const int32_t propIndex)
{
    Attributes::AttrListExpanded result;

    for (auto &kv : propertyIndex)
        if (kv.first.index == propIndex && kv.first.value != NONE)
            result.push_back({ kv.first.value, kv.second });

    return result;
}

Attributes::AttrList Attributes::getPropertyValues(const int32_t propIndex, const listMode_e mode, const int64_t value)
{
    Attributes::AttrList result;

    switch (mode)
    {
        // so.. NEQ is handled outside of this function
        // in query indexing
    case listMode_e::NEQ:
    case listMode_e::EQ:
        if (const auto tAttr = get(propIndex, value); tAttr)
            result.emplace_back(propIndex, value);
        return result;
        default: ;
    }

    for (auto &kv : propertyIndex)
    {
        if (kv.first.index != propIndex)
            continue;

        switch (mode)
        {
        case listMode_e::PRESENT: // sum of all indexes - slow but accurate for `== nil` test
            result.push_back(kv.first);
        break;
        case listMode_e::GT:
            if (kv.first.value > value)
                result.push_back(kv.first);
            break;
        case listMode_e::GTE:
            if (kv.first.value >= value)
                result.push_back(kv.first);
            break;
        case listMode_e::LT:
            if (kv.first.value < value)
                result.push_back(kv.first);
            break;
        case listMode_e::LTE:
            if (kv.first.value <= value)
                result.push_back(kv.first);
            break;
        default:
            // never happens
            break;
        }
    }

    return result;
}

void Attributes::createCustomerPropIndexes()
{
    const auto props = table->getCustomerIndexProps();
    for (auto prop : *props)
        customerIndexing.createIndex(prop);
}

void Attributes::serialize(HeapStack* mem)
{
    // grab 8 bytes, and set the block type at that address
    *recast<serializedBlockType_e*>(mem->newPtr(sizeof(int64_t))) = serializedBlockType_e::attributes;

    // grab 8 more bytes, this will be the length of the attributes data within the block
    const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
    (*sectionLength) = 0;

    //for (auto& kv : propertyIndex)
    //{
        // add a header to the HeapStack
        //const auto blockHeader = recast<serializedAttr_s*>(mem->newPtr(sizeof(serializedAttr_s)));
    //}

}

int64_t Attributes::deserialize(char* mem)
{
    auto read = mem;

    if (*recast<serializedBlockType_e*>(read) != serializedBlockType_e::attributes)
        return 0;

    read += sizeof(int64_t);

    const auto blockSize = *recast<int64_t*>(read);

    if (blockSize == 0)
    {
        Logger::get().info("no attributes to deserialize for partition " + to_string(partition));
        return 16;
    }

    read += sizeof(int64_t);

    // end is the length of the block after the 16 bytes of header
    const auto end = read + blockSize;

    while (read < end)
    {
        // pointer to block
        const auto blockHeader = recast<serializedAttr_s*>(read);
        const auto blockLength = sizeof(serializedAttr_s) + blockHeader->textSize + blockHeader->compSize;

        const auto textPtr = read + sizeof(serializedAttr_s);
        const auto dataPtr = textPtr + blockHeader->textSize;

        char* blobPtr = nullptr;

        // is there text? Lets add this to the blob and use this pointer after to set the
        // text member of attr
        if (blockHeader->textSize)
            blobPtr = blob->storeValue(blockHeader->column, std::string{ textPtr, static_cast<size_t>(blockHeader->textSize) });

        // create an attr_s object
        const auto attr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + blockHeader->compSize));
        attr->text = blobPtr;

        // TODO - copy the data

        // add it to the index
        propertyIndex.emplace(attr_key_s{ blockHeader->column, blockHeader->hashValue }, attr);

        // next block please
        read += blockLength;
    }

    return blockSize + 16;
}