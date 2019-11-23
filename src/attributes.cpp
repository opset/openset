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
    indexCache(50)
{}

Attributes::~Attributes()
{
    for (auto &attr: propertyIndex)
    {
        PoolMem::getPool().freePtr(attr.second);
        attr.second = nullptr;
    }
}

IndexBits* Attributes::getBits(const int32_t propIndex, const int64_t value)
{

    if (const auto bits = indexCache.get(propIndex, value); bits)
        return bits;

    const auto attribute = Attributes::getMake(propIndex, value);

    auto bits = new IndexBits();
    bits->mount(attribute->index, attribute->ints, attribute->ofs, attribute->len, attribute->linId);

    // cache these bits
    const auto [evictPropIndex, evictValue, evictBits] = indexCache.set(propIndex, value, bits);

    // if anything got squeezed out compress it
    if (evictBits)
    {
        const auto attrPair = propertyIndex.find({ evictPropIndex, evictValue });
        const auto evictAttribute = attrPair->second;

        int64_t compBytes = 0; // OUT value via reference
        int64_t linId;
        int32_t ofs, len;

        // compress the data, get it back in a pool ptr
        const auto compData = bits->store(compBytes, linId, ofs, len, table->indexCompression);
        const auto destAttr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + compBytes));

        // copy header
        memcpy(destAttr, evictAttribute, sizeof(Attr_s));
        if (compData)
        {
            memcpy(destAttr->index, compData, compBytes);
            // return work buffer from bits.store to the pool
            PoolMem::getPool().freePtr(compData);
        }

        destAttr->ints = bits->ints;//(isList) ? 0 : bits.ints;
        destAttr->comp = static_cast<int>(compBytes);
        destAttr->linId = linId;
        destAttr->ofs = ofs;
        destAttr->len = len;

        attrPair->second = destAttr;
        PoolMem::getPool().freePtr(evictAttribute);

        delete evictBits;
    }

    return bits;
}

void Attributes::addChange(const int64_t customerId, const int32_t propIndex, const int64_t value, const int32_t linearId, const bool state)
{
    if (propIndex == PROP_STAMP || propIndex == PROP_UUID || propIndex == PROP_SESSION)
        return;

    const auto key = attr_key_s{ propIndex, value };

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

Attr_s* Attributes::getMake(const int32_t propIndex, const int64_t value)
{

    if (auto& res = propertyIndex.emplace(attr_key_s{ propIndex, value }, nullptr); res.second == true)
    {
        const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
        res.first->second = attr;
        return attr;
    }
    else
    {
        return res.first->second;
    }
    /*
    if (auto attrPair = propertyIndex.find({ propIndex, value }); attrPair == propertyIndex.end())
    {
        const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
        propertyIndex.emplace(attr_key_s{ propIndex, value }, attr);
        return attr;
    }
    else
    {
        return attrPair->second;
    }
    */
}

Attr_s* Attributes::getMake(const int32_t propIndex, const string& value)
{
    const auto valueHash = MakeHash(value);

    if (auto& res = propertyIndex.emplace(attr_key_s{ propIndex, valueHash }, nullptr); res.second == true)
    {
        const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
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
    //IndexBits bits;

    for (auto& change : changeIndex)
    {
        getMake(change.first.index, change.first.value);

        //if (attrPair == propertyIndex.end() || !attrPair->second)
          //  continue;

        auto bits = getBits(change.first.index, change.first.value);

        for (const auto& t : change.second)
        {
            if (t.state)
                bits->bitSet(t.linId);
            else
                bits->bitClear(t.linId);
        }

        // TODO - check for non-existent prop.


        /*
        const auto attr = attrPair->second;

        bits.mount(attr->index, attr->ints, attr->ofs, attr->len, attr->linId);

        for (const auto& t : change.second)
        {
            if (t.state)
                bits.bitSet(t.linId);
            else
                bits.bitClear(t.linId);
        }

        if (!bits.population(bits.ints * 64)) //pop count zero? remove this
        {
            drop(change.first.index, change.first.value );
            PoolMem::getPool().freePtr(attr);
        }
        else
        {
            int64_t compBytes = 0; // OUT value via reference
            int64_t linId;
            int32_t ofs, len;

            // compress the data, get it back in a pool ptr
            const auto compData = bits.store(compBytes, linId, ofs, len, table->indexCompression);
            const auto destAttr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + compBytes));

            // copy header
            memcpy(destAttr, attr, sizeof(Attr_s));
            if (compData)
            {
                memcpy(destAttr->index, compData, compBytes);
                // return work buffer from bits.store to the pool
                PoolMem::getPool().freePtr(compData);
            }

            destAttr->ints = bits.ints;//(isList) ? 0 : bits.ints;
            destAttr->comp = static_cast<int>(compBytes);
            destAttr->linId = linId;
            destAttr->ofs = ofs;
            destAttr->len = len;

            // if we made a new destination, we have to update the
            // index to point to it, and free the old one up.
            // update the Attr pointer directly in the index
            attrPair->second = destAttr;
            PoolMem::getPool().freePtr(attr);
        }
        */
    }
    changeIndex.clear();
}

/*
void Attributes::swap(const int32_t propIndex, const int64_t value, IndexBits* newBits)
{
    auto attrPair = propertyIndex.find(attr_key_s{ propIndex, value });

    if (attrPair == propertyIndex.end())
        return;

    const auto attr = attrPair->second;

    int64_t compBytes = 0; // OUT value
    int64_t linId = -1;
    int32_t len, ofs;

    // compress the data, get it back in a pool ptr, size returned in compBytes
    const auto compData = newBits->store(compBytes, linId, ofs, len);
    auto destAttr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + compBytes));

    // copy header
    memcpy(destAttr, attr, sizeof(Attr_s));
    if (compData)
    {
        memcpy(destAttr->index, compData, compBytes);
        // return work buffer from bits.store to the pool
        PoolMem::getPool().freePtr(compData);
    }

    destAttr->text = attr->text;
    destAttr->ints = (compBytes) ? newBits->ints: 0;//asList ? 0 : newBits->ints;
    destAttr->comp = static_cast<int32_t>(compBytes); // TODO - check for overflow
    destAttr->linId = linId;
    destAttr->ofs = ofs;
    destAttr->len = len;

    // if we made a new destination, we have to update the
    // index to point to it, and free the old one up.
    propertyIndex.insert({attr_key_s{ propIndex, value }, destAttr});

    // FIX - memory leak
    PoolMem::getPool().freePtr(attr);
}
*/

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
    //case listMode_e::PRESENT_FAST: // fast for reducing set in `!= nil` test
    //    if (const auto tAttr = get(propIndex, NONE); tAttr)
    //        result.push_back(tAttr);
    //    return result;
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

    for (auto& kv : propertyIndex)
    {
        /* STL ugliness - I wish they let you alias these names somehow
         *
         * kv.first is property and value
         * kv.second is Attr_s*
         *
         * so
         *
         * kv.first.first is property
         * kv.first.second is value
         */

        // add a header to the HeapStack
        const auto blockHeader = recast<serializedAttr_s*>(mem->newPtr(sizeof(serializedAttr_s)));

        // fill in the header
        blockHeader->column = kv.first.index;
        blockHeader->hashValue = kv.first.value;
        blockHeader->ints = kv.second->ints;
        blockHeader->ofs = kv.second->ofs;
        blockHeader->len = kv.second->len;
        blockHeader->linId = kv.second->linId;
        const auto text = this->blob->getValue(kv.first.index, kv.first.value);
        blockHeader->textSize = text ? strlen(text) : 0;
        //blockHeader->textSize = item.second->text ? strlen(item.second->text) : 0;
        blockHeader->compSize = kv.second->comp;

        // copy a text/blob value if any
        if (blockHeader->textSize)
        {
            const auto textData = recast<char*>(mem->newPtr(blockHeader->textSize));
            memcpy(textData, text, blockHeader->textSize);
            //memcpy(textData, item.second->text, blockHeader->textSize);
        }

        // copy the compressed data
        if (blockHeader->compSize)
        {
            const auto blockData = recast<char*>(mem->newPtr(blockHeader->compSize));
            memcpy(blockData, kv.second->index, blockHeader->compSize);
        }

        (*sectionLength) +=
            sizeof(serializedAttr_s) +
            blockHeader->textSize +
            blockHeader->compSize;
    }
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
        attr->ints = blockHeader->ints;
        attr->ofs = blockHeader->ofs;
        attr->len = blockHeader->len;
        attr->comp = blockHeader->compSize;
        attr->linId = blockHeader->linId;

        // copy the data in
        memcpy(attr->index, dataPtr, blockHeader->compSize);

        // add it to the index
        propertyIndex.emplace(attr_key_s{ blockHeader->column, blockHeader->hashValue }, attr);

        // next block please
        read += blockLength;
    }

    return blockSize + 16;
}