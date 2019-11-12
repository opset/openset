#include "grid.h"
#include "table.h"
#include "lz4.h"
#include "time/epoch.h"
#include "sba/sba.h"
#include "var/varblob.h"

using namespace openset::db;

void IndexDiffing::reset()
{
    before.clear();
    after.clear();
}

void IndexDiffing::add(int32_t propIndex, int64_t value, Mode_e mode)
{
    if (mode == Mode_e::before)
    {
        if (const auto iter = before.find({ propIndex, value }); iter != before.end())
            iter->second++;
        else
            before[{ propIndex, value }] = 1;
    }
    else
    {
        if (const auto iter = after.find({ propIndex, value }); iter != after.end())
            iter->second++;
        else
            after[{ propIndex, value }] = 1;
    }

    // a Value of NONE in combination with a property indicates that
    // the property is referenced. This is used to index a property, rather
    // than a property and value.
    if (value != NONE)
        add(propIndex, NONE, mode);
}

void IndexDiffing::add(const Grid* grid, const cvar& props, Mode_e mode)
{
    if (props.typeOf() != cvar::valueType::DICT)
        return;

    const auto properties = grid->getTable()->getProperties();
    const auto attributes = grid->getAttributes();

    for (const auto &key : *props.getDict())
    {
        const auto propInfo =  properties->getProperty(key.first.getString());
        const auto& value = key.second;

        if (!propInfo || !propInfo->isCustomerProperty)
            continue;

        const auto propertyIndex = propInfo->idx;

        auto indexedValue = NONE;

        if (value.typeOf() == cvar::valueType::SET)
        {
            attributes->getMake(propertyIndex, NONE);

            // breaking this into loops and cases by type will be faster but much more verbose
            for (const auto& setValue: *value.getSet())
            {
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    indexedValue = setValue.getInt64();
                    break;
                case PropertyTypes_e::doubleProp:
                    indexedValue = setValue.getDouble() * 10'000;
                    break;
                case PropertyTypes_e::boolProp:
                    indexedValue = setValue.isEvalTrue() ? 1 : 0;
                    break;
                case PropertyTypes_e::textProp:
                    indexedValue = MakeHash(setValue.getString());
                    break;
                default: ;
                }

                if (propInfo->type == PropertyTypes_e::textProp)
                    attributes->getMake(propertyIndex, setValue.getString());
                else
                    attributes->getMake(propertyIndex, indexedValue);

                add(propertyIndex, indexedValue, mode);
            }
        }
        else
        {
            switch (propInfo->type)
            {
            case PropertyTypes_e::intProp:
                indexedValue = value.getInt64();
                break;
            case PropertyTypes_e::doubleProp:
                indexedValue = value.getDouble() * 10'000;
                break;
            case PropertyTypes_e::boolProp:
                indexedValue = value.isEvalTrue() ? 1 : 0;
                break;
            case PropertyTypes_e::textProp:
                indexedValue = MakeHash(value.getString());
                break;
            default: ;
            }

            attributes->getMake(propertyIndex, NONE);

            if (propInfo->type == PropertyTypes_e::textProp)
                attributes->getMake(propertyIndex, value.getString());
            else
                attributes->getMake(propertyIndex, indexedValue);

            add(propertyIndex, indexedValue, mode);
        }
    }
}

void IndexDiffing::add(const Grid* grid, Mode_e mode)
{
    const auto properties = grid->getTable()->getProperties();
    const auto rows = grid->getRows();
    const auto& setData = grid->getSetData();
    const auto colMap = grid->getPropertyMap();
    for (auto r : *rows)
    {
        for (auto c = 0; c < colMap->propertyCount; ++c)
        {
            const auto actualProperty = colMap->propertyMap[c];
            // skip NONE values, placeholder (non-event) properties and auto-generated properties (like session)
            if (r->cols[c] == NONE || (actualProperty >= PROP_INDEX_OMIT_FIRST && actualProperty <= PROP_INDEX_OMIT_LAST))
                continue;

            if (const auto propInfo = properties->getProperty(actualProperty); propInfo)
            {
                if (propInfo->isSet)
                {
                    // cast SetInfo_s over the value and get offset and length
                    const auto& ol = reinterpret_cast<SetInfo_s*>(&r->cols[c]);

                    // write out values
                    for (auto idx = ol->offset; idx < ol->offset + ol->length; ++idx)
                        add(actualProperty, setData[idx], mode);
                }
                else
                {
                    add(actualProperty, r->cols[c], mode);
                }
            }
        }
    }
}

void IndexDiffing::iterAdded(const std::function<void(int32_t, int64_t)>& cb)
{
    for (auto& a : after)
        if (before.find(a.first) == before.end())
            cb(a.first.first, a.first.second);
}

void IndexDiffing::iterRemoved(const std::function<void(int32_t, int64_t)>& cb)
{
    for (auto& b : before)
        if (after.find(b.first) == after.end() && b.first.second != NONE)
            cb(b.first.first, b.first.second);
}

Grid::~Grid()
{
    if (propertyMap && table)
        table->getPropertyMapper()->releaseMap(propertyMap);
}

void Grid::reset()
{
    rows.clear(); // release the rows - likely to not free vector internals
    mem.reset();  // release the memory to the pool - will always leave one page
    rawData = nullptr;
    propHash = 0;
    hasInsert = { false };
}

void Grid::reinitialize()
{
    reset();
    if (propertyMap && table)
        table->getPropertyMapper()->releaseMap(propertyMap);
    propertyMap = nullptr;
    table = nullptr;
    blob = nullptr;
    attributes = nullptr;
}

bool Grid::mapSchema(Table* tablePtr, Attributes* attributesPtr)
{
    // if we are already mapped on this object, skip all this
    if (tablePtr && table && tablePtr->getName() == table->getName())
        return true;
    if (propertyMap)
        table->getPropertyMapper()->releaseMap(propertyMap);
    table = tablePtr;
    attributes = attributesPtr;
    blob = attributes->getBlob();
    propertyMap = table->getPropertyMapper()->mapSchema(tablePtr, attributesPtr);
    emptyRow = newRow();
    return true;
}

bool Grid::mapSchema(Table* tablePtr, Attributes* attributesPtr, const vector<string>& propertyNames)
{
    // if we are already mapped on this object, skip all this
    if (tablePtr && table && tablePtr->getName() == table->getName())
        return true;
    if (propertyMap)
        table->getPropertyMapper()->releaseMap(propertyMap);
    table = tablePtr;
    attributes = attributesPtr;
    blob = attributes->getBlob();
    propertyMap = table->getPropertyMapper()->mapSchema(tablePtr, attributesPtr, propertyNames);
    emptyRow = newRow();
    return true;
}

AttributeBlob* Grid::getAttributeBlob() const { return attributes->blob; }

cjson Grid::toJSON()
{
    auto properties = table->getProperties();
    cjson doc;

    if (table->numericCustomerIds)
        doc.set("id", this->rawData->id);
    else
        doc.set("id", this->rawData->getIdStr());

    auto propDoc = doc.setObject("properties");
    const auto props = getProps(false);

    const auto propDict = props.getDict();
    if (propDict)
    {
        for (const auto &key : *propDict)
        {
            const auto propInfo = properties->getProperty(key.first);

            if (!propInfo)
                continue;

            if (propInfo->isSet && key.second.typeOf() == cvar::valueType::SET)
            {
                auto propList = propDoc->setArray(key.first);
                for (const auto &setItem : *key.second.getSet())
                {
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    propList->push(key.second.getInt64());
                    break;
                case PropertyTypes_e::doubleProp:
                    propList->push(key.second.getDouble());
                    break;
                case PropertyTypes_e::boolProp:
                    propList->push(key.second.getBool());
                    break;
                case PropertyTypes_e::textProp:
                    propList->push(key.second.getString());
                    break;
                }
                }
            }
            else if (propInfo->isSet && key.second.typeOf() == cvar::valueType::LIST)
            {
                auto propList = propDoc->setArray(key.first);
                for (const auto &setItem : *key.second.getList())
                {
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    propList->push(key.second.getInt64());
                    break;
                case PropertyTypes_e::doubleProp:
                    propList->push(key.second.getDouble());
                    break;
                case PropertyTypes_e::boolProp:
                    propList->push(key.second.getBool());
                    break;
                case PropertyTypes_e::textProp:
                    propList->push(key.second.getString());
                    break;
                }
                }
            }
            else
            {
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    propDoc->set(key.first, key.second.getInt64());
                    break;
                case PropertyTypes_e::doubleProp:
                    propDoc->set(key.first, key.second.getDouble());
                    break;
                case PropertyTypes_e::boolProp:
                    propDoc->set(key.first, key.second.getBool());
                    break;
                case PropertyTypes_e::textProp:
                    propDoc->set(key.first, key.second.getString());
                    break;
                }
            }
        }

    }

    auto rowDoc = doc.setArray("events");

    const auto convertToJSON = [&](cjson* branch, Properties::Property_s* propInfo, int64_t value, bool isArray)
    {
        switch (propInfo->type)
        {
        case PropertyTypes_e::intProp:
            if (isArray)
                branch->push(value);
            else
                branch->set(propInfo->name, value);
            break;
        case PropertyTypes_e::doubleProp:
            if (isArray)
                branch->push(value / 10000.0);
            else
                branch->set(propInfo->name, value / 10000.0);
            break;
        case PropertyTypes_e::boolProp:
            if (isArray)
                branch->push(value != 0);
            else
                branch->set(propInfo->name, value != 0);
            break;
        case PropertyTypes_e::textProp:
        {
            if (const auto text = attributes->blob->getValue(propInfo->idx, value); text)
            {
                if (isArray)
                    branch->push(text);
                else
                    branch->set(propInfo->name, text);
            }
        }
        break;
        default:
            break;
        }
    };

    for (auto row : rows)
    {
        auto rootObj = rowDoc->pushObject();
        rootObj->set("stamp", row->cols[PROP_STAMP]);
        rootObj->set("stamp_iso", Epoch::EpochToISO8601(row->cols[PROP_STAMP]));
        rootObj->set("event", attributes->blob->getValue(PROP_EVENT, row->cols[PROP_EVENT]));
        auto rowObj = rootObj->setObject("_");
        for (auto c = 0; c < propertyMap->propertyCount; ++c)
        {
            // get the property information
            const auto propInfo = properties->getProperty(propertyMap->propertyMap[c]);

            if (propInfo->idx < 1000) // first 1000 are reserved
                continue;

            const auto value = row->cols[c];
            if (value == NONE)
                continue;

            if (propInfo->isSet)
            {
                const auto set = rowObj->setArray(propInfo->name);
                const auto ol = reinterpret_cast<const SetInfo_s*>(&value);
                for (auto offset = ol->offset; offset < ol->offset + ol->length; ++offset)
                    convertToJSON(set, propInfo, this->setData[offset], true);
            }
            else
            {
                convertToJSON(rowObj, propInfo, value, false);
            }
        }
    }
    return doc;
}

Col_s* Grid::newRow()
{
    // NOTE: gcc seems to find the for loop below some sort of undefined
    // behavior, and with -o# it compiles incorrectly, it will segfault while
    // assigning *iter.
    // adding volatile makes it happy. I've had gcc do similar things with
    // for loops using pointers and *value = something
    const volatile auto row = recast<int64_t*>(mem.newPtr(propertyMap->rowBytes));

    for (auto iter = row; iter < row + propertyMap->propertyCount; ++iter)
        *iter = NONE;

    if (propertyMap->uuidPropIndex != -1 && rawData)
        *(row + propertyMap->uuidPropIndex) = rawData->id;

    return reinterpret_cast<Col_s*>(row);
}

cvar Grid::getProps(const bool propsMayChange)
{
    if (!rawData->props)
        return cvar(cvar::valueType::DICT);

    cvar var;

    // deserialize the props into a cvar for injection into the interpreter
    varBlob::deserialize(var, rawData->props);

    // hash props so we can detect changes
    propHash = varBlob::hash(var);

    if (propsMayChange)
        diff.add(this, var, IndexDiffing::Mode_e::before);

    return var;
}

void Grid::setProps(cvar& var)
{

    diff.add(this, var, IndexDiffing::Mode_e::after);

    // are the props deleted or empty? Yes, then lets free memory
    if (var == NONE || var.len() == 0)
    {
        if (rawData->props)
            PoolMem::getPool().freePtr(rawData->props);
        rawData->props = nullptr;
        return;
    }

    // if anything has changed, lets replace the props and free the last props
    const auto afterHash = varBlob::hash(var);

    if  (afterHash != propHash)
    {
        if (rawData->props)
            PoolMem::getPool().freePtr(rawData->props);

        varBlob::serialize(propMem, var);
        rawData->props = propMem.flatten();
        propMem.reset();

        diff.iterRemoved(
            [&](const int32_t col, const int64_t val)
            {
                attributes->setDirty(this->rawData->linId, col, val, false);
            }
        );

        diff.iterAdded(
            [&](const int32_t col, const int64_t val)
            {
                attributes->setDirty(this->rawData->linId, col, val, true);
            }
        );
    }
}

void Grid::mount(PersonData_s* personData)
{
#ifdef DEBUG
    Logger::get().fatal((table), "mapSchema must be called before mount");
#endif
    reset();
    rawData = personData;

    if (propertyMap->uuidPropIndex != -1 && emptyRow)
        emptyRow->cols[propertyMap->uuidPropIndex] = rawData->id;
}

void Grid::prepare()
{
    if (!propertyMap || !rawData || !rawData->bytes || !propertyMap->propertyCount)
        return;

    setData.clear();

    const auto expandedBytes = cast<char*>(PoolMem::getPool().getPtr(rawData->bytes));
    LZ4_decompress_fast(rawData->getComp(), expandedBytes, rawData->bytes);

    // make a blank row
    auto row = newRow();
    // read pointer - will increment through the compacted set
    auto read = expandedBytes;
    // end pointer - when we get here we are done
    const auto end = read + rawData->bytes;
    auto session = 0;
    int64_t lastSessionTime = 0;
    auto properties = table->getProperties();

    while (read < end)
    {
        const auto cursor = reinterpret_cast<Cast_s*>(read);
        /**
        * when we are querying we only need the properties
        * referenced in the query, as such, many properties
        * will be skipped, as we are not serializing the
        * data out (saving it) after a query it's okay to
        * selectively deserialize it.
        */
        if (cursor->propIndex == -1) // -1 is new row
        {
            if (propertyMap->sessionPropIndex != -1)
            {
                if (row->cols[PROP_STAMP] - lastSessionTime > sessionTime)
                    ++session;
                lastSessionTime = row->cols[PROP_STAMP];
                row->cols[propertyMap->sessionPropIndex] = session;
            } // if we are parsing the property row we do not
            // push it, we store it under `propRow`
            rows.push_back(row);
            row = newRow();
            read += sizeOfCastHeader;
            continue;
        }
        const auto mappedProperty = propertyMap->reverseMap[cursor->propIndex];
        if (const auto propInfo = properties->getProperty(cursor->propIndex); propInfo)
        {
            if (propInfo->isSet)
            {
                read += sizeof(int16_t); // += 2
                const auto count = static_cast<int>(*reinterpret_cast<int16_t*>(read));
                read += sizeof(int16_t); // += 2
                const auto startIdx = setData.size();
                auto counted = 0;

                while (counted < count)
                {
                    setData.push_back(*reinterpret_cast<int64_t*>(read));
                    read += sizeof(int64_t);
                    ++counted;
                }

                if (mappedProperty < 0 || mappedProperty >= propertyMap->propertyCount)
                {
                    continue;
                }

                // let our row use an encoded value for the property.
                SetInfo_s info { count, static_cast<int>(startIdx) };
                *(row->cols + mappedProperty) = *reinterpret_cast<int64_t*>(&info);
            }
            else
            {
                if (mappedProperty < 0 || mappedProperty >= propertyMap->propertyCount)
                {
                    read += sizeOfCast;
                    continue;
                }
                *(row->cols + mappedProperty) = cursor->val64;
                read += sizeOfCast;
            }
        }
        else
            read += sizeOfCast;
    }
    PoolMem::getPool().freePtr(expandedBytes);
}

PersonData_s* Grid::commit()
{

    if (!hasInsert)
        return rawData;

    // this is the worst case scenario temp buffer size for this data.
    // (properties * rows) + (properties * row headers) + number_of_set_values
    const auto rowCount = rows.size();
    const auto tempBufferSize =
        (rowCount * (propertyMap->propertyCount * sizeOfCast)) +
        (rowCount * sizeOfCastHeader) + (setData.size() * sizeof(int64_t)) + // the set data
        ((rowCount * propertyMap->propertyCount) * (sizeOfCastHeader + sizeof(int32_t))); // the NONES at the end of the list

    // make an intermediate buffer that is fully uncompressed
    const auto intermediateBuffer = recast<char*>(PoolMem::getPool().getPtr(tempBufferSize));
    auto write = intermediateBuffer;

    Cast_s* cursor;
    auto bytesNeeded = 0;
    auto properties = table->getProperties();

    // lambda to encode and minimize an row
    const auto pushRow = [&](Row* r)
    {
        for (auto c = 0; c < propertyMap->propertyCount; ++c)
        {
            const auto actualProperty = propertyMap->propertyMap[c];

            // skip NONE values, placeholder (non-event) properties and auto-generated properties (like session)
            if (r->cols[c] == NONE || (actualProperty >= PROP_INDEX_OMIT_FIRST && actualProperty <= PROP_INDEX_OMIT_LAST))
                continue;

            if (const auto propInfo = properties->getProperty(actualProperty); propInfo)
            {
                if (propInfo->isSet)
                {
                    /* Output stream looks like this:
                    *
                    *  int16_t property
                    *  int16_t length
                    *  int64_t values[]
                    */

                    // write out property id
                    *reinterpret_cast<int16_t*>(write) = actualProperty;
                    write += sizeof(int16_t);
                    bytesNeeded += sizeof(int16_t);

                    // cast SetInfo_s over the value and get offset and length
                    // write out count
                    const auto start = static_cast<int32_t>(reinterpret_cast<SetInfo_s*>(&r->cols[c])->offset);
                    auto& count = *reinterpret_cast<int16_t*>(write);

                    count = static_cast<int16_t>(reinterpret_cast<SetInfo_s*>(&r->cols[c])->length);
                    write += sizeof(int16_t);
                    bytesNeeded += sizeof(int16_t); // write out values

                    for (auto idx = start; idx < start + count; ++idx)
                    {
                        *recast<int64_t*>(write) = setData[idx];
                        write += sizeof(int64_t);
                        bytesNeeded += sizeof(int64_t);
                    }
                }
                else
                {
                    cursor = recast<Cast_s*>(write);
                    cursor->propIndex = actualProperty;
                    cursor->val64 = r->cols[c];

                    write += sizeOfCast;
                    bytesNeeded += sizeOfCast;
                }
            }
        }

        cursor = recast<Cast_s*>(write); // END OF ROW - write a "row" marker at the end of the row
        cursor->propIndex = -1;

        write += sizeOfCastHeader;

        bytesNeeded += sizeOfCastHeader;
    };

    // push the rows through the encode
    for (auto r : rows)
        pushRow(r);

    const auto maxBytes = LZ4_compressBound(bytesNeeded);
    const auto compBuffer = cast<char*>(PoolMem::getPool().getPtr(maxBytes));

    const auto oldCompBytes = rawData->comp;
    const auto newCompBytes = LZ4_compress_fast(
        intermediateBuffer,
        compBuffer,
        bytesNeeded,
        maxBytes,
        table->personCompression);

    const auto newPersonSize = (rawData->size() - oldCompBytes) + newCompBytes;

    // size() includes data, we adjust
    const auto newPerson = recast<PersonData_s*>(PoolMem::getPool().getPtr(newPersonSize)); // copy old header
    memcpy(newPerson, rawData, PERSON_DATA_SIZE);

    newPerson->comp = newCompBytes; // adjust offsets
    newPerson->bytes = bytesNeeded; // copy old id bytes

    if (rawData->idBytes)
        memcpy(newPerson->getIdPtr(), rawData->getIdPtr(), static_cast<size_t>(rawData->idBytes)); // copy NEW flags

    // copy NEW compressed events
    if (newCompBytes)
        memcpy(newPerson->getComp(), compBuffer, static_cast<size_t>(newCompBytes)); // get rid of the intermediate copy

    PoolMem::getPool().freePtr(intermediateBuffer);
    PoolMem::getPool().freePtr(compBuffer); // release the original
    PoolMem::getPool().freePtr(rawData);    // it probably got longer!

    rawData = newPerson;
    return rawData;
}

bool Grid::cull()
{
    // empty? no cull
    if (rows.empty())
        return false; // not at row limit, and first event is within time window? no cull

    if (rows.size() < static_cast<size_t>(table->eventMax) && rows[0]->cols[PROP_STAMP] > Now() - table->eventTtl)
        return false;

    diff.reset();
    auto removed = false;
    auto rowCount = rows.size();
    diff.add(this, IndexDiffing::Mode_e::before);

    // cull if row count exceeds limit
    if (static_cast<int>(rowCount) > table->eventMax)
    {
        const auto numToErase = rowCount - table->eventMax;
        rows.erase(rows.begin(), rows.begin() + numToErase);
        rowCount = rows.size();
        removed = true;
    }

    const auto cullStamp = Now() - table->eventTtl;
    auto expiredCount = 0;
    for (const auto& r : rows)
    {
        if (r->cols[PROP_STAMP] > cullStamp)
            break;
        ++expiredCount;
    }

    if (expiredCount)
    {
        const auto numToErase = rowCount - expiredCount;
        rows.erase(rows.begin(), rows.begin() + numToErase);
        removed = true;
    }

    diff.add(this, IndexDiffing::Mode_e::after);

    // what things are no longer referenced in anyway
    // within our row set? De-index those items.
    diff.iterRemoved(
        [&](int32_t col, int64_t val)
        {
            attributes->setDirty(this->rawData->linId, col, val, false);
        }
    );

    return removed;
}

int Grid::getGridProperty(const int propIndex) const
{
    return propertyMap->reverseMap[propIndex];
}

bool Grid::isFullSchema() const
{
    return (propertyMap && propertyMap->hash == 0);
}

Grid::RowType_e Grid::insertParse(Properties* properties, cjson* doc, Col_s* insertRow)
{
    auto hasEventProp = false;
    auto eventPropCount = 0;
    auto hasCustomerProps = false;

    const auto inboundProperties = doc->getNodes();

    for (auto c : inboundProperties)
    {
        const auto propName = c->name();
        // look for the name (by hash) in the insertMap
        if (const auto iter = propertyMap->insertMap.find(MakeHash(propName)); iter != propertyMap->insertMap.end())
        {
            const auto schemaCol = propertyMap->propertyMap[iter->second];
            const auto propInfo = properties->getProperty(schemaCol);
            const auto col = iter->second;

            if (propInfo->isCustomerProperty)
            {
                hasCustomerProps = true;
                continue;
            }

            if (propInfo->idx >= PROP_INDEX_USER_DATA)
            {
                // do we actually have event props, or just a bare 'event' property, well check below
                ++eventPropCount;
            }

            // we need an the 'event' prop to be set to record event row properties,
            if (propName == "event")
                hasEventProp = true;

            attributes->getMake(schemaCol, NONE);
            attributes->setDirty(this->rawData->linId, schemaCol, NONE);
            auto tempVal = NONE;
            string tempString;

            switch (c->type())
            {
            case cjson::Types_e::INT:
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    tempVal = c->getInt();
                    break;
                case PropertyTypes_e::doubleProp:
                    tempVal = cast<int64_t>(c->getInt() * 10000LL);
                    break;
                case PropertyTypes_e::boolProp:
                    tempVal = c->getInt() ? 1 : 0;
                    break;
                case PropertyTypes_e::textProp:
                    tempString = to_string(c->getInt());
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::DBL:
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    tempVal = cast<int64_t>(c->getDouble());
                    break;
                case PropertyTypes_e::doubleProp:
                    tempVal = cast<int64_t>(c->getDouble() * 10000LL);
                    break;
                case PropertyTypes_e::boolProp:
                    tempVal = c->getDouble() != 0;
                    break;
                case PropertyTypes_e::textProp:
                    tempString = to_string(c->getDouble());
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::STR:
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp: case PropertyTypes_e::doubleProp:
                    continue;
                case PropertyTypes_e::boolProp:
                    tempVal = c->getString() != "0";
                    break;
                case PropertyTypes_e::textProp:
                    tempString = c->getString();
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::BOOL:
                switch (propInfo->type)
                {
                case PropertyTypes_e::intProp:
                    tempVal = c->getBool() ? 1 : 0;
                    break;
                case PropertyTypes_e::doubleProp:
                    tempVal = c->getBool() ? 10000 : 0;
                    break;
                case PropertyTypes_e::boolProp:
                    tempVal = c->getBool();
                    break;
                case PropertyTypes_e::textProp:
                    tempString = c->getBool() ? "true" : "false";
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::ARRAY:
            {
                if (!propInfo->isSet)
                    continue;
                auto aNodes = c->getNodes();
                const auto startIdx = setData.size();
                for (auto n : aNodes)
                {
                    switch (n->type())
                    {
                    case cjson::Types_e::INT:
                        switch (propInfo->type)
                        {
                        case PropertyTypes_e::intProp:
                            tempVal = n->getInt();
                            break;
                        case PropertyTypes_e::doubleProp:
                            tempVal = cast<int64_t>(n->getInt() * 10000LL);
                            break;
                        case PropertyTypes_e::boolProp:
                            tempVal = n->getInt() ? 1 : 0;
                            break;
                        case PropertyTypes_e::textProp:
                            tempString = to_string(n->getInt());
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::DBL:
                        switch (propInfo->type)
                        {
                        case PropertyTypes_e::intProp:
                            tempVal = cast<int64_t>(n->getDouble());
                            break;
                        case PropertyTypes_e::doubleProp:
                            tempVal = cast<int64_t>(n->getDouble() * 10000LL);
                            break;
                        case PropertyTypes_e::boolProp:
                            tempVal = n->getDouble() != 0;
                            break;
                        case PropertyTypes_e::textProp:
                            tempString = to_string(n->getDouble());
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::STR:
                        switch (propInfo->type)
                        {
                        case PropertyTypes_e::intProp: case PropertyTypes_e::doubleProp:
                            continue;
                        case PropertyTypes_e::boolProp:
                            tempVal = n->getString() != "0";
                            break;
                        case PropertyTypes_e::textProp:
                            tempString = n->getString();
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::BOOL:
                        switch (propInfo->type)
                        {
                        case PropertyTypes_e::intProp:
                            tempVal = n->getBool() ? 1 : 0;
                            break;
                        case PropertyTypes_e::doubleProp:
                            tempVal = n->getBool() ? 10000 : 0;
                            break;
                        case PropertyTypes_e::boolProp:
                            tempVal = n->getBool();
                            break;
                        case PropertyTypes_e::textProp:
                            tempString = n->getBool() ? "true" : "false";
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    default:
                        continue;
                    }

                    if (propInfo->type == PropertyTypes_e::textProp)
                        attributes->getMake(schemaCol, tempString);
                    else
                        attributes->getMake(schemaCol, tempVal);

                    attributes->setDirty(this->rawData->linId, schemaCol, tempVal);
                    setData.push_back(tempVal);
                }

                // put value in row
                SetInfo_s info { static_cast<int>(setData.size() - startIdx), static_cast<int>(startIdx) };
                insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
                hasInsert = true;

                // FIX??
            }
            default:
                continue;
            }


            if (c->type() != cjson::Types_e::ARRAY)
            {
                // if it's pure prop, or it's not a prop at all, or it is a prop with an event
                // and this event is the same or more recent than the last prop in the dataset
                if (propInfo->type == PropertyTypes_e::textProp)
                    attributes->getMake(schemaCol, tempString);
                else
                    attributes->getMake(schemaCol, tempVal);

                attributes->setDirty(this->rawData->linId, schemaCol, tempVal);

                if (propInfo->isSet)
                {
                    SetInfo_s info { 1, static_cast<int>(setData.size()) };
                    insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
                    setData.push_back(tempVal);
                }
                else
                {
                    insertRow->cols[col] = tempVal;
                }

                hasInsert = true;
            }
        }
        else
        {
            // todo: do we care about non-mapped properties.
        }
    }

    // if there are no event row properties then we don't really have an event
    // in which case we will skip inserting the empty event
    if (eventPropCount == 0)
        hasEventProp = false;

    if (hasCustomerProps)
    {
        auto insertProps = getProps(true);

        for (auto c : inboundProperties)
        {
            // look for the name (by hash) in the insertMap
            if (const auto iter = propertyMap->insertMap.find(MakeHash(c->name())); iter != propertyMap->insertMap.end())
            {
                const auto schemaCol = propertyMap->propertyMap[iter->second];
                const auto propInfo = properties->getProperty(schemaCol);
                const auto& colName = propInfo->name;

                if (!propInfo->isCustomerProperty)
                    continue;

                switch (c->type())
                {
                case cjson::Types_e::INT:
                    switch (propInfo->type)
                    {
                    case PropertyTypes_e::intProp:
                    case PropertyTypes_e::doubleProp:
                        insertProps[colName] = c->getInt();
                        break;
                    case PropertyTypes_e::boolProp:
                        insertProps[colName] = c->getInt() ? true : false;
                        break;
                    case PropertyTypes_e::textProp:
                        insertProps[colName] = to_string(c->getInt());
                        break;
                    }
                    break;
                case cjson::Types_e::DBL:
                    switch (propInfo->type)
                    {
                    case PropertyTypes_e::intProp:
                    case PropertyTypes_e::doubleProp:
                        insertProps[colName] = c->getDouble();
                        break;
                    case PropertyTypes_e::boolProp:
                        insertProps[colName] = c->getDouble() != 0 ? true : false;
                        break;
                    case PropertyTypes_e::textProp:
                        insertProps[colName] = to_string(c->getDouble());
                        break;
                    }
                    break;
                case cjson::Types_e::STR:
                    switch (propInfo->type)
                    {
                    case PropertyTypes_e::intProp:
                    case PropertyTypes_e::doubleProp:
                        continue;
                    case PropertyTypes_e::boolProp:
                        insertProps[colName] = c->getString() != "0";
                        break;
                    case PropertyTypes_e::textProp:
                        insertProps[colName] = c->getString();
                        break;
                    }
                    break;
                case cjson::Types_e::BOOL:
                    switch (propInfo->type)
                    {
                    case PropertyTypes_e::intProp:
                    case PropertyTypes_e::doubleProp:
                        insertProps[colName] = c->getBool() ? 1 : 0;
                        break;
                    case PropertyTypes_e::boolProp:
                        insertProps[colName] = c->getBool();
                        break;
                    case PropertyTypes_e::textProp:
                        insertProps[colName] = c->getBool() ? "true" : "false";
                        break;
                    }
                    break;
                case cjson::Types_e::ARRAY:
                    {
                        if (!propInfo->isSet)
                            continue;

                        insertProps[colName].set();

                        auto aNodes = c->getNodes();
                        const auto startIdx = setData.size();
                        for (auto n : aNodes)
                        {
                            switch (n->type())
                            {
                            case cjson::Types_e::INT:
                                switch (propInfo->type)
                                {
                                case PropertyTypes_e::intProp:
                                case PropertyTypes_e::doubleProp:
                                    insertProps[colName] += n->getInt();
                                    break;
                                case PropertyTypes_e::boolProp:
                                    insertProps[colName] += n->getInt() ? true : false;
                                    break;
                                case PropertyTypes_e::textProp:
                                    insertProps[colName] += to_string(n->getInt());
                                    break;
                                }
                                break;
                            case cjson::Types_e::DBL:
                                switch (propInfo->type)
                                {
                                case PropertyTypes_e::intProp:
                                case PropertyTypes_e::doubleProp:
                                    insertProps[colName] += cast<int64_t>(n->getDouble());
                                    break;
                                case PropertyTypes_e::boolProp:
                                    insertProps[colName] += n->getDouble() != 0;
                                    break;
                                case PropertyTypes_e::textProp:
                                    insertProps[colName] += to_string(n->getDouble());
                                    break;
                                }
                                break;
                            case cjson::Types_e::STR:
                                switch (propInfo->type)
                                {
                                case PropertyTypes_e::intProp:
                                case PropertyTypes_e::doubleProp:
                                    continue;
                                case PropertyTypes_e::boolProp:
                                    insertProps[colName] += n->getString() != "0";
                                    break;
                                case PropertyTypes_e::textProp:
                                    insertProps[colName] += n->getString();
                                    break;
                                }
                                break;
                            case cjson::Types_e::BOOL:
                                switch (propInfo->type)
                                {
                                case PropertyTypes_e::intProp:
                                case PropertyTypes_e::doubleProp:
                                    insertProps[colName] += c->getBool() ? 1 : 0;
                                    break;
                                case PropertyTypes_e::boolProp:
                                    insertProps[colName] += c->getBool();
                                    break;
                                case PropertyTypes_e::textProp:
                                    insertProps[colName] += c->getBool() ? "true" : "false";
                                    break;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        setProps(insertProps);
    }

    if (hasCustomerProps && hasEventProp)
        return RowType_e::event_and_prop;
    if (hasCustomerProps)
        return RowType_e::prop;
    if (hasEventProp)
        return RowType_e::event;

    return RowType_e::junk;
}

void Grid::insertEvent(cjson* rowData)
{
    const auto attrNode = rowData;

    if (!attrNode)
        return;

    const auto stampNode = rowData->xPath("/stamp");
    const auto eventName = rowData->xPathString("/event", "");

    const auto insertRow = newRow();
    const auto properties = table->getProperties();

    // parse the event (properties & props)
    const auto insertType = insertParse(properties, attrNode, insertRow);

    // is there any event here? if not, lets leave
    if (insertType == RowType_e::junk || insertType == RowType_e::prop)
        return;

    int64_t stamp = 0;

    // check for stamps
    if (stampNode && stampNode->type() == cjson::Types_e::STR)
        stamp = Epoch::fixMilli(Epoch::ISO8601ToEpoch(stampNode->getString()));
    else if (stampNode)
        stamp = Epoch::fixMilli(stampNode->getInt());

    if (stamp < 0)
        return;

    insertRow->cols[PROP_STAMP] = stamp;

    auto rowCount = rows.size();
    const auto lastRowStamp = rowCount ? rows.back()->cols[PROP_STAMP] : 0;

    decltype(newRow()) row = nullptr;

    const auto getRowHash = [&](Col_s* rowPtr) -> int64_t
    {
        auto hash = rowPtr->cols[PROP_STAMP];
        for (auto col = PROP_INDEX_USER_DATA; col < propertyMap->propertyCount; ++col)
        {
            if (rowPtr->cols[col] == NONE)
                continue;

            const auto propInfo = properties->getProperty(propertyMap->propertyMap[col]);

            // don't count deleted properties or props
            if (!propInfo || propInfo->deleted || propInfo->isCustomerProperty)
                continue;

            if (propInfo->isSet)
            {
                const auto ol = *reinterpret_cast<SetInfo_s*>(&rowPtr->cols[col]);
                for (auto idx = ol.offset; idx < ol.offset + ol.length; ++idx)
                    hash = HashPair(setData[idx], hash);
            }
            else
            {
                hash = HashPair(rowPtr->cols[col], hash);
            }
        }
        return hash;
    };
    auto insertBefore = -1; // where a new row will be inserted if needed

    const auto hashedEvent = MakeHash(eventName);
    const auto eventOrderInts = table->getEventOrderHashes();
    const auto getEventOrder = [&](int64_t value) -> int
    {
        const auto iter = eventOrderInts->find(value);
        if (iter != eventOrderInts->end())
            return (*iter).second;
        return 99;
    };
    const auto insertZOrder = getEventOrder(hashedEvent);

    // lambda using binary search to find insert index
    const auto findInsert = [&]() -> int
    {
        auto first = 0;
        auto last = static_cast<int>(rowCount - 1);
        auto mid = last >> 1;
        while (first <= last)
        {
            if (stamp > rows[mid]->cols[PROP_STAMP])
                first = mid + 1; // search bottom of list
            else if (stamp < rows[mid]->cols[PROP_STAMP])
                last = mid - 1; // search top of list
            else
                return mid;
            mid = (first + last) >> 1; // usually written like first + ((last - first) / 2)
        }
        return -(first + 1);
    };

    auto i = rowCount ? findInsert() : 0;

    // negative value (made positive - 1) is the insert position
    if (i < 0)
        i = -i - 1;

    if (i != static_cast<int>(rowCount)) // if they are equal skip all this, we are appending
    {
        // walk back to the beginning of all rows sharing this time stamp
        if (rowCount)
        {
            while (i > 0 && rows[i]->cols[PROP_STAMP] == stamp)
                --i; // walk forward to find our insertion point
        }

        for (; i < static_cast<int>(rowCount); i++)
        {
            // we have found rows with same stamp
            if (rows[i]->cols[0] == stamp)
            {
                auto zOrder = getEventOrder(rows[i]->cols[PROP_EVENT]);
                // we have found rows in this stamp with the same zOrder
                if (zOrder == insertZOrder)
                {
                    // look this date range and zorder to see if we have a row group
                    // match (as in, we are replacing a row)
                    for (; i < static_cast<int>(rowCount); i++)
                    {
                        zOrder = getEventOrder(rows[i]->cols[PROP_EVENT]);

                        // we have moved passed replaceable rows, so insert here
                        if (rows[i]->cols[PROP_STAMP] > stamp || zOrder > insertZOrder)
                        {
                            insertBefore = i;
                            break;
                        }

                        const auto insertHash = getRowHash(insertRow);
                        const auto currentRowHash = getRowHash(rows[i]);

                        // we have a matching row, we will replace this
                        if (insertHash == currentRowHash)
                        {
                            row = rows[i];
                            insertBefore = i;
                            break;
                        }
                    }
                    break;
                }
                if (zOrder > insertZOrder)
                {
                    row = rows[i];
                    insertBefore = i;
                    break;
                }
            }
            else if (rows[i]->cols[PROP_STAMP] > stamp)
            {
                insertBefore = i;
                break;
            }
        }
    }

    if (row) // delete the rows that matched, we will be replacing them
    {
        for (const auto iter = rows.begin() + insertBefore; iter != rows.end();)
            if ((*iter) == row)
            {
                rows.erase(iter);
                break;
            }
    }
    if (insertBefore == -1) // no insertion found so append
        rows.push_back(insertRow);
    else // insert before
        rows.insert(rows.begin() + insertBefore, insertRow);
}
