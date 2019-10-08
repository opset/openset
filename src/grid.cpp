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

void IndexDiffing::add(int32_t column, int64_t value, Mode_e mode)
{
    if (mode == Mode_e::before)
    {
        if (const auto iter = before.find({ column, value }); iter != before.end())
            iter->second++;
        else
            before[{ column, value }] = 1;
    }
    else
    {
        if (const auto iter = after.find({ column, value }); iter != after.end())
            iter->second++;
        else
            after[{ column, value }] = 1;
    }

    // a Value of NONE in combination with a column indicates that
    // the column is referenced. This is used to index a column, rather
    // than a column and value.
    if (value != NONE)
        add(column, NONE, mode);
}

void IndexDiffing::add(const Grid* grid, const cvar& props, Mode_e mode)
{
    if (props.typeOf() != cvar::valueType::DICT)
        return;

    const auto columns = grid->getTable()->getColumns();
    const auto attributes = grid->getAttributes();

    for (const auto &key : *props.getDict())
    {
        const auto colInfo =  columns->getColumn(key.first.getString());
        const auto& value = key.second;

        if (!colInfo || !colInfo->isProp)
            continue;

        const auto column = colInfo->idx;

        auto indexedValue = NONE;

        if (value.typeOf() == cvar::valueType::SET)
        {
            attributes->getMake(column, NONE);

            // breaking this into loops and cases by type will be faster but much more verbose
            for (const auto& setValue: *value.getSet())
            {
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    indexedValue = setValue.getInt64();
                    break;
                case columnTypes_e::doubleColumn:
                    indexedValue = setValue.getDouble() * 10'000;
                    break;
                case columnTypes_e::boolColumn:
                    indexedValue = setValue.isEvalTrue() ? 1 : 0;
                    break;
                case columnTypes_e::textColumn:
                    indexedValue = MakeHash(setValue.getString());
                    break;
                default: ;
                }

                if (colInfo->type == columnTypes_e::textColumn)
                    attributes->getMake(column, setValue.getString());
                else
                    attributes->getMake(column, indexedValue);

                add(column, indexedValue, mode);
            }
        }
        else
        {
            switch (colInfo->type)
            {
            case columnTypes_e::intColumn:
                indexedValue = value.getInt64();
                break;
            case columnTypes_e::doubleColumn:
                indexedValue = value.getDouble() * 10'000;
                break;
            case columnTypes_e::boolColumn:
                indexedValue = value.isEvalTrue() ? 1 : 0;
                break;
            case columnTypes_e::textColumn:
                indexedValue = MakeHash(value.getString());
                break;
            default: ;
            }

            attributes->getMake(column, NONE);

            if (colInfo->type == columnTypes_e::textColumn)
                attributes->getMake(column, value.getString());
            else
                attributes->getMake(column, indexedValue);

            add(column, indexedValue, mode);
        }
    }
}

void IndexDiffing::add(const Grid* grid, Mode_e mode)
{
    const auto columns = grid->getTable()->getColumns();
    const auto rows = grid->getRows();
    const auto& setData = grid->getSetData();
    const auto colMap = grid->getColumnMap();
    for (auto r : *rows)
    {
        for (auto c = 0; c < colMap->columnCount; ++c)
        {
            const auto actualColumn = colMap->columnMap[c];
            // skip NONE values, placeholder (non-event) columns and auto-generated columns (like session)
            if (r->cols[c] == NONE || (actualColumn >= COL_OMIT_FIRST && actualColumn <= COL_OMIT_LAST))
                continue;

            if (const auto colInfo = columns->getColumn(actualColumn); colInfo)
            {
                if (colInfo->isSet)
                {
                    // cast SetInfo_s over the value and get offset and length
                    const auto& ol = reinterpret_cast<SetInfo_s*>(&r->cols[c]);

                    // write out values
                    for (auto idx = ol->offset; idx < ol->offset + ol->length; ++idx)
                        add(actualColumn, setData[idx], mode);
                }
                else
                {
                    add(actualColumn, r->cols[c], mode);
                }
            }
        }
    }
}

IndexDiffing::CVList IndexDiffing::getAdded()
{
    CVList result;
    for (auto& a : after)
        if (!before.count(a.first))
            result.push_back(a.first);
    return result;
}

IndexDiffing::CVList IndexDiffing::getRemoved()
{
    CVList result;
    for (auto& b : before)
        if (!after.count(b.first))
            result.push_back(b.first);
    return result;
}

void IndexDiffing::iterAdded(const std::function<void(int32_t, int64_t)>& cb)
{
    for (auto& a : after)
        if (!before.count(a.first))
            cb(a.first.first, a.first.second);
}

void IndexDiffing::iterRemoved(const std::function<void(int32_t, int64_t)>& cb)
{
    for (auto& b : before)
        if (!after.count(b.first) && b.first.second != NONE)
            cb(b.first.first, b.first.second);
}

Grid::~Grid()
{
    if (colMap && table)
        table->getColumnMapper()->releaseMap(colMap);
}

void Grid::reset()
{
    rows.clear(); // release the rows - likely to not free vector internals
    mem.reset();  // release the memory to the pool - will always leave one page
    rawData = nullptr;
    propHash = 0;
}

void Grid::reinit()
{
    reset();
    if (colMap && table)
        table->getColumnMapper()->releaseMap(colMap);
    colMap = nullptr;
    table = nullptr;
    blob = nullptr;
    attributes = nullptr;
}

bool Grid::mapSchema(Table* tablePtr, Attributes* attributesPtr)
{
    // if we are already mapped on this object, skip all this
    if (tablePtr && table && tablePtr->getName() == table->getName())
        return true;
    if (colMap)
        table->getColumnMapper()->releaseMap(colMap);
    table = tablePtr;
    attributes = attributesPtr;
    blob = attributes->getBlob();
    colMap = table->getColumnMapper()->mapSchema(tablePtr, attributesPtr);
    return true;
}

bool Grid::mapSchema(Table* tablePtr, Attributes* attributesPtr, const vector<string>& columnNames)
{
    // if we are already mapped on this object, skip all this
    if (tablePtr && table && tablePtr->getName() == table->getName())
        return true;
    if (colMap)
        table->getColumnMapper()->releaseMap(colMap);
    table = tablePtr;
    attributes = attributesPtr;
    blob = attributes->getBlob();
    colMap = table->getColumnMapper()->mapSchema(tablePtr, attributesPtr, columnNames);
    return true;
}

AttributeBlob* Grid::getAttributeBlob() const { return attributes->blob; }

cjson Grid::toJSON() const
{
    cjson doc;
    doc.set("id_string", this->rawData->getIdStr());
    doc.set("id", this->rawData->id);
    auto rowDoc = doc.setArray("rows");
    auto columns = table->getColumns();
    const auto convertToJSON = [&](cjson* branch, Columns::Columns_s* colInfo, int64_t value, bool isArray)
    {
        switch (colInfo->type)
        {
        case columnTypes_e::intColumn:
            if (isArray)
                branch->push(value);
            else
                branch->set(colInfo->name, value);
            break;
        case columnTypes_e::doubleColumn:
            if (isArray)
                branch->push(value / 10000.0);
            else
                branch->set(colInfo->name, value / 10000.0);
            break;
        case columnTypes_e::boolColumn:
            if (isArray)
                branch->push(value != 0);
            else
                branch->set(colInfo->name, value != 0);
            break;
        case columnTypes_e::textColumn:
        {
            if (const auto text = attributes->blob->getValue(colInfo->idx, value); text)
            {
                if (isArray)
                    branch->push(text);
                else
                    branch->set(colInfo->name, text);
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
        rootObj->set("stamp", row->cols[COL_STAMP]);
        rootObj->set("stamp_iso", Epoch::EpochToISO8601(row->cols[COL_STAMP]));
        rootObj->set("event", attributes->blob->getValue(COL_EVENT, row->cols[COL_EVENT]));
        auto rowObj = rootObj->setObject("_");
        for (auto c = 0; c < colMap->columnCount; ++c)
        {
            // get the column information
            const auto colInfo = columns->getColumn(colMap->columnMap[c]);

            if (colInfo->idx < 1000) // first 1000 are reserved
                continue;

            const auto value = row->cols[c];
            if (value == NONE)
                continue;

            if (colInfo->isSet)
            {
                const auto set = rowObj->setArray(colInfo->name);
                const auto ol = reinterpret_cast<const SetInfo_s*>(&value);
                for (auto offset = ol->offset; offset < ol->offset + ol->length; ++offset)
                    convertToJSON(set, colInfo, this->setData[offset], true);
            }
            else
            {
                convertToJSON(rowObj, colInfo, value, false);
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
    const volatile auto row = recast<int64_t*>(mem.newPtr(colMap->rowBytes));
    for (auto iter = row; iter < row + colMap->columnCount; ++iter)
        *iter = NONE;
    if (colMap->uuidColumn != -1)
        *(row + colMap->uuidColumn) = rawData->id;
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
    }

    diff.iterAdded(
        [&](const int32_t col, const int64_t val)
        {
            attributes->setDirty(this->rawData->linId, col, val, true);
        }
    );

    diff.iterRemoved(
        [&](const int32_t col, const int64_t val)
        {
            attributes->setDirty(this->rawData->linId, col, val, false);
        }
    );
}

void Grid::mount(PersonData_s* personData)
{
#ifdef DEBUG
    Logger::get().fatal((table), "mapSchema must be called before mount");
#endif
    reset();
    rawData = personData;
}

void Grid::prepare()
{
    if (!colMap || !rawData || !rawData->bytes || !colMap->columnCount)
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
    auto columns = table->getColumns();

    while (read < end)
    {
        const auto cursor = reinterpret_cast<Cast_s*>(read);
        /**
        * when we are querying we only need the columns
        * referenced in the query, as such, many columns
        * will be skipped, as we are not serializing the
        * data out (saving it) after a query it's okay to
        * selectively deserialize it.
        */
        if (cursor->columnNum == -1) // -1 is new row
        {
            if (colMap->sessionColumn != -1)
            {
                if (row->cols[COL_STAMP] - lastSessionTime > sessionTime)
                    ++session;
                lastSessionTime = row->cols[COL_STAMP];
                row->cols[colMap->sessionColumn] = session;
            } // if we are parsing the property row we do not
            // push it, we store it under `propRow`
            rows.push_back(row);
            row = newRow();
            read += sizeOfCastHeader;
            continue;
        }
        const auto mappedColumn = colMap->reverseMap[cursor->columnNum];
        if (const auto colInfo = columns->getColumn(cursor->columnNum); colInfo)
        {
            if (colInfo->isSet)
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

                if (mappedColumn < 0 || mappedColumn >= colMap->columnCount)
                {
                    continue;
                }

                // let our row use an encoded value for the column.
                SetInfo_s info { count, static_cast<int>(startIdx) };
                *(row->cols + mappedColumn) = *reinterpret_cast<int64_t*>(&info);
            }
            else
            {
                if (mappedColumn < 0 || mappedColumn >= colMap->columnCount)
                {
                    read += sizeOfCast;
                    continue;
                }
                *(row->cols + mappedColumn) = cursor->val64;
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
    if (!rows.size()) { cout << "no rows" << endl; }

    // this is the worst case scenario temp buffer size for this data.
    // (columns * rows) + (columns * row headers) + number_of_set_values
    const auto rowCount = rows.size();
    const auto tempBufferSize =
        (rowCount * (colMap->columnCount * sizeOfCast)) +
        (rowCount * sizeOfCastHeader) + (setData.size() * sizeof(int64_t)) + // the set data
        ((rowCount * colMap->columnCount) * (sizeOfCastHeader + sizeof(int32_t))); // the NONES at the end of the list

    // make an intermediate buffer that is fully uncompressed
    const auto intermediateBuffer = recast<char*>(PoolMem::getPool().getPtr(tempBufferSize));
    auto write = intermediateBuffer;

    Cast_s* cursor;
    auto bytesNeeded = 0;
    auto columns = table->getColumns();

    // lambda to encode and minimize an row
    const auto pushRow = [&](Row* r)
    {
        for (auto c = 0; c < colMap->columnCount; ++c)
        {
            const auto actualColumn = colMap->columnMap[c];

            // skip NONE values, placeholder (non-event) columns and auto-generated columns (like session)
            if (r->cols[c] == NONE || (actualColumn >= COL_OMIT_FIRST && actualColumn <= COL_OMIT_LAST))
                continue;

            if (const auto colInfo = columns->getColumn(actualColumn); colInfo)
            {
                if (colInfo->isSet)
                {
                    /* Output stream looks like this:
                    *
                    *  int16_t column
                    *  int16_t length
                    *  int64_t values[]
                    */

                    // write out column id
                    *reinterpret_cast<int16_t*>(write) = actualColumn;
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
                    cursor->columnNum = actualColumn;
                    cursor->val64 = r->cols[c];

                    write += sizeOfCast;
                    bytesNeeded += sizeOfCast;
                }
            }
        }

        cursor = recast<Cast_s*>(write); // END OF ROW - write a "row" marker at the end of the row
        cursor->columnNum = -1;

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

    if (rows.size() < static_cast<size_t>(table->eventMax) && rows[0]->cols[COL_STAMP] > Now() - table->eventTtl)
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
        if (r->cols[COL_STAMP] > cullStamp)
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

int Grid::getGridColumn(const int schemaColumn) const
{
    return colMap->reverseMap[schemaColumn];
}

bool Grid::isFullSchema() const
{
    return (colMap && colMap->hash == 0);
}

Grid::RowType_e Grid::insertParse(const std::string& event, Columns* columns, cjson* doc, Col_s* insertRow)
{
    auto hasEventAttributes = false;
    auto hasPropAttributes = false;

    if (event.length())
        doc->set("event", event);

    const auto attrColumns = doc->getNodes();

    for (auto c : attrColumns)
    {
        // look for the name (by hash) in the insertMap
        if (const auto iter = colMap->insertMap.find(MakeHash(c->name())); iter != colMap->insertMap.end())
        {
            const auto schemaCol = colMap->columnMap[iter->second];
            const auto colInfo = columns->getColumn(schemaCol);
            const auto col = iter->second;

            if (colInfo->isProp)
            {
                hasPropAttributes = true;
                continue;
            }

            hasEventAttributes = true;

            attributes->getMake(schemaCol, NONE);
            attributes->setDirty(this->rawData->linId, schemaCol, NONE);
            auto tempVal = NONE;
            string tempString;

            switch (c->type())
            {
            case cjson::Types_e::INT:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tempVal = c->getInt();
                    break;
                case columnTypes_e::doubleColumn:
                    tempVal = cast<int64_t>(c->getInt() * 10000LL);
                    break;
                case columnTypes_e::boolColumn:
                    tempVal = c->getInt() ? 1 : 0;
                    break;
                case columnTypes_e::textColumn:
                    tempString = to_string(c->getInt());
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::DBL:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tempVal = cast<int64_t>(c->getDouble());
                    break;
                case columnTypes_e::doubleColumn:
                    tempVal = cast<int64_t>(c->getDouble() * 10000LL);
                    break;
                case columnTypes_e::boolColumn:
                    tempVal = c->getDouble() != 0;
                    break;
                case columnTypes_e::textColumn:
                    tempString = to_string(c->getDouble());
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::STR:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn: case columnTypes_e::doubleColumn:
                    continue;
                case columnTypes_e::boolColumn:
                    tempVal = c->getString() != "0";
                    break;
                case columnTypes_e::textColumn:
                    tempString = c->getString();
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::BOOL:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tempVal = c->getBool() ? 1 : 0;
                    break;
                case columnTypes_e::doubleColumn:
                    tempVal = c->getBool() ? 10000 : 0;
                    break;
                case columnTypes_e::boolColumn:
                    tempVal = c->getBool();
                    break;
                case columnTypes_e::textColumn:
                    tempString = c->getBool() ? "true" : "false";
                    tempVal = MakeHash(tempString);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::ARRAY:
            {
                if (!colInfo->isSet)
                    continue;
                auto aNodes = c->getNodes();
                const auto startIdx = setData.size();
                for (auto n : aNodes)
                {
                    switch (n->type())
                    {
                    case cjson::Types_e::INT:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                            tempVal = n->getInt();
                            break;
                        case columnTypes_e::doubleColumn:
                            tempVal = cast<int64_t>(n->getInt() * 10000LL);
                            break;
                        case columnTypes_e::boolColumn:
                            tempVal = n->getInt() ? 1 : 0;
                            break;
                        case columnTypes_e::textColumn:
                            tempString = to_string(n->getInt());
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::DBL:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                            tempVal = cast<int64_t>(n->getDouble());
                            break;
                        case columnTypes_e::doubleColumn:
                            tempVal = cast<int64_t>(n->getDouble() * 10000LL);
                            break;
                        case columnTypes_e::boolColumn:
                            tempVal = n->getDouble() != 0;
                            break;
                        case columnTypes_e::textColumn:
                            tempString = to_string(n->getDouble());
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::STR:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn: case columnTypes_e::doubleColumn:
                            continue;
                        case columnTypes_e::boolColumn:
                            tempVal = n->getString() != "0";
                            break;
                        case columnTypes_e::textColumn:
                            tempString = n->getString();
                            tempVal = MakeHash(tempString);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::BOOL:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                            tempVal = n->getBool() ? 1 : 0;
                            break;
                        case columnTypes_e::doubleColumn:
                            tempVal = n->getBool() ? 10000 : 0;
                            break;
                        case columnTypes_e::boolColumn:
                            tempVal = n->getBool();
                            break;
                        case columnTypes_e::textColumn:
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

                    if (colInfo->type == columnTypes_e::textColumn)
                        attributes->getMake(schemaCol, tempString);
                    else
                        attributes->getMake(schemaCol, tempVal);

                    attributes->setDirty(this->rawData->linId, schemaCol, tempVal);
                    setData.push_back(tempVal);
                }

                // put value in row
                SetInfo_s info { static_cast<int>(setData.size() - startIdx), static_cast<int>(startIdx) };
                insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
            }
            default:
                continue;
            }

            // if it's pure prop, or it's not a prop at all, or it is a prop with an event
            // and this event is the same or more recent than the last prop in the dataset
            if (colInfo->type == columnTypes_e::textColumn)
                attributes->getMake(schemaCol, tempString);
            else
                attributes->getMake(schemaCol, tempVal);

            attributes->setDirty(this->rawData->linId, schemaCol, tempVal);
            if (colInfo->isSet)
            {
                SetInfo_s info { 1, static_cast<int>(setData.size()) };
                insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
                setData.push_back(tempVal);
            }
            else
            {
                insertRow->cols[col] = tempVal;
            }
        }
        else
        {
            // todo: do we care about non-mapped columns.
        }
    }

    if (hasPropAttributes)
    {
        auto insertProps = getProps(true);

        for (auto c : attrColumns)
        {
            // look for the name (by hash) in the insertMap
            if (const auto iter = colMap->insertMap.find(MakeHash(c->name())); iter != colMap->insertMap.end())
            {
                const auto schemaCol = colMap->columnMap[iter->second];
                const auto colInfo = columns->getColumn(schemaCol);
                const auto& colName = colInfo->name;

                if (!colInfo->isProp)
                    continue;

                switch (c->type())
                {
                case cjson::Types_e::INT:
                    switch (colInfo->type)
                    {
                    case columnTypes_e::intColumn:
                    case columnTypes_e::doubleColumn:
                        insertProps[colName] = c->getInt();
                        break;
                    case columnTypes_e::boolColumn:
                        insertProps[colName] = c->getInt() ? true : false;
                        break;
                    case columnTypes_e::textColumn:
                        insertProps[colName] = to_string(c->getInt());
                        break;
                    }
                    break;
                case cjson::Types_e::DBL:
                    switch (colInfo->type)
                    {
                    case columnTypes_e::intColumn:
                    case columnTypes_e::doubleColumn:
                        insertProps[colName] = c->getDouble();
                        break;
                    case columnTypes_e::boolColumn:
                        insertProps[colName] = c->getDouble() != 0 ? true : false;
                        break;
                    case columnTypes_e::textColumn:
                        insertProps[colName] = to_string(c->getDouble());
                        break;
                    }
                    break;
                case cjson::Types_e::STR:
                    switch (colInfo->type)
                    {
                    case columnTypes_e::intColumn:
                    case columnTypes_e::doubleColumn:
                        continue;
                    case columnTypes_e::boolColumn:
                        insertProps[colName] = c->getString() != "0";
                        break;
                    case columnTypes_e::textColumn:
                        insertProps[colName] = c->getString();
                        break;
                    }
                    break;
                case cjson::Types_e::BOOL:
                    switch (colInfo->type)
                    {
                    case columnTypes_e::intColumn:
                    case columnTypes_e::doubleColumn:
                        insertProps[colName] = c->getBool() ? 1 : 0;
                        break;
                    case columnTypes_e::boolColumn:
                        insertProps[colName] = c->getBool();
                        break;
                    case columnTypes_e::textColumn:
                        insertProps[colName] = c->getBool() ? "true" : "false";
                        break;
                    }
                    break;
                case cjson::Types_e::ARRAY:
                    {
                        if (!colInfo->isSet)
                            continue;

                        insertProps[colName].set();

                        auto aNodes = c->getNodes();
                        const auto startIdx = setData.size();
                        for (auto n : aNodes)
                        {
                            switch (n->type())
                            {
                            case cjson::Types_e::INT:
                                switch (colInfo->type)
                                {
                                case columnTypes_e::intColumn:
                                case columnTypes_e::doubleColumn:
                                    insertProps[colName] += n->getInt();
                                    break;
                                case columnTypes_e::boolColumn:
                                    insertProps[colName] += n->getInt() ? true : false;
                                    break;
                                case columnTypes_e::textColumn:
                                    insertProps[colName] += to_string(n->getInt());
                                    break;
                                }
                                break;
                            case cjson::Types_e::DBL:
                                switch (colInfo->type)
                                {
                                case columnTypes_e::intColumn:
                                case columnTypes_e::doubleColumn:
                                    insertProps[colName] += cast<int64_t>(n->getDouble());
                                    break;
                                case columnTypes_e::boolColumn:
                                    insertProps[colName] += n->getDouble() != 0;
                                    break;
                                case columnTypes_e::textColumn:
                                    insertProps[colName] += to_string(n->getDouble());
                                    break;
                                }
                                break;
                            case cjson::Types_e::STR:
                                switch (colInfo->type)
                                {
                                case columnTypes_e::intColumn:
                                case columnTypes_e::doubleColumn:
                                    continue;
                                case columnTypes_e::boolColumn:
                                    insertProps[colName] += n->getString() != "0";
                                    break;
                                case columnTypes_e::textColumn:
                                    insertProps[colName] += n->getString();
                                    break;
                                }
                                break;
                            case cjson::Types_e::BOOL:
                                switch (colInfo->type)
                                {
                                case columnTypes_e::intColumn:
                                case columnTypes_e::doubleColumn:
                                    insertProps[colName] += c->getBool() ? 1 : 0;
                                    break;
                                case columnTypes_e::boolColumn:
                                    insertProps[colName] += c->getBool();
                                    break;
                                case columnTypes_e::textColumn:
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

    if (hasPropAttributes && hasEventAttributes)
        return RowType_e::event_and_prop;
    if (hasPropAttributes)
        return RowType_e::prop;
    if (hasEventAttributes)
        return RowType_e::event;

    return RowType_e::junk;
}

void Grid::insertEvent(cjson* rowData)
{
    const auto attrNode = rowData->xPath("/_");

    if (!attrNode)
        return;

    const auto stampNode = rowData->xPath("/stamp");
    const auto event = rowData->xPathString("/event", "");

    const auto insertRow = newRow();
    const auto columns = table->getColumns();

    // parse the event (columns & props)
    const auto insertType = insertParse(event, columns, attrNode, insertRow);

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

    insertRow->cols[COL_STAMP] = stamp;

    auto rowCount = rows.size();
    const auto lastRowStamp = rowCount ? rows.back()->cols[COL_STAMP] : 0;

    decltype(newRow()) row = nullptr;

    const auto getRowHash = [&](Col_s* rowPtr) -> int64_t
    {
        auto hash = rowPtr->cols[COL_STAMP];
        for (auto col = COL_FIRST_USER_DATA; col < colMap->columnCount; ++col)
        {
            if (rowPtr->cols[col] == NONE)
                continue;

            const auto colInfo = columns->getColumn(colMap->columnMap[col]);

            // don't count deleted columns or props
            if (!colInfo || colInfo->deleted || colInfo->isProp)
                continue;

            if (colInfo->isSet)
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

    const auto hashedAction = MakeHash(event);
    const auto zOrderInts = table->getZOrderHashes();
    const auto getZOrder = [&](int64_t value) -> int
    {
        const auto iter = zOrderInts->find(value);
        if (iter != zOrderInts->end())
            return (*iter).second;
        return 99;
    };
    const auto insertZOrder = getZOrder(hashedAction);

    // lambda using binary search to find insert index
    const auto findInsert = [&]() -> int
    {
        auto first = 0;
        auto last = static_cast<int>(rowCount - 1);
        auto mid = last >> 1;
        while (first <= last)
        {
            if (stamp > rows[mid]->cols[COL_STAMP])
                first = mid + 1; // search bottom of list
            else if (stamp < rows[mid]->cols[COL_STAMP])
                last = mid - 1; // search top of list
            else
                return mid;
            mid = (first + last) >> 1; // usually written like first + ((last - first) / 2)
        }
        return -(first + 1);
    };

    auto i = rowCount ? findInsert() : 0;
    if (i < 0) // negative value (made positive - 1) is the insert position
        i = -i - 1;

    if (i != static_cast<int>(rowCount)) // if they are equal skip all this, we are appending
    {
        // walk back to the beginning of all rows sharing this time stamp
        if (rowCount)
        {
            while (i > 0 && rows[i]->cols[COL_STAMP] == stamp)
                --i; // walk forward to find our insertion point
        }

        for (; i < static_cast<int>(rowCount); i++)
        {
            // we have found rows with same stamp
            if (rows[i]->cols[0] == stamp)
            {
                auto zOrder = getZOrder(rows[i]->cols[COL_EVENT]);
                // we have found rows in this stamp with the same zOrder
                if (zOrder == insertZOrder)
                {
                    // look this date range and zorder to see if we have a row group
                    // match (as in, we are replacing a row)
                    for (; i < static_cast<int>(rowCount); i++)
                    {
                        zOrder = getZOrder(rows[i]->cols[COL_EVENT]);

                        // we have moved passed replaceable rows, so insert here
                        if (rows[i]->cols[COL_STAMP] > stamp || zOrder > insertZOrder)
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
            else if (rows[i]->cols[COL_STAMP] > stamp)
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
