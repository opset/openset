#include "grid.h"
#include "table.h"
#include "lz4.h"
#include "time/epoch.h"
#include "sba/sba.h"

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
        if (const auto iter = before.find({column, value}); iter != before.end())
            iter->second++;
        else
            before[{column, value}] = 1;
    }
    else
    {
        if (const auto iter = after.find({column, value}); iter != after.end())
            iter->second++;
        else
            after[{column, value}] = 1;
    }

    // a Value of NONE in combination with a column indicates that
    // the column is referenced. This is used to index a column, rather
    // than a column and value.
    if (value != NONE)
        add(column, NONE, mode);
}

void IndexDiffing::add(Grid* grid, Mode_e mode)
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
            if (r->cols[c] == NONE ||
                (actualColumn >= COL_OMIT_FIRST && actualColumn <= COL_OMIT_LAST))
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

IndexDiffing::CVList IndexDiffing::getRemoved()
{
    CVList result;

    for (auto& b : before)
        if (!after.count(b.first))
            result.push_back(b.first);

    return result;
}

IndexDiffing::CVList IndexDiffing::getAdded()
{
    CVList result;

    for (auto& a : after)
        if (!before.count(a.first))
            result.push_back(a.first);

    return result;
}

void openset::db::IndexDiffing::iterRemoved(const std::function<void(int32_t,int64_t)> cb)
{
    for (auto& a : after)
        if (!before.count(a.first))
            cb(a.first.first, a.first.second);
}

Grid::~Grid()
{
    if (colMap && table)
        table->getColumnMapper()->releaseMap(colMap);
}

void Grid::reset()
{
	rows.clear(); // release the rows - likely to not free vector internals
	mem.reset(); // release the memory to the pool - will always leave one page
	rawData = nullptr;
}

void Grid::reinit()
{
	reset();
	table = nullptr;
	blob = nullptr;
	attributes = nullptr;
	//memset(columnMap, 0, sizeof(columnMap)); // all zeros
	//memset(isSet, 0, sizeof(isSet)); // all false
}

/**
 * \brief maps schema to the columnMap
 *
 * Why? The schema can have up to 8192 columns. These columns have 
 * numeric indexes that allow allocated columns to be distributed 
 * throughout that range. The Column map is a sequential list of 
 * indexes into the actual schema, allowing us to create compact 
 * grids that do not contain 8192 columns (which would be bulky 
 * and slow)
 */
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

AttributeBlob* Grid::getAttributeBlob() const
{
	return attributes->blob;
}

cjson Grid::toJSON() const
{
	cjson doc;

	doc.set("id_string", this->rawData->getIdStr());
	doc.set("id_key", this->rawData->id);

	auto rowDoc = doc.setArray("rows");
	auto columns = table->getColumns();

    std::vector<openset::db::Col_s*> accumulator;

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

	for (auto iter = rows.begin(); iter != rows.end(); ++iter)
	{
		accumulator.push_back(*iter);

		// if we are at the end or the next one is a different group, lets push 
		// out some JSON 
		if (iter + 1 == rows.end() || 
			HashPair((*(iter + 1))->cols[COL_STAMP], (*(iter + 1))->cols[COL_ACTION]) !=
			HashPair( (*iter)->cols[COL_STAMP], (*iter)->cols[COL_ACTION]))
		{
			// just one row? easy!
			if (accumulator.size() == 1)
			{
				auto rootObj = rowDoc->pushObject();
				rootObj->set("stamp", (*iter)->cols[COL_STAMP]);
				rootObj->set("stamp_iso", Epoch::EpochToISO8601((*iter)->cols[COL_STAMP]));
				rootObj->set("action", attributes->blob->getValue(COL_ACTION, (*iter)->cols[COL_ACTION]));
			    auto rowObj = rootObj->setObject("attr");

				for (auto c = 0; c < colMap->columnCount; ++c)
				{
					// get the column information
					const auto colInfo = columns->getColumn(colMap->columnMap[c]);

					if (colInfo->idx < 1000) // first 1000 are reserved
						continue;

					const auto value = accumulator[0]->cols[c];

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
            else // more than one row... kindof annoying
            {
                // pair is pair<column,value>, .second is count
                unordered_map<std::pair<int64_t, int64_t>, int64_t> counts;

                auto rootObj = rowDoc->pushObject();
                rootObj->set("stamp_iso", Epoch::EpochToISO8601((*iter)->cols[COL_STAMP]));
                rootObj->set("stamp", (*iter)->cols[COL_STAMP]);
                rootObj->set("action", attributes->blob->getValue(COL_ACTION, (*iter)->cols[COL_ACTION]));

                // values that are the same on all rows get put under "attr":{}
                const auto attrObj = rootObj->setObject("attr");
                
                // every combination of column:value counted
                for (auto r : accumulator)
                    for (auto c = 0; c < colMap->columnCount; ++c)
                        if (r->cols[c] != NONE)
                        {
                            const auto key = make_pair(c, r->cols[c]);
                            if (!counts.count(key))
                                counts[key] = 1;
                            else
                                counts[key]++;
                        }

                // these values are the same on all rows in the rowset
                // these will end up in the root
                unordered_set<int64_t> sameOnAllRows;
                for (auto &c : counts)
                {
                    if (c.second == accumulator.size())
                        sameOnAllRows.emplace(c.first.first);
                }

                // this is the number of variations (in value) a column has
                unordered_map<int64_t, int> columnCountMap; 
                for (auto &c : counts)
                {                    
                    if (auto found = columnCountMap.find(c.first.first); found != columnCountMap.end())
                        ++found->second;
                    else
                        columnCountMap[c.first.first] = 1;
                }

                // this counts the size of common groups in the rewset, for example
                // product_name and product_price both occur 3 times, the set size is 2 (columns)
                unordered_map<int64_t, int> setSizes;
                for (auto &cc : columnCountMap)
                {
                    if (auto found = setSizes.find(cc.second); found != setSizes.end())
                        ++found->second;
                    else
                        setSizes[cc.second] = 1;
                }

                // this fills the sorted vector with column id and size of it's set,
                // ideally we want things that are most together at the front, and things
                // that are highly variant on the end. We fudge the "sameOnAllRows" columns
                // to sort to the front
                vector<std::pair<int64_t, int>> sorted;
                for (auto &cc : columnCountMap)
                {
                    std::pair<int64_t, int> value{ cc.first, 0 };
                    if (const auto t = setSizes.find(cc.second); t != setSizes.end())
                        value.second = t->second;
                    if (sameOnAllRows.count(cc.first))
                       value.second = 999999;

                    sorted.push_back(value);
                }

                // sort the list
                sort(sorted.begin(), sorted.end(), [](const auto& a, const auto &b) {
                    return a.second > b.second;                   
                });


                auto setKeyValue = [&](cjson* current, Columns::Columns_s* colInfo, int64_t value)
                {
                    switch (colInfo->type)
                    {
                    case columnTypes_e::freeColumn:
                        return;
                    case columnTypes_e::intColumn:
                        current->set(colInfo->name, value);
                        break;
                    case columnTypes_e::doubleColumn:
                        current->set(colInfo->name, value / 10000.0);
                        break;
                    case columnTypes_e::boolColumn:
                        current->set(colInfo->name, value != 0);
                        break;
                    case columnTypes_e::textColumn:
                    {
                        // value here is the hashed value							
                        if (const auto text = attributes->blob->getValue(colInfo->idx, value); text)
                            current->set(colInfo->name, text);
                    }
                    break;
                    }
                };

                auto isKeyValue = [&](cjson* current, Columns::Columns_s* colInfo, int64_t value)->bool
                {
                    if (auto t = current->find(colInfo->name); !t)
                        return false;
                    else
                        switch (colInfo->type)
                        {
                        case columnTypes_e::freeColumn:
                            return false;
                        case columnTypes_e::intColumn:
                            return (t->getInt() == value);
                        case columnTypes_e::doubleColumn:
                            return (t->getDouble() == value / 10000.0);
                        case columnTypes_e::boolColumn:
                            return (t->getBool() == (value != 0));
                        case columnTypes_e::textColumn:
                            // value here is the hashed value							
                            if (const auto text = attributes->blob->getValue(colInfo->idx, value); text)
                                return (t->getString() == string{ text });
                            else
                                return false;
                        }
                    return false;
                };


                cjson branch;

                // This will make an overly verbose tree, there will be way more branches than
                // required. The recurive `fold` and `tailCondense` lambdas will 
                // clean this up.
                for (auto row : accumulator)
                {
                    auto current = &branch;

                    for (auto &c : sorted)
                    {
                        const auto ac = c.first; // actual column
                        const auto colInfo = columns->getColumn(colMap->columnMap[ac]);

                        if (colInfo->idx < 1000) // first 1000 are reserved
                            continue;

                        const auto value = row->cols[ac];

                        if (const auto t = current->find("_"); t)
                            current = t->membersHead;
                        else
                        {
                            current = current->setArray("_");
                            current = current->pushObject();
                        }

                        auto nodes = current->parentNode->getNodes();

                        // this "_" array has no objects?
                        cjson* foundNode = nullptr;
                        cjson* notFoundNode = nullptr;
                        for (auto n : nodes)
                        {
                            if (isKeyValue(n, colInfo, value))
                            {
                                foundNode = n;
                                break;
                            }
                            if (!n->find(colInfo->name))
                                notFoundNode = n;
                        }

                        if (foundNode)
                            current = foundNode;
                        else
                        {                           
                            if (notFoundNode)
                                current = notFoundNode;
                            else
                                current = current->parentNode->pushObject();
                            
                            setKeyValue(current, colInfo, value);
                        }

                    }
                }

                // this will clean lists of end-nodes (leafs) that
                // contain just one value by turning them into arrays
                function<void(cjson*)> tailCondense;
                tailCondense = [&tailCondense](cjson* current)
                {

                    if (!current)
                        return;

                    auto nodes = current->getNodes();

                    unordered_set<string> propCounter;

                    for (auto n: nodes)
                    {
                        if (auto tNode = n->find("_"); tNode)
                            tailCondense(tNode);
                        auto props = n->getNodes();
                        for (auto p : props)
                            if (p->name().length())
                                propCounter.emplace(p->name());
                    }

                    if (propCounter.size() == 1)
                    {
                        auto propName = *propCounter.begin();
                        auto newNode = current->parentNode->setArray(propName);
                        
                        for (auto n : nodes)
                        {
                            auto props = n->getNodes();
                            for (auto p : props)
                            {
                                if (p->name() == propName)
                                {
                                    switch (p->type())
                                    {
                                        case cjson::Types_e::VOIDED: break;
                                        case cjson::Types_e::NUL: break;
                                        case cjson::Types_e::OBJECT: break;
                                        case cjson::Types_e::ARRAY: break;
                                        case cjson::Types_e::INT: 
                                            newNode->push(p->getInt());
                                        break;
                                        case cjson::Types_e::DBL: 
                                            newNode->push(p->getDouble());
                                        break;
                                        case cjson::Types_e::STR: 
                                            newNode->push(p->getString());
                                        break;
                                        case cjson::Types_e::BOOL: 
                                            newNode->push(p->getBool());
                                        break;
                                        default: ;
                                    }
                                }
                            }                            
                        }

                        current->removeNode();
                    }

                };

                // this will remove all the extra nodes by packing down
                // nodes with just one member into their parent recursively until
                // it's done.
                function<void(cjson*)> fold;
                fold = [&fold](cjson* current)
                {

                    if (!current)
                        return;

                    auto nodes = current->getNodes();

                    for (auto n : nodes)
                    {
                        if (auto tNode = n->find("_"); tNode)
                            fold(tNode);
                    }

                    if (current->memberCount == 1)
                    {                       
                        auto members = current->membersHead->getNodes(); // array then object

                        current->parentNode->find("_")->removeNode();
                       
                        for (auto m: members)
                            current->parentNode->push(m);
                    }                                                

                };

                // fold and condense
                fold(branch.find("_"));
                tailCondense(branch.find("_"));

                // this Parses the `branch` document into the attributes node using
                // using Stringify (not great, but not overly slow)
                cjson::parse(cjson::stringify(&branch), attrObj, true);

			}

			accumulator.clear();
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

	const auto output = cast<char*>(PoolMem::getPool().getPtr(rawData->bytes));
	LZ4_decompress_fast(rawData->getComp(), output, rawData->bytes);

	// make a blank row
	auto row = newRow();

	// read pointer - will increment through the compacted set
	auto read = output;
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
			}
			
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
                SetInfo_s info{ count, static_cast<int>(startIdx) };
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

	PoolMem::getPool().freePtr(output);
}

PersonData_s* Grid::addFlag(const flagType_e flagType, const int64_t reference, const int64_t context, const int64_t value)
{
	int idx;
	Flags_s* newFlags;

	if (!rawData->flagRecords)
	{
		newFlags = recast<Flags_s*>(PoolMem::getPool().getPtr(sizeof(Flags_s)));
		idx = 0;
	}
	else
	{
		// copy the old flags to the new flags
		newFlags = recast<Flags_s*>(PoolMem::getPool().getPtr(sizeof(Flags_s) * (rawData->flagRecords + 1)));
		memcpy(newFlags, rawData->getFlags(), sizeof(Flags_s) * rawData->flagRecords);

		// index is count
		idx = rawData->flagRecords;
	}

	// insert our new flag
	newFlags[idx].set(flagType, reference, context, value);

	const auto newFlagRecords = rawData->flagRecords + 1;
	const auto oldFlagBytes = rawData->flagBytes();
	const auto newFlagBytes = newFlagRecords * sizeof(Flags_s);

	const auto newPersonSize = rawData->size() - oldFlagBytes + newFlagBytes;
	const auto newPerson = recast<PersonData_s*>(PoolMem::getPool().getPtr(newPersonSize));

	// copy old header
	memcpy(newPerson, rawData, sizeof(PersonData_s));
	newPerson->flagRecords = static_cast<int16_t>(newFlagRecords); // adjust offsets in new person

	// copy old id bytes
	if (rawData->idBytes)
		memcpy(newPerson->getIdPtr(), rawData->getIdPtr(), static_cast<size_t>(rawData->idBytes));
	// copy NEW flags
	memcpy(newPerson->getFlags(), newFlags, newFlagBytes);
	// copy old props
	if (rawData->setBytes)
		memcpy(newPerson->getSets(), rawData->getSets(), static_cast<size_t>(rawData->setBytes));
	// copy old compressed events
	if (rawData->comp)
		memcpy(newPerson->getComp(), rawData->getComp(), static_cast<size_t>(rawData->comp));
	
	PoolMem::getPool().freePtr(newFlags);

	// release the original
	PoolMem::getPool().freePtr(rawData);

	rawData = newPerson;

	return newPerson;
}

PersonData_s* Grid::clearFlag(const flagType_e flagType, const int64_t reference, const int64_t context)
{
	if (!rawData->flagRecords)
		return rawData;

	auto found = false;
    const auto start = rawData->getFlags();
    const auto end = start + rawData->flagRecords;
    for (auto iter = start; iter < end; ++iter)
	{
		if (iter->flagType == flagType && 
			iter->reference == reference &&
			iter->context == context)
			found = true;
	}

	if (!found)
		return rawData;

	// copy flags over to new structure, skip the one we are omitting
	const auto newFlags = recast<Flags_s*>(PoolMem::getPool().getPtr(rawData->flagBytes() - sizeof(Flags_s)));

    // TODO - remove redundant buffer
    const auto newFlagRecords = rawData->flagRecords - 1; 
	auto writer = newFlags;
	for (auto iter = start; iter < end; ++iter)
	{
		if (iter->flagType == flagType &&
			iter->reference == reference &&
			iter->context == context)
			continue;
		*writer = *iter;
		++writer;
	}

	const auto oldFlagBytes = rawData->flagBytes();
	const auto newFlagBytes = newFlagRecords * sizeof(Flags_s);

	const auto newPersonSize = rawData->size() - oldFlagBytes + newFlagBytes;
	const auto newPerson = recast<PersonData_s*>(PoolMem::getPool().getPtr(newPersonSize));

	// copy old header
	memcpy(newPerson, rawData, sizeof(PersonData_s));
	newPerson->flagRecords = static_cast<int16_t>(newFlagRecords); // adjust offsets in new person

	// copy old id bytes											 
	if (rawData->idBytes) 
		memcpy(newPerson->getIdPtr(), rawData->getIdPtr(), static_cast<size_t>(rawData->idBytes));
	// copy NEW flags
	if (newFlagBytes)
		memcpy(newPerson->getFlags(), newFlags, newFlagBytes);
	// copy old props
	if (rawData->setBytes)
		memcpy(newPerson->getSets(), rawData->getSets(), static_cast<size_t>(rawData->setBytes));
	// copy old compressed events
	if (rawData->comp)
		memcpy(newPerson->getComp(), rawData->getComp(), static_cast<size_t>(rawData->comp));

	PoolMem::getPool().freePtr(newFlags);

	// release the original
	PoolMem::getPool().freePtr(rawData);

	rawData = newPerson;

	return newPerson;
}

PersonData_s* Grid::commit()
{
	if (!rows.size())
	{
		cout << "no rows" << endl;
	}
    
    // this is the worst case scenario temp buffer size for this data.
    // (columns * rows) + (columns * row headers) + number_of_set_values
    const auto tempBufferSize = 
        (rows.size() * (colMap->columnCount * sizeOfCast)) + 
        (rows.size() * sizeOfCastHeader) + 
        (setData.size() * sizeof(int64_t)) + // the set data
        ((rows.size() * colMap->columnCount) * (sizeOfCastHeader + sizeof(int32_t))); // the NONES at the end of the list

	// make an intermediate buffer that is fully uncompresed
	const auto intermediateBuffer = recast<char*>(PoolMem::getPool().getPtr(tempBufferSize));

	// copy over header and ID text
	//memcpy(intermediateBuffer, rawData, sizeof(PersonData_s) + rawData->idBytes);

	auto write = intermediateBuffer;
	Cast_s* cursor;
    auto bytesNeeded = 0;

    auto columns = table->getColumns();

	for (auto r : rows)
	{
		for (auto c = 0; c < colMap->columnCount; ++c)
		{
            const auto actualColumn = colMap->columnMap[c];

			// skip NONE values, placeholder (non-event) columns and auto-generated columns (like session)
			if (r->cols[c] == NONE ||
				(actualColumn >= COL_OMIT_FIRST && actualColumn <= COL_OMIT_LAST))
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
                    bytesNeeded += sizeof(int16_t);

                    // write out values
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

		cursor = recast<Cast_s*>(write);

		// END OF ROW - write a "row" marker at the end of the row
		cursor->columnNum = -1;
		write += sizeOfCastHeader;
        bytesNeeded += sizeOfCastHeader;
	}
        
	const auto maxBytes = LZ4_compressBound(bytesNeeded);
	const auto compBuffer = cast<char*>(PoolMem::getPool().getPtr(maxBytes));

	const auto oldCompBytes = rawData->comp;
	const auto newCompBytes = LZ4_compress_fast(
		intermediateBuffer, 
		compBuffer, 
		bytesNeeded,
		maxBytes,
		4);

	const auto newPersonSize = (rawData->size() - oldCompBytes) + newCompBytes; // size() includes data, we adjust
	const auto newPerson = recast<PersonData_s*>(PoolMem::getPool().getPtr(newPersonSize));

	// copy old header
	memcpy(newPerson, rawData, sizeof(PersonData_s));
	newPerson->comp = newCompBytes; // adjust offsets
	newPerson->bytes = bytesNeeded;
									
	// copy old id bytes											 
	if (rawData->idBytes)
		memcpy(newPerson->getIdPtr(), rawData->getIdPtr(), static_cast<size_t>(rawData->idBytes));
	// copy NEW flags
	if (rawData->flagRecords)
	memcpy(newPerson->getFlags(), rawData->getFlags(), static_cast<size_t>(rawData->flagBytes()));
	// copy old props
	if (rawData->setBytes)
		memcpy(newPerson->getSets(), rawData->getSets(), static_cast<size_t>(rawData->setBytes));
	// copy NEW compressed events
	if (newCompBytes)
		memcpy(newPerson->getComp(), compBuffer, static_cast<size_t>(newCompBytes));

	// get rid of the intermediate copy
	PoolMem::getPool().freePtr(intermediateBuffer);
	PoolMem::getPool().freePtr(compBuffer);
	
	// release the original
	PoolMem::getPool().freePtr(rawData);

	// it probably got longer!
	rawData = newPerson;
	
	return rawData;
}

bool Grid::cull()
{
    // empty? no cull
    if (rows.empty())
        return false;

    // not at row limit, and first event is within time window? no cull
    if (rows.size() < static_cast<size_t>(table->eventMax) && 
        rows[0]->cols[COL_STAMP] > Now() - table->eventTtl)
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

    for (const auto &r: rows)
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
    //const auto noLongerReferenced = diff.getRemoved();

    //for (const auto& cv: noLongerReferenced)
    diff.iterRemoved([&](int32_t col, int64_t val) {
        attributes->setDirty(this->rawData->linId, col, val, false); 
    });

    //if (!noLongerReferenced.empty())
      //  cout << ("removed " + to_string(noLongerReferenced.size())) << endl;
    
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

void Grid::insert(cjson* rowData)
{
	// ensure we have ms on the time stamp
	const auto stampNode = rowData->xPath("/stamp");

	if (!stampNode)
		return;

	const auto stamp = (stampNode->type() == cjson::Types_e::STR) ?
		Epoch::fixMilli(Epoch::ISO8601ToEpoch(stampNode->getString())) :
		Epoch::fixMilli(stampNode->getInt());

	if (stamp < 0)
		return;

	const auto action = rowData->xPathString("/action", "");
	const auto attrNode = rowData->xPath("/_");
	
	decltype(newRow()) row = nullptr;

	if (!attrNode || !action.length())
		return;

    // over row limit, and first event older than time window? cull
    // note - added leway to reduce calls, proper culling happens
    // in in oloop_cleaner hourly.
    if (rows.size() > static_cast<size_t>(table->eventMax + (table->eventMax * 0.1)) && 
        rows[0]->cols[COL_STAMP] < Now() - (table->eventTtl + 3'600'000))
        cull();

    auto rowCount = rows.size();

	// move the action into the attrs so it will be integrated into the row set
	attrNode->set("__action", action);
	auto columns = table->getColumns();

    const auto insertRow = newRow();

    insertRow->cols[COL_STAMP] = stamp;

    const auto attrColumns = attrNode->getNodes();

    for (auto c : attrColumns) // columns in row
    {
        // look for the name (by hash) in the insertMap
        if (const auto iter = colMap->insertMap.find(MakeHash(c->name())); iter != colMap->insertMap.end())
        {
            const auto schemaCol = colMap->columnMap[iter->second];            
            const auto colInfo = columns->getColumn(schemaCol);
            const auto col = iter->second;

            attributes->getMake(schemaCol, NONE);
            attributes->setDirty(this->rawData->linId, schemaCol, NONE);

            auto tval = NONE;
            string tstr;

            switch (c->type())
            {
            case cjson::Types_e::INT:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tval = c->getInt();
                    break;
                case columnTypes_e::doubleColumn:
                    tval = cast<int64_t>(c->getInt() * 10000LL);
                    break;
                case columnTypes_e::boolColumn:
                    tval = c->getInt() ? 1 : 0;
                    break;
                case columnTypes_e::textColumn:
                    tstr = to_string(c->getInt());
                    tval = MakeHash(tstr);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::DBL:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tval = cast<int64_t>(c->getDouble());
                    break;
                case columnTypes_e::doubleColumn:
                    tval = cast<int64_t>(c->getDouble() * 10000LL);
                    break;
                case columnTypes_e::boolColumn:
                    tval = c->getDouble() != 0;
                    break;
                case columnTypes_e::textColumn:
                    tstr = to_string(c->getDouble());
                    tval = MakeHash(tstr);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::STR:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                case columnTypes_e::doubleColumn:
                    continue;
                case columnTypes_e::boolColumn:
                    tval = c->getString() != "0";
                    break;
                case columnTypes_e::textColumn:
                    tstr = c->getString();
                    tval = MakeHash(tstr);
                    break;
                default:
                    continue;
                }
                break;
            case cjson::Types_e::BOOL:
                switch (colInfo->type)
                {
                case columnTypes_e::intColumn:
                    tval = c->getBool() ? 1 : 0;
                    break;
                case columnTypes_e::doubleColumn:
                    tval = c->getBool() ? 10000 : 0;
                    break;
                case columnTypes_e::boolColumn:
                    tval = c->getBool();
                    break;
                case columnTypes_e::textColumn:
                    tstr = c->getBool() ? "true" : "false";
                    tval = MakeHash(tstr);
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
                            tval = n->getInt();
                            break;
                        case columnTypes_e::doubleColumn:
                            tval = cast<int64_t>(n->getInt() * 10000LL);
                            break;
                        case columnTypes_e::boolColumn:
                            tval = n->getInt() ? 1 : 0;
                            break;
                        case columnTypes_e::textColumn:
                            tstr = to_string(n->getInt());
                            tval = MakeHash(tstr);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::DBL:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                            tval = cast<int64_t>(n->getDouble());
                            break;
                        case columnTypes_e::doubleColumn:
                            tval = cast<int64_t>(n->getDouble() * 10000LL);
                            break;
                        case columnTypes_e::boolColumn:
                            tval = n->getDouble() != 0;
                            break;
                        case columnTypes_e::textColumn:
                            tstr = to_string(n->getDouble());
                            tval = MakeHash(tstr);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::STR:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                        case columnTypes_e::doubleColumn:
                            continue;
                        case columnTypes_e::boolColumn:
                            tval = n->getString() != "0";
                            break;
                        case columnTypes_e::textColumn:
                            tstr = n->getString();
                            tval = MakeHash(tstr);
                            break;
                        default:
                            continue;
                        }
                        break;
                    case cjson::Types_e::BOOL:
                        switch (colInfo->type)
                        {
                        case columnTypes_e::intColumn:
                            tval = n->getBool() ? 1 : 0;
                            break;
                        case columnTypes_e::doubleColumn:
                            tval = n->getBool() ? 10000 : 0;
                            break;
                        case columnTypes_e::boolColumn:
                            tval = n->getBool();
                            break;
                        case columnTypes_e::textColumn:
                            tstr = n->getBool() ? "true" : "false";
                            tval = MakeHash(tstr);
                            break;
                        default:
                            continue;
                        }
                        break;
                    default:
                        continue;
                    }

                    if (colInfo->type == columnTypes_e::textColumn)
                        attributes->getMake(schemaCol, tstr);
                    else
                        attributes->getMake(schemaCol, tval);
                    attributes->setDirty(this->rawData->linId, schemaCol, tval);

                    setData.push_back(tval);
                }

                // let our row use an encoded value for the column.
                SetInfo_s info{ static_cast<int>(setData.size() - startIdx), info.offset = startIdx };
                insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
            }
            continue; // special handler for ARRAY

            default:
                tval = NONE;
                break;
            }

            if (colInfo->type == columnTypes_e::textColumn)
                attributes->getMake(schemaCol, tstr);
            else
                attributes->getMake(schemaCol, tval);
            attributes->setDirty(this->rawData->linId, schemaCol, tval);

            if (colInfo->isSet)
            {
                SetInfo_s info{ 1, static_cast<int>(setData.size()) };
                insertRow->cols[col] = *reinterpret_cast<int64_t*>(&info);
                setData.push_back(tval);
            }
            else
            {
                insertRow->cols[col] = tval;
            }
        }
        else
        {
            // todo: do we care about non-mapped columns.
        }
    }

    const auto getRowHash = [&](Col_s* rowPtr) -> int64_t
    {
        int64_t hash = 0;
        for (auto col = 0; col < colMap->columnCount; col++)
        {
            if (rowPtr->cols[col] == NONE)
                continue;

            const auto colInfo = columns->getColumn(colMap->columnMap[col]);

            if (colInfo->deleted)
                continue;

            if (colInfo->isSet)
            {
                const auto& ol = *reinterpret_cast<SetInfo_s*>(&rowPtr->cols[col]);
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

	const auto hashedAction = MakeHash(action);
	//const auto insertRowGroup = HashPair(stamp, hashedAction); 

	const auto zOrderInts = table->getZOrderHashes();

	const auto getZOrder = [&](int64_t value) -> int {
		auto iter = zOrderInts->find(value);

		if (iter != zOrderInts->end())
			return (*iter).second;

		return 99;
	};

	const auto insertZOrder = getZOrder(hashedAction);

	const auto findInsert = [&]() -> int {
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
			while (i > 0 && rows[i]->cols[COL_STAMP] == stamp)
				--i;

		// walk forward to find our insertion point
		for (; i < static_cast<int>(rowCount); i++)
		{
			// we have found rows with same stamp
			if (rows[i]->cols[0] == stamp)
			{
				auto zOrder = getZOrder(rows[i]->cols[COL_ACTION]);				

				// we have found rows in this stamp with the same zOrder
				if (zOrder == insertZOrder)
				{
					// look this date range and zorder to see if we have a row group
					// match (as in, we are replacing a row)
					for (; i < static_cast<int>(rowCount); i++)
					{
						zOrder = getZOrder(rows[i]->cols[COL_ACTION]);

						// we have moved passed replacable rows, so insert here
						if (rows[i]->cols[COL_STAMP] > stamp ||
							zOrder > insertZOrder)
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

