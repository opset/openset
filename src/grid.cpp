#include "grid.h"
#include "table.h"
#include "lz4.h"
#include "time/epoch.h"
#include "sba/sba.h"

using namespace openset::db;

Grid::Grid()
{
	memset(columnMap, 0, sizeof(columnMap)); // all zeros
	memset(isSet, 0, sizeof(isSet)); // all false
	rows.reserve(10000);
}

Grid::~Grid()
{}

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
	memset(columnMap, 0, sizeof(columnMap)); // all zeros
	memset(isSet, 0, sizeof(isSet)); // all false
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

	fullSchemaMap = true;

	table = tablePtr;
	attributes = attributesPtr;

	if (!table || !attributes)
		return false;

	// map the attributes blob (where text and things go) to this object
	blob = attributes->getBlob();

	// negative one fill these as -1 means no mapping
	for (auto& i : columnMap)
		i = -1;

	for (auto& i : reverseMap)
		i = -1;

	columnCount = 0;

	insertMap.clear();

	for (auto& s : table->getColumns()->columns)
		if (s.type != columnTypes_e::freeColumn)
		{
			if (s.idx == COL_UUID)
				uuidColumn = columnCount;
			else if (s.idx == COL_SESSION)
				sessionColumn = columnCount;

			columnMap[columnCount] = static_cast<int16_t>(s.idx); // maps local column to schema
			reverseMap[s.idx] = static_cast<int16_t>(columnCount); // maps schema to local column
			insertMap[MakeHash(s.name)] = columnCount; // maps to local column
			isSet[columnCount] = true;

			++columnCount;
		}

	rowBytes = columnCount * 8LL;

	return true;
}

bool Grid::mapSchema(Table* tablePtr, Attributes* attributesPtr, const vector<string>& columnNames)
{
	// if we are already mapped on this object, skip all this
	if (tablePtr && table && tablePtr->getName() == table->getName())
		return true;

	fullSchemaMap = false;

	table = tablePtr;
	attributes = attributesPtr;

	if (!table || !attributes || !columnNames.size())
		return false;

	// map the attributes blob (where text and things go) to this object
	blob = attributes->getBlob();

	// negative one fill these as -1 means no mapping
	for (auto& i : columnMap)
		i = -1;

	for (auto& i : reverseMap)
		i = -1;

	columnCount = 0;

	auto schema = table->getColumns();

	for (const auto colName: columnNames)
	{
		const auto s = schema->getColumn(colName);

		if (!s)
			return false;

		if (s->idx == COL_UUID)
			uuidColumn = columnCount;
		else if (s->idx == COL_SESSION)
			sessionColumn = columnCount;

		columnMap[columnCount] = static_cast<int16_t>(s->idx); // maps local column to schema
		reverseMap[s->idx] = static_cast<int16_t>(columnCount); // maps schema to local column

		++columnCount;
	}

	rowBytes = columnCount * 8LL;

	return true;
}

AttributeBlob* Grid::getAttributeBlob() const
{
	return attributes->blob;
}

cjson Grid::toJSON(const bool condensed) const
{
	cjson doc;

	doc.set("id_string", this->rawData->getIdStr());
	doc.set("id_key", this->rawData->id);

	auto rowDoc = doc.setArray("rows");

	auto columns = table->getColumns();
	cjson* rowObj;

	std::vector<openset::db::Col_s*> accumulator;

	for (auto iter = rows.begin(); iter != rows.end(); ++iter)
	{
		accumulator.push_back(*iter);

		// if we are at the end or the next one is a different group, lets push 
		// out some JSON snots
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
				rowObj = rootObj->setObject("attr");

				for (auto c = 0; c < columnCount; ++c)
				{
					// get the column information
					const auto colInfo = columns->getColumn(columnMap[c]);

					if (colInfo->idx < 1000) // first 1000 are reserved
						continue;

					const auto value = accumulator[0]->cols[c];

					if (value == NONE)
						continue;

					switch (colInfo->type)
					{
					case columnTypes_e::freeColumn:
						continue;
					case columnTypes_e::intColumn:
						rowObj->set(colInfo->name, value);
						break;
					case columnTypes_e::doubleColumn:
						rowObj->set(colInfo->name, value * 10000.0);
						break;
					case columnTypes_e::boolColumn:
						rowObj->set(colInfo->name, value ? true : false);
						break;
					case columnTypes_e::textColumn:
					{						
						if (const auto text = attributes->blob->getValue(colInfo->idx, value); text)
							rowObj->set(colInfo->name, text);
					}
					break;
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
				cjson* attrObj;
				
				if (condensed)
					attrObj = rootObj->setObject("attr");
				else
					attrObj = rootObj->setArray("attr");
				
				
				// values that vary at some point across the rows get put under "attr":{"_":[]}
				

				for (auto r : accumulator)
					for (auto c = 0; c < columnCount; ++c)
						if (r->cols[c] != NONE)
						{
							const auto key = make_pair(c, r->cols[c]);
							if (!counts.count(key))
								counts[key] = 1;
							else
								counts[key]++;
						}

				// ok, so, if any column/value pair that has a count that is the same
				// as the number of rows accumulated, they will be inserted directly under
				// the "attr:{" node

				rowObj = attrObj;

				if (condensed)
					for (auto c = 0; c < columnCount; ++c)
					{
						// get the column information
						const auto colInfo = columns->getColumn(columnMap[c]);

						if (colInfo->idx < 1000) // first 1000 are reserved
							continue;

						auto value = accumulator[0]->cols[c];

						if (value == NONE)
							continue;

						// are these all the same on every row in the group? If not, skip
						if (counts[std::make_pair(c, value)] != accumulator.size())
							continue;

						switch (colInfo->type)
						{
						case columnTypes_e::freeColumn:
							continue;
						case columnTypes_e::intColumn:
							rowObj->set(colInfo->name, value);
							break;
						case columnTypes_e::doubleColumn:
							rowObj->set(colInfo->name, value * 10000.0);
							break;
						case columnTypes_e::boolColumn:
							rowObj->set(colInfo->name, value != 0);
							break;
						case columnTypes_e::textColumn:
						{
							// value here is the hashed value							
							if (const auto attr = attributes->get(colInfo->idx, value); attr->text)
								rowObj->set(colInfo->name, attr->text);
						}
						break;
						}
					}

				// now we will emit the individual sub-rows (under "attr":{"_": [...]} ) 
				// for rows that had non-consistent values accross the row group

				cjson* variable;
				if (condensed)
					variable = attrObj->setArray("_");
				else
					variable = attrObj;

				for (auto r : accumulator)
				{
					rowObj = variable->pushObject();

					for (auto c = 0; c < columnCount; ++c)
					{
						// get the column information
						const auto colInfo = columns->getColumn(columnMap[c]);

						if (colInfo->idx < 1000) // first 1000 are reserved
							continue;

						auto value = r->cols[c];

						if (value == NONE)
							continue;

						// are these all the same on every row in the group? If not, skip
						if (condensed && 
							counts[std::make_pair(c, value)] == accumulator.size())
							continue;

						switch (colInfo->type)
						{
						case columnTypes_e::freeColumn:
							continue;
						case columnTypes_e::intColumn:
							rowObj->set(colInfo->name, value);
							break;
						case columnTypes_e::doubleColumn:
							rowObj->set(colInfo->name, value * 10000.0);
							break;
						case columnTypes_e::boolColumn:
							rowObj->set(colInfo->name, value != 0);
							break;
						case columnTypes_e::textColumn:
						{							
							if (const auto attr = attributes->get(colInfo->idx, value); attr && attr->text)
								rowObj->set(colInfo->name, attr->text);
						}
						break;
						}
					}
				}
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
	const volatile auto row = recast<int64_t*>(mem.newPtr(rowBytes));

	for (auto iter = row; iter < row + columnCount; ++iter)
        *iter = NONE;

	if (uuidColumn != -1)
		*(row + uuidColumn) = rawData->id;

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
	if (!rawData || !rawData->bytes || !columnCount)
		return;

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
			if (sessionColumn != -1)
			{
				if (lastSessionTime && row->cols[0] - lastSessionTime > table->sessionTime)
					++session;
				lastSessionTime = row->cols[0];
				row->cols[sessionColumn] = session;
			}
			
			rows.push_back(row);
			row = newRow();
			read += sizeOfCastHeader;
			continue;

		} // skip non-mapped columns

		const int32_t gridColumn = reverseMap[cursor->columnNum];

		if (gridColumn < 0 || gridColumn >= columnCount)
		{
			read += sizeOfCast;
			continue;
		}

		*(row->cols + gridColumn) = cursor->val64;

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
		const auto count = rawData->flagRecords;
		// copy the old flags to the new flags
		newFlags = recast<Flags_s*>(PoolMem::getPool().getPtr(sizeof(Flags_s) * (count + 1)));
		memcpy(newFlags, rawData->getFlags(), sizeof(Flags_s) * count);

		// index is count
		idx = count;
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
	if (rawData->propBytes)
		memcpy(newPerson->getProps(), rawData->getProps(), static_cast<size_t>(rawData->propBytes));
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
	for (auto iter = rawData->getFlags(); iter->flagType != flagType_e::feature_eof; ++iter)
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

	auto writer = newFlags;
	for (auto iter = rawData->getFlags(); iter->flagType != flagType_e::feature_eof; ++iter)
	{
		if (iter->flagType == flagType &&
			iter->reference == reference &&
			iter->context == context)
			continue;
		*writer = *iter;
		++writer;
	}

	const auto newFlagRecords = rawData->flagRecords - 1;
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
	if (rawData->propBytes)
		memcpy(newPerson->getProps(), rawData->getProps(), static_cast<size_t>(rawData->propBytes));
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

	// calculate space needed
	auto bytesNeeded = 0;
	for (auto r : rows)
	{
		for (auto c = 0; c < columnCount; ++c)
		{
			// skip NONE values, placeholder (non-event) columns and auto-generated columns (like session)
			if (r->cols[c] == NONE ||
				(reverseMap[c] >= COL_OMIT_FIRST && reverseMap[c] <= COL_OMIT_LAST))
				continue;
			bytesNeeded += sizeOfCast;
		}
		bytesNeeded += sizeOfCastHeader; // row end
	}

	//TODO - technically we can predict the maximum size without iteration and counting... hmmm..

	// make an intermediate buffer that is fully uncompresed
	const auto intermediateBuffer = recast<char*>(PoolMem::getPool().getPtr(bytesNeeded));
	// copy over header and ID text
	memcpy(intermediateBuffer, rawData, sizeof(PersonData_s) + rawData->idBytes);

	auto write = intermediateBuffer;
	Cast_s* cursor;

	for (auto r : rows)
	{
		for (auto c = 0; c < columnCount; ++c)
		{
			cursor = recast<Cast_s*>(write);

			// skip NONE values, placeholder (non-event) columns and auto-generated columns (like session)
			if (r->cols[c] == NONE ||
				(columnMap[c] >= COL_OMIT_FIRST && columnMap[c] <= COL_OMIT_LAST))
				continue;

			cursor->columnNum = columnMap[c];
			cursor->val64 = r->cols[c];
			write += sizeOfCast;
		}

		cursor = recast<Cast_s*>(write);

		// END OF ROW - write a "row" marker at the end of the row
		cursor->columnNum = -1;
		write += sizeOfCastHeader;
	}

	const auto maxBytes = LZ4_compressBound(bytesNeeded);
	const auto compBuffer = cast<char*>(PoolMem::getPool().getPtr(maxBytes));

	const auto oldCompBytes = rawData->comp;
	const auto newCompBytes = LZ4_compress_fast(
		intermediateBuffer, 
		compBuffer, 
		bytesNeeded,
		maxBytes,
		2);

	const auto newPersonSize = rawData->size() - oldCompBytes + newCompBytes;
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
	if (rawData->propBytes)
		memcpy(newPerson->getProps(), rawData->getProps(), static_cast<size_t>(rawData->propBytes));
	// copy old compressed events
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

Grid::ExpandedRows Grid::iterate_expand(cjson* json) const
{
	auto goodRows = ExpandedRows{ json->getNodes() };

	while (true)
	{
		ExpandedRows newRes;

		auto arrayCount = 0;
		auto objectCount = 0;
		auto nullCount = 0;

		for (auto row : goodRows)
		{
			LineNodes tLine;

			for (auto tNode : row)
			{
				if (!tNode)
				{
					++nullCount;
					continue;
				}

				switch (tNode->type())
				{
				case cjsonType::NUL:
				case cjsonType::INT:
				case cjsonType::DBL:
				case cjsonType::STR:
				case cjsonType::BOOL:
					tLine.push_back(tNode);
					break;
				case cjsonType::OBJECT:
				{
					++objectCount;
					auto objects = tNode->getNodes();
					for (auto tO : objects)
						tLine.push_back(tO);
				}
				break;
				case cjsonType::ARRAY:
					++arrayCount;
					tLine.push_back(tNode);
					break;
				default: 
					break;
				}
			}
			newRes.push_back(tLine);
		}

		if (arrayCount)
		{
			ExpandedRows unRolled;

			for (auto row : newRes)
			{
				auto found = 0;
				auto idx = -1;
				for (auto &tNode : row) // walk through attrs in row
				{
					++idx;
					if (tNode->type() == cjsonType::ARRAY)
					{
						++found;
						auto arrayItems = tNode->getNodes();
						for (auto aI : arrayItems)
						{
							auto newLine(row); // copy row
							if (aI->type() != cjsonType::OBJECT &&
								tNode->hasName())
							{
								newLine[idx] = nullptr;
								newLine.push_back(aI);
								newLine.back()->setName(tNode->name());
							}
							else
								newLine[idx] = aI;
							unRolled.push_back(newLine);
						}
						break;
					}
				}

				if (!found)
					unRolled.push_back(LineNodes(row));
			}

			goodRows = std::move(unRolled);
			continue;
		}

		if (objectCount || nullCount)
		{
			goodRows = std::move(newRes);
			continue;
		}

		if (!arrayCount && !objectCount && !nullCount)
			return goodRows;
	}
}


void Grid::insert(cjson* rowData)
{
	// ensure we have ms on the time stamp
	const auto stampNode = rowData->xPath("/stamp");

	if (!stampNode)
		return;

	auto stamp = (stampNode->type() == cjsonType::STR) ?
		Epoch::ISO8601ToEpoch(stampNode->getString()) :
		stampNode->getInt();

	stamp = Epoch::fixMilli(stamp);

	if (stamp < 0)
		return;

	auto action = rowData->xPathString("/action", "");
	auto attrNode = rowData->xPath("/attr");
	auto rowCount = rows.size();
	decltype(newRow()) row = nullptr;

	if (!attrNode || !action.length())
		return;

	// cull on insert
	if (rowCount > table->rowCull)
	{
		const auto numToErase = rowCount - table->rowCull;
		rows.erase(rows.begin(), rows.begin() + numToErase);
		rowCount = rows.size();
	}

	// move the action into the attrs so it will be integrated into the row set
	attrNode->set("__action", action);

	auto columns = table->getColumns();

	auto insertBefore = -1; // where a new row will be inserted if needed

	const auto hashedAction = MakeHash(action);
	const auto insertRowGroup = HashPair(stamp, hashedAction); //MakeHash(recast<const char*>(&pkValues.front()), 8 * pkValues.size());

	const auto zOrderInts = table->getZOrderInts();

	auto getZOrder = [zOrderInts](int64_t value) -> int {
		auto iter = zOrderInts->find(value);

		if (iter != zOrderInts->end())
			return (*iter).second;

		return 99;
	};

	auto insertZOrder = getZOrder(hashedAction);

	auto findInsert = [&]() -> int {
		int first = 0;
		int last = static_cast<int>(rowCount - 1);
		int mid = last >> 1;

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

	if (i != rowCount) // if they are equal skip all this, we are appending
	{
		// walk back to the beginning of all rows sharing this time stamp
		if (rowCount)
			while (i > 0 && rows[i]->cols[COL_STAMP] == stamp)
				--i;

		// walk forward to find our insertion point
		for (; i < rowCount; i++)
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
					for (; i < rowCount; i++)
					{
						auto rowGroup = HashPair(rows[i]->cols[COL_STAMP], rows[i]->cols[COL_ACTION]);
						zOrder = getZOrder(rows[i]->cols[COL_ACTION]);

						// we have moved passed replacable rows, so insert here
						if (rows[i]->cols[COL_STAMP] > stamp ||
							zOrder > insertZOrder ||
							rowGroup > insertRowGroup)
						{
							insertBefore = i;
							break;
						}

						// we have a matching row, we will replace this
						if (rowGroup == insertRowGroup)
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
		for (auto iter = rows.begin() + insertBefore; iter != rows.end();)
		{
			const auto rowGroup =HashPair((*iter)->cols[COL_STAMP], (*iter)->cols[COL_ACTION]);

			// if stamp changes we are done scanning this row group
			if ((*iter)->cols[0] != stamp)
				break;

			if ((*iter)->cols[COL_STAMP] == stamp &&
				rowGroup == insertRowGroup)
				iter = rows.erase(iter);
			else
				++iter;
		}
	}

	auto expandedSet = iterate_expand(attrNode);

	for (auto& r: expandedSet) // rows in set
	{
		auto fillCount = 0;
		row = newRow();

		for (auto& c: r) // columns in row
		{			
			if (const auto iter = insertMap.find(MakeHash(c->name())); iter != insertMap.end())
			{
				const auto schemaCol = columnMap[iter->second];
				const auto col = iter->second;
				const auto colInfo = columns->getColumn(schemaCol);

				fillCount++;

				auto attrInfo = attributes->getMake(schemaCol, NONE);
				attributes->setDirty(this->rawData->linId, schemaCol, NONE, attrInfo);
				
				auto val = NONE;

				switch (c->type())
				{
				case cjsonType::INT:
					switch (colInfo->type)
					{
						case columnTypes_e::intColumn: 
							val = c->getInt();
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::doubleColumn: 
							val = cast<int64_t>(c->getInt() * 10000LL);
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::boolColumn: 
							val = c->getInt() != 0;
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::textColumn: 
							{
								const auto tstr = to_string(c->getInt());
								val = MakeHash(tstr);
								attrInfo = attributes->getMake(schemaCol, tstr);
							}
							break;
						default:
							break;
					}					
					attributes->setDirty(this->rawData->linId, schemaCol, val, attrInfo);
					break;
				case cjsonType::DBL:
					switch (colInfo->type)
					{
						case columnTypes_e::intColumn: 
							val = cast<int64_t>(c->getDouble());
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::doubleColumn: 
							val = cast<int64_t>(c->getDouble() * 10000LL);
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::boolColumn: 
							val = c->getDouble() != 0;
							attrInfo = attributes->getMake(schemaCol, val);
							break;
						case columnTypes_e::textColumn: 
							{
								const auto tstr = to_string(c->getDouble());
								val = MakeHash(tstr);
								attrInfo = attributes->getMake(schemaCol, tstr);
							}
							break;
						default:
							break;
					}					
					attributes->setDirty(this->rawData->linId, schemaCol, val, attrInfo);
					break;
				case cjsonType::STR:
					switch (colInfo->type)
					{
					case columnTypes_e::intColumn:
					case columnTypes_e::doubleColumn:
						continue;
					case columnTypes_e::boolColumn:
						val = c->getString() != "0";
						attrInfo = attributes->getMake(schemaCol, val);
						break;
					case columnTypes_e::textColumn:
						{
							const auto tstr = c->getString();
							val = MakeHash(tstr);
							attrInfo = attributes->getMake(schemaCol, tstr);
						}
						break;
					default:
						break;
					}
					attributes->setDirty(this->rawData->linId, schemaCol, val, attrInfo);
					break;
				case cjsonType::BOOL:
					switch (colInfo->type)
					{
					case columnTypes_e::intColumn:
						val = c->getBool() ? 1 : 0;
						attrInfo = attributes->getMake(schemaCol, val);
						break;
					case columnTypes_e::doubleColumn:
						val = c->getBool() ? 10000 : 0;
						attrInfo = attributes->getMake(schemaCol, val);
						break;
					case columnTypes_e::boolColumn:
						val = c->getBool();
						attrInfo = attributes->getMake(schemaCol, val);
						break;
					case columnTypes_e::textColumn:
						{
							const auto tstr = c->getBool() ? "true" : "false";
							val = MakeHash(tstr);
							attrInfo = attributes->getMake(schemaCol, tstr);
						}
						break;
					default:
						break;
					}

					// attrInfo = attributes->getMake(schemaCol, val);
					attributes->setDirty(this->rawData->linId, schemaCol, val, attrInfo);
					break;

				default:
					val = NONE;
					break;
				}

				row->cols[col] = val;
			}
			else
			{
				// todo: do we care about non-mapped columns.
			}
		}

		// Do we have values in our Row?
		if (fillCount)
		{
			row->cols[0] = stamp;

			// action is injected into the attrs section so it will be indexed

			if (insertBefore == -1) // no insertion found so append
				rows.push_back(row);
			else // insert before 
			{
				rows.insert(rows.begin() + insertBefore, row);
			}
		}
	}
}

