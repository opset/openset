#include "result.h"
#include <algorithm>
#include <sstream>
#include "cjson/cjson.h"
#include "tablepartitioned.h"

using namespace openset::result;

static char NA_TEXT[] = "n/a";

ResultSet::ResultSet()
{
    for (auto &a : accTypes)
        a = ResultTypes_e::Int;

    for (auto &m : accModifiers)
        m = query::Modifiers_e::sum;
}

ResultSet::ResultSet(ResultSet&& other) noexcept:
	results(std::move(other.results)),
	mem(std::move(other.mem)),
	localText(std::move(other.localText))
{
	cout << "result move constructor" << endl;
}

void ResultSet::makeSortedList()
{
	if (isPremerged)
		return;

	sortedResult.clear();

	sortedResult.reserve(results.distinct);

	for (auto& kv: results)
		sortedResult.emplace_back(kv);

	std::sort(
		sortedResult.begin(),
		sortedResult.end(),
		[](const RowPair& left, const RowPair& right ) -> bool
		{
			for (auto i = 0; i < keyDepth; ++i)
			{
				if (left.first.key[i] > right.first.key[i])
					return false;
				if (left.first.key[i] < right.first.key[i])
					return true;
			}
			return true;
		});
}

void ResultSet::setAtDepth(RowKey& key, const function<void(Accumulator*)> set_cb)
{
	auto tPair = results.get(key);

	if (!tPair)
	{
		const auto t = new (mem.newPtr(sizeof(Accumulator))) Accumulator();
		tPair = results.set(key, t);
	}

	set_cb(tPair->second);
}

void ResultSet::setAccTypesFromMacros(const openset::query::Macro_s macros)
{

    auto dataIndex = -1;
    for (auto& g : macros.vars.columnVars) // WAS TABLE VARS
    {
        ++dataIndex;

        if (g.modifier == query::Modifiers_e::var)
        {
            switch (g.value.typeof())
            {
                case cvar::valueType::INT32: 
                case cvar::valueType::INT64: 
                    accTypes[dataIndex] = ResultTypes_e::Int;
                break;
                case cvar::valueType::FLT: 
                case cvar::valueType::DBL: 
                    accTypes[dataIndex] = ResultTypes_e::Double;
                break;
                case cvar::valueType::STR: 
                    accTypes[dataIndex] = ResultTypes_e::Text;
                break;
                case cvar::valueType::BOOL: 
                    accTypes[dataIndex] = ResultTypes_e::Bool;
                break;
                case cvar::valueType::LIST: 
                case cvar::valueType::DICT: 
                case cvar::valueType::SET:
                case cvar::valueType::REF:
                default:
                    accTypes[dataIndex] = ResultTypes_e::None;
            }
        }
        else if (g.modifier == query::Modifiers_e::value)
        {
            switch (g.schemaType)
            {

                case db::columnTypes_e::intColumn: 
                    accTypes[dataIndex] = ResultTypes_e::Int;
                break;
                case db::columnTypes_e::doubleColumn: 
                    accTypes[dataIndex] = ResultTypes_e::Double;
                break;
                case db::columnTypes_e::boolColumn: 
                    accTypes[dataIndex] = ResultTypes_e::Bool;
                break;
                case db::columnTypes_e::textColumn: 
                    accTypes[dataIndex] = ResultTypes_e::Text;
                break;
                case db::columnTypes_e::freeColumn:
                default: 
                    accTypes[dataIndex] = ResultTypes_e::None;
            }
        }
        else 
        {
            switch (g.schemaType)
            {
            case db::columnTypes_e::intColumn:
                accTypes[dataIndex] = ResultTypes_e::Int;
                break;
            case db::columnTypes_e::doubleColumn:
                accTypes[dataIndex] = ResultTypes_e::Double;
                break;
            case db::columnTypes_e::boolColumn:
                accTypes[dataIndex] = ResultTypes_e::Int;
                break;
            case db::columnTypes_e::textColumn:
                accTypes[dataIndex] = ResultTypes_e::Int;
                break;
            case db::columnTypes_e::freeColumn:
            default:
                accTypes[dataIndex] = ResultTypes_e::Int;
            }
        }

        accModifiers[dataIndex] = g.modifier;

    }
}


void mergeResultTypes(
    std::vector<openset::result::ResultSet*>& resultSets)
{
    ResultTypes_e accTypes[ACCUMULATOR_DEPTH];

    for (auto &a : accTypes)
        a = ResultTypes_e::Int;

    for (auto s : resultSets)
    {
        auto idx = -1;
        for (auto &a : s->accTypes)
        {
            ++idx;
            if (a != ResultTypes_e::Int)
                accTypes[idx] = a;
        }
    }

    for (auto s : resultSets)
        memcpy(s->accTypes, accTypes, sizeof(accTypes));
}

bigRing<int64_t, const char*> mergeResultText(
    std::vector<openset::result::ResultSet*>& resultSets)
{
    // merge all the text between from all the results
    bigRing<int64_t, const char*> mergedText(ringHint_e::lt_compact);

    // merge all the localText mappings into a merged text mapping
    for (auto &r : resultSets)
        for (const auto &t : r->localText)
            mergedText[t.first] = t.second;

    return mergedText;
}


/* merge
*
* merge performs a sync merge on a vector of sorted results.
*
* STL was used here because it has great iterators, but a little is lost in
* readabilty. I apologize in advance for the **blah stuff.
*
* Step one make a vector of iterators for each result in the results vector.
* (note, the results vector contains vectors of sorted results).
*
* Step two, iterate until there are no items remaining to be merged.
*
* The iteration step evaluates each result iterator (at it's current location)
* to decide if it has the lowest value (our key type has comparison operators
* overloaded for this purpose).
*
* After all iterators have been checked the one with the lowest value is either
* pushed into the merged list, or if it has the same key as last item in the
* merged list, it is instead summed into that item.
*
* When 'lowestIdx' is equal to end() it means all results passed to merge
* have been merged.
*/
ResultSet::RowVector mergeResultSets(
    const int resultColumnCount,
    const int resultSetCount,
	std::vector<openset::result::ResultSet*>& resultSets)
{      
    mergeResultTypes(resultSets);

	vector<ResultSet::RowVector*> mergeList;

	auto count = 0;

	for (auto &r : resultSets)
	{
		// sort the list
		r->makeSortedList(); 

		// if no data, skip
		if (!r->sortedResult.size()) 
			continue;

		// add it the merge list
		mergeList.push_back(&r->sortedResult);
		count += r->sortedResult.size();
	}

	ResultSet::RowVector merged;
	merged.reserve(count);

    if (mergeList.size() == 0)
        return merged;

	vector<ResultSet::RowVector::iterator> iterators;

	const auto shiftIterations = resultSetCount ? resultSetCount : 1;
    const auto shiftSize = resultColumnCount;

	// get an iterator the beginning of each result in results
	for (auto r : mergeList)
		iterators.emplace_back(r->begin());

    auto &modifiers = resultSets[0]->accModifiers;

	while (true)
	{
		auto lowestIdx = iterators.end();
		auto idx = 0;

		// we have an iterator of iterators (it)
		// we have multiple iterators from multiple results, we are looking for 
		// the one with the lowest key.
		for (auto it = iterators.begin(); it != iterators.end(); ++it , ++idx)
		{
			// get the iterator for the result set pointed to by "it"
			const auto t = (*it);

			if (t == mergeList[idx]->end()) // this iterator is done, so skip
				continue;

			// is it less than equal or 
			// not set (lowestIdx defaults to end(), so not set)
			if (lowestIdx == iterators.end() ||
				(*t).first < (**lowestIdx).first ||
				(*t).first == (**lowestIdx).first)
			{
				lowestIdx = it;
			}
		}

		if (lowestIdx != iterators.end())
		{
			if (merged.size() == 0)
			{
				merged.push_back(**lowestIdx);
			}
			else
			{
				if (merged.back().first == (**lowestIdx).first)
				{
					// make lambda or function
					auto& left = merged.back().second;
					auto& right = (**lowestIdx).second;

					for (auto shiftCount = 0, shiftOffset = 0; shiftCount < shiftIterations; ++shiftCount, shiftOffset += shiftSize)
					{					
						for (auto columnIndex = 0; columnIndex < resultColumnCount; ++columnIndex)
						{
							const auto valueIndex = columnIndex + shiftOffset;

							if (right->columns[valueIndex].value != NONE)
							{
								if (left->columns[valueIndex].value == NONE)
								{
									// if it's the first setting, copy the whole dang thang.
									left->columns[valueIndex] = right->columns[valueIndex];
								}
								else
								{
									// we are updating columns here, accumulator rules apply here
									switch (modifiers[columnIndex]) // WAS TABLEVAR
									{
									case openset::query::Modifiers_e::min:
										if (left->columns[valueIndex].value < right->columns[valueIndex].value)
										{
											left->columns[valueIndex].value = right->columns[valueIndex].value;
											left->columns[valueIndex].count = right->columns[valueIndex].count;
										}
										break;
									case openset::query::Modifiers_e::max:
										if (left->columns[valueIndex].value > right->columns[valueIndex].value)
										{
											left->columns[valueIndex].value = right->columns[valueIndex].value;
											left->columns[valueIndex].count = right->columns[valueIndex].count;
										}
										break;
									case openset::query::Modifiers_e::value:

										left->columns[valueIndex].value = right->columns[valueIndex].value;
										left->columns[valueIndex].count = right->columns[valueIndex].count;
										break;
									case openset::query::Modifiers_e::var:
									case openset::query::Modifiers_e::avg: // average is determined later
									case openset::query::Modifiers_e::sum:
									case openset::query::Modifiers_e::count:
									case openset::query::Modifiers_e::dist_count_person:
										left->columns[valueIndex].value += right->columns[valueIndex].value;
										left->columns[valueIndex].count += right->columns[valueIndex].count;
										break;
									default:;
									}

								}
							}
						}
					}
				}
				else
				{
					merged.push_back(**lowestIdx);
				}
			}

			// increment the iterator pointed to by lowestIdx in our list of iterators
			++(*lowestIdx); 
		}
		else
		{ // no more rows... 
			break;
		}
	}

	return merged;
}


void ResultMuxDemux::mergeMacroLiterals(
    const openset::query::Macro_s macros,
    std::vector<openset::result::ResultSet*>& resultSets)
{
    // copy literals from macros into a localtext object
    for (auto &l : macros.vars.literals)
        resultSets.front()->addLocalText(l.hashValue, l.value);
}

char* ResultMuxDemux::multiSetToInternode(
    const int resultColumnCount, 
    const int resultSetCount,
    std::vector<openset::result::ResultSet*>& resultSets,
    int64_t &bufferLength)
{

    auto mergedText = mergeResultText(resultSets);
    auto rows = mergeResultSets(resultColumnCount, resultSetCount, resultSets);

    bufferLength = 0;

    // we are going to serialize to a HeapStack object
    // and flatten it when we are done
    HeapStack mem;

    // marker are oldschool pre-emoji happy faces ☺☻ (0x01, 0x02)
    const auto binaryMarker = reinterpret_cast<uint16_t*>(mem.newPtr(2));
    const auto binaryMarkerPtr = recast<char*>(binaryMarker);

    binaryMarkerPtr[0] = 0x01; // a hollow happy face
    binaryMarkerPtr[1] = 0x02; // a filled happy face

                               // first 8 bytes are the sized of the block
                               // Note: this is a pointer to the the first 8 bytes of the block
    const auto blockCount = reinterpret_cast<int64_t*>(mem.newPtr(8));
    *blockCount = rows.size();

    // next 8 bytes of block are the offset into the block
    // where text values are going to be stored. 
    // Note: this is a pointer to the the second 8 bytes of the block
    const auto textCount = reinterpret_cast<int64_t*>(mem.newPtr(8));
    *textCount = mergedText.size();

    // record the types and accumulators
    const auto types = reinterpret_cast<char*>(mem.newPtr(sizeof(ResultSet::accTypes)));
    memcpy(types, resultSets[0]->accTypes, sizeof(ResultSet::accTypes));

    const auto modifiers = reinterpret_cast<char*>(mem.newPtr(sizeof(ResultSet::accModifiers)));
    memcpy(modifiers, resultSets[0]->accModifiers, sizeof(ResultSet::accModifiers));

    // iterate the result set
    for (const auto r : rows)
    {
        // make space for a key
        const auto keyPtr = recast<openset::result::RowKey*>(mem.newPtr(sizeof(openset::result::RowKey)));
        // make space for the columns
        const auto accumulatorPtr = recast<openset::result::Accumulator*>(mem.newPtr(sizeof(openset::result::Accumulator)));

        // copy the values
        memcpy(keyPtr, r.first.key, sizeof(openset::result::RowKey));
        memcpy(accumulatorPtr, r.second->columns, sizeof(openset::result::Accumulator));
    }

    // lets encode the text. 
    // text is stored with the hash value (8 bytes) and
    // a null terminated c-style string, it can contain UTF-8 or whatever,
    // we don't really care, it's all just bytes to us.
    for (const auto t : mergedText)
    {
        const auto hash = recast<int64_t*>(mem.newPtr(8)); // get 8 bytes for hash
        const auto length = recast<int32_t*>(mem.newPtr(4)); // get 4 bytes for length

        *hash = t.first; // store the hash
        *length = strlen(t.second); // store the length

                                    // get buffer for text + 1 for 0x00
        const auto textBuffer = mem.newPtr(*length + 1);

        memcpy(textBuffer, t.second, *length);
        textBuffer[*length] = 0; // set null at the last position in the buffer

                                 // NOTE: when parsing, size of record is 8+4+(length of string)+(1 for null)
    }

    bufferLength = mem.getBytes();

    return mem.flatten();

}

bool ResultMuxDemux::isInternode(
	char* data,
	const int64_t blockLength)
{
	const auto binaryMarkerPtr = static_cast<char*>(data);

	return (blockLength >= 18 &&
		binaryMarkerPtr[0] == 0x01 &&
		binaryMarkerPtr[1] == 0x02);
}

openset::result::ResultSet* ResultMuxDemux::internodeToResultSet(
	char* data,
	const int64_t blockLength)
{
	// we are going to make a sorta-bogus result object.
	// the actual 
	auto result = new openset::result::ResultSet();

	// we are making a partial result set, just sorteResult vector filled
	result->isPremerged = true; 

	// empty result
	if (!isInternode(data, blockLength))
		return result;

	auto read = data;
	const auto end = data + blockLength;

	read += 2; // move passed binary marker

	// more naughty C'ish looking stuff
	const auto blockCount = *reinterpret_cast<int64_t*>(read);
	read += 8;
	const auto textCount = *reinterpret_cast<int64_t*>(read);
	read += 8;

    // record the types and accumulators
    memcpy(result->accTypes, read, sizeof(ResultSet::accTypes));
    read += sizeof(ResultSet::accTypes);

    memcpy(result->accModifiers, read, sizeof(ResultSet::accModifiers));
    read += sizeof(ResultSet::accModifiers);
    
	for (auto i = 0; i < blockCount; ++i)
	{
		if (read >= end)
			break;

		const auto keyPtr = recast<openset::result::RowKey*>(read);
		read += sizeof(openset::result::RowKey);

		auto accumulatorPtr = recast<openset::result::Accumulator*>(read);
		read += sizeof(openset::result::Accumulator);

		result->sortedResult.emplace_back(
			openset::result::ResultSet::RowPair{ *keyPtr, accumulatorPtr });
	}

	for (auto i = 0; i < textCount; ++i)
	{
		if (read >= end)
			break;

		const auto hash = recast<int64_t*>(read); // get 8 bytes for hash
		read += 8;
		const auto length = recast<int32_t*>(read); // get 4 bytes for length
		read += 4;
		
		result->addLocalText(*hash, read, *length);
		read += (*length) + 1; // increment 1 more for the 0x00
	}
	
	return result;
}

void ResultMuxDemux::resultSetToJson(
    const int resultColumnCount,
    const int resultSetCount,
    std::vector<openset::result::ResultSet*>& resultSets,
    cjson* doc)
{

    auto mergedText = mergeResultText(resultSets);
    auto rows = mergeResultSets(resultColumnCount, resultSetCount, resultSets);

    const auto shiftIterations = resultSetCount ? resultSetCount : 1;
    const auto shiftSize = resultColumnCount;

	// this will retrieve either the string literals from the macros,
	// the merged localText or exorcise a lock and look in the blob
	const auto getText = [&](int64_t valueHash) -> const char*
	{
		auto textPair = mergedText.get(valueHash);
		
		// if it is already cached return the cached pointer
		if (textPair)
			return textPair->second;

		// nothing found, NA_TEXT
		return NA_TEXT;
	};

	RowKey lastKey;
	lastKey.clear();

	// we are going to move the root down a node
	auto current = doc->pushArray();
	current->setName("_");

	auto maxDepth = 0;
	for (auto &r : rows)
	{
		const auto depth = r.first.getDepth();
		if (depth > maxDepth)
			maxDepth = depth;
	}


    auto &modifiers = resultSets[0]->accModifiers;
    auto &types = resultSets[0]->accTypes;

	auto rowCounter = -1;
	for (auto& r : rows)
	{
		++rowCounter;

		// currentKey is r.first if that makes reading this easier :)
		auto &currentKey = r.first;

		// key is narrower than last, so close objects... } ] etc.
		if (currentKey.getDepth() < lastKey.getDepth())
		{
			const auto diff = lastKey.getDepth() - currentKey.getDepth();

			for (auto i = 0; i < diff; i++)
			{
				if (i) 
					current = current->hasParent();
				current = current->hasParent();
			}
			
			current = current->hasParent();		
		}
		
		// add a new entry to the current object
		auto entry = current->pushObject();
		const auto depth = r.first.getDepth() - 1;

		// set group - this could be text... so, lets see if we cached it (all text 
		// stored by script will be cached)

        switch (currentKey.types[depth])
        {
            case ResultTypes_e::Int: 
                entry->set("g", currentKey.key[depth]);
                break;
            case ResultTypes_e::Double: 
                entry->set("g", currentKey.key[depth] / 10000.0);
                break;
            case ResultTypes_e::Bool:
                entry->set("g", currentKey.key[depth] ? true : false);
                break;
            case ResultTypes_e::Text:
                {
                auto text = getText(currentKey.key[depth]);

                if (text != NA_TEXT)
                    entry->set("g", text);
                else
                    entry->set("g", currentKey.key[depth]);
                }
                break;
            case ResultTypes_e::None: 
            default:
                entry->set("g", "n/a");
        }

		// set columns

		for (auto shiftCount = 0, shiftOffset = 0; shiftCount < shiftIterations; ++shiftCount, shiftOffset += shiftSize)
		{
			auto array = entry->pushArray();
			// one result columns branch will be "c", if multiple it will be "c", "c2", "c3", "c4"
			array->setName(!shiftCount ? "c" : "c" + to_string(shiftCount + 1)); 
            			
            for (auto dataIndex = shiftOffset, colIndex = 0; dataIndex < shiftOffset + shiftSize; ++dataIndex, ++colIndex)
			{

                const auto& value = r.second->columns[dataIndex].value;
                const auto& count = r.second->columns[dataIndex].count;

				// Is this a null, a double, a string or anything else (ints)
				if (r.second->columns[dataIndex].value == NONE)
				{
                    if (types[colIndex] == ResultTypes_e::Double ||
                        types[colIndex] == ResultTypes_e::Int)
                        array->push(static_cast<int64_t>(0));
                    else
					    array->pushNull();
				}
				else
				{
					switch (modifiers[colIndex])
					{
					case query::Modifiers_e::sum:
					case query::Modifiers_e::min:
					case query::Modifiers_e::max:
						if (types[colIndex] == ResultTypes_e::Double)
							array->push(value / 10000.0);
						else
							array->push(value);
						break;
					case query::Modifiers_e::avg:
						if (!count)
							array->pushNull();
						else if (types[colIndex] == ResultTypes_e::Double)
							array->push((value / 10000.0) / static_cast<double>(count));
						else
							array->push(value / static_cast<double>(count));
						break;
					case query::Modifiers_e::count:
					case query::Modifiers_e::dist_count_person:
						array->push(value);
						break;
					case query::Modifiers_e::value:
                        if (types[colIndex] == ResultTypes_e::Text)
							array->push(getText(value));
						else if (types[colIndex] == ResultTypes_e::Double)
							array->push(value / 10000.0);
                        else if (types[colIndex] == ResultTypes_e::Bool)
                            array->push(value ? true : false);
						else
							array->push(value);
						break;
					case query::Modifiers_e::var:
					{
                        if (types[colIndex] == ResultTypes_e::Text)
                            array->push(getText(value));
                        else if (types[colIndex] == ResultTypes_e::Double)
                            array->push(value / 10000.0);
                        else if (types[colIndex] == ResultTypes_e::Bool)
                            array->push(value ? true : false);
                        else
                            array->push(value);
                    }
					break;

					default:
						array->push(value);
					}
				}
			}
		}

		// check to see if the next row is wider (rows[count+1].first is next key)
		// if it is, lets add a nesting level and set current to that level
		if (rowCounter < rows.size()-1 && rows[rowCounter+1].first.getDepth() > currentKey.getDepth())
		{
			current = entry->pushArray();
			current->setName("_");
		}		

		lastKey = r.first;
	}

    /*
	if (macros.isSegment)
	{
		// lock the globals while we modify them
		csLock gLock(*table->getGlobalsLock());

		// set globals for the table
		const auto tableGlobals = table->getGlobalsPtr();

		if (!tableGlobals->contains("segment"))
		{
			(*tableGlobals)["segment"] = cvar{};
			(*tableGlobals)["segment"].dict();
		}

		const auto resultNodes = doc->xPath("/_");

		if (resultNodes)
			for (auto n : resultNodes->getNodes()) // _ is an array
			{
				auto segmentName = n->xPathString("/g", "");
				const auto columns = n->xPath("/c");
				if (columns && segmentName.length())
					(*tableGlobals)["segment"][segmentName] = columns->at(0)->getInt();
			}			
	}
    */
}

void ResultMuxDemux::jsonResultSortByColumn(cjson* doc, const ResultSortOrder_e sort, const int column)
{
    doc->recurseSort("_", [&](const cjson* left, const cjson* right) -> bool
    {
        auto colLeft = left->xPath("/c");
        auto colRight = right->xPath("/c");

        switch (colLeft->at(column)->type())
        {
            case cjsonType::BOOL: 
            case cjsonType::INT: 
                if (sort == ResultSortOrder_e::Asc)
                    return (colLeft->at(column)->getInt() < colRight->at(column)->getInt());
                else
                    return (colLeft->at(column)->getInt() > colRight->at(column)->getInt());
            case cjsonType::DBL: 
                if (sort == ResultSortOrder_e::Asc)
                    return (colLeft->at(column)->getDouble() < colRight->at(column)->getDouble());
                else
                    return (colLeft->at(column)->getDouble() > colRight->at(column)->getDouble());
            case cjsonType::STR: 
                if (sort == ResultSortOrder_e::Asc)
                    return (colLeft->at(column)->getString() < colRight->at(column)->getString());
                else
                    return (colLeft->at(column)->getString() > colRight->at(column)->getString());
            break;           

            case cjsonType::OBJECT:
            case cjsonType::ARRAY: 
            case cjsonType::VOIDED: 
            case cjsonType::NUL: 
            default: 
                return false;
        }
    });
}

void ResultMuxDemux::jsonResultTrim(cjson* doc, const int trim)
{
    if (trim <= 0)
        return;
    
    doc->recurseTrim("_", trim);
}
