#include "result.h"
#include <algorithm>
#include <sstream>
#include "cjson/cjson.h"
#include "tablepartitioned.h"

using namespace openset::result;

static char NA_TEXT[] = "n/a";

ResultSet::ResultSet():
	results(ringHint_e::lt_1_million),
	localText(ringHint_e::lt_compact) 
{}

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

void ResultSet::setAtDepth(RowKey& key, function<void(Accumulator*)> set_cb)
{
	// SHARED FIX
	//csLock lock(cs);

	auto tPair = results.get(key);

	if (!tPair)
	{
		auto t = new (mem.newPtr(sizeof(Accumulator))) Accumulator();
		tPair = results.set(key, t);
	}

	set_cb(tPair->second);
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

ResultSet::RowVector ResultMuxDemux::mergeResultSets(
	const openset::query::macro_s macros,
	openset::db::Table* table,
	std::vector<openset::result::ResultSet*> resultSets)
{

	auto schema = table->getColumns();

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

	vector<ResultSet::RowVector::iterator> iterators;

	// get an iterator the beginning of each result in results
	for (auto r : mergeList)
		iterators.emplace_back(r->begin());

	while (true)
	{
		auto lowestIdx = iterators.end();

		auto idx = 0;

		for (auto it = iterators.begin(); it != iterators.end(); ++it , ++idx)
		{
			// look at the iterator IN the iterator (*)
			auto t = (*it);

			if (t == mergeList[idx]->end())
				continue;

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

					for (auto valueIndex = 0; valueIndex < accDepth; ++valueIndex)
					{
						if (right->columns[valueIndex].value != NULLCELL)
						{
							if (left->columns[valueIndex].value == NULLCELL)
							{
								// if it's the first setting, copy the whole dang thang.
								left->columns[valueIndex] = right->columns[valueIndex];
								//left->columns[valueIndex].distinctId = 1; // dummy not NULLCELL value
							}
							else
							{
								// we are updating columns here, accumulator rules apply here
								switch (macros.vars.columnVars[valueIndex].modifier) // WAS TABLEVAR
								{
								case query::modifiers_e::min: 
									if (left->columns[valueIndex].value < right->columns[valueIndex].value)
									{
										left->columns[valueIndex].value = right->columns[valueIndex].value;
										left->columns[valueIndex].count = right->columns[valueIndex].count;
									}
									break;
								case query::modifiers_e::max: 
									if (left->columns[valueIndex].value > right->columns[valueIndex].value)
									{
										left->columns[valueIndex].value = right->columns[valueIndex].value;
										left->columns[valueIndex].count = right->columns[valueIndex].count;
									}
									break;
								case query::modifiers_e::value: 

									left->columns[valueIndex].value = right->columns[valueIndex].value;
									left->columns[valueIndex].count = right->columns[valueIndex].count;
									break;
								case query::modifiers_e::var:
								case query::modifiers_e::avg: // average is determined later
								case query::modifiers_e::sum:
								case query::modifiers_e::count:
									left->columns[valueIndex].value += right->columns[valueIndex].value;
									left->columns[valueIndex].count += right->columns[valueIndex].count;
									break;
								
								/*case query::modifiers_e::date: 
									break;
									default: 
									// TODO error?
									break;*/

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

			++(*lowestIdx);
		}
		else
		{ // no more rows... 
			break;
		}
	}

	return merged;
}

bigRing<int64_t, const char*> ResultMuxDemux::mergeText(
	const openset::query::macro_s macros,
	openset::db::Table* table,
	std::vector<openset::result::ResultSet*> resultSets)
{
	// merge all the text between from al the results
	bigRing<int64_t, const char*> mergedText(ringHint_e::lt_compact);


	// copy literals from macros into a localtext object
	for (auto &l : macros.vars.literals)
		resultSets.front()->addLocalText(l.hashValue, l.value);

	// merge all the localText mappings into a merged text mapping
	for (auto &r : resultSets)
		for (auto t : r->localText)
			mergedText[t.first] = t.second;

	return mergedText;
}

char* ResultMuxDemux::resultSetToInternode(
	const openset::query::macro_s macros,
	openset::db::Table* table,
	ResultSet::RowVector& rows,
	bigRing<int64_t, const char*>& mergedText,
	int64_t& bufferLength)
{
	char* result = nullptr;
	bufferLength = 0;

	// we are going to serialize to a HeapStack object
	// and flatten it when we are done
	HeapStack mem;

	// marker are oldschool pre-emoji happy faces ☺☻ (0x01, 0x02)
	auto binaryMarker = reinterpret_cast<uint16_t*>(mem.newPtr(2));
	auto binaryMarkerPtr = recast<char*>(binaryMarker);

	binaryMarkerPtr[0] = 0x01; // a hollow happy face
	binaryMarkerPtr[1] = 0x02; // a filled happy face

	// first 8 bytes are the sized of the block
	// Note: this is a pointer to the the first 8 bytes of the block
	auto blockCount = reinterpret_cast<int64_t*>(mem.newPtr(8));
	*blockCount = rows.size();

	// next 8 bytes of block are the offset into the block
	// where text values are going to be stored. 
	// Note: this is a pointer to the the second 8 bytes of the block
	auto textCount = reinterpret_cast<int64_t*>(mem.newPtr(8));
	*textCount = mergedText.size();

	// iterate the result set
	for (auto r: rows)
	{
		// make space for a key
		auto keyPtr = recast<openset::result::RowKey*>(mem.newPtr(sizeof(openset::result::RowKey)));
		// make space for the columns
		auto accumulatorPtr = recast<openset::result::Accumulator*>(mem.newPtr(sizeof(openset::result::Accumulator)));

		// copy the values
		memcpy(keyPtr, r.first.key, sizeof(openset::result::RowKey));
		memcpy(accumulatorPtr, r.second->columns, sizeof(openset::result::Accumulator));		
	}
	
	// lets encode the text. 
	// text is stored with the hash value (8 bytes) and
	// a null terminated c-style string, it can contain UTF-8 or whatever,
	// we don't really care, it's all just bytes to us.
	for (auto t : mergedText)
	{
		auto hash = recast<int64_t*>(mem.newPtr(8)); // get 8 bytes for hash
		auto length = recast<int32_t*>(mem.newPtr(4)); // get 4 bytes for length

		*hash = t.first; // store the hash
		*length = strlen(t.second); // store the length

		// get buffer for text + 1 for 0x00
		auto textBuffer = mem.newPtr(*length + 1);

		memcpy(textBuffer, t.second, *length);
		textBuffer[*length] = 0; // set null at the last position in the buffer

		// NOTE: when parsing, size of record is 8+4+(length of string)+(1 for null)
	}

	bufferLength = mem.getBytes();

	return mem.flatten();
}

bool ResultMuxDemux::isInternode(
	char* data,
	int64_t blockLength)
{
	auto binaryMarkerPtr = static_cast<char*>(data);

	return (blockLength >= 18 &&
		binaryMarkerPtr[0] == 0x01 &&
		binaryMarkerPtr[1] == 0x02);
}

openset::result::ResultSet* ResultMuxDemux::internodeToResultSet(
	char* data, 
	int64_t blockLength)
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
	auto end = data + blockLength;

	read += 2; // move passed binary marker

	// more naughty C'ish looking stuff
	auto blockCount = *reinterpret_cast<int64_t*>(read);
	read += 8;
	auto textCount = *reinterpret_cast<int64_t*>(read);
	read += 8;

	for (auto i = 0; i < blockCount; ++i)
	{
		if (read >= end)
			break;

		auto keyPtr = recast<openset::result::RowKey*>(read);
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

		auto hash = recast<int64_t*>(read); // get 8 bytes for hash
		read += 8;
		auto length = recast<int32_t*>(read); // get 4 bytes for length
		read += 4;
		
		result->addLocalText(*hash, read, *length);
		read += (*length) + 1; // increment 1 more for the 0x00
	}
	
	return result;
}

void ResultMuxDemux::resultSetToJSON(
	const openset::query::macro_s macros,
	openset::db::Table* table,
	cjson* doc,
	ResultSet::RowVector& rows,
	bigRing<int64_t, const char*>& mergedText)
{

	auto blob = table->getAttributeBlob();

	// this will retrieve either the string literals from the macros,
	// the merged localText or exorcise a lock and look in the blob
	auto getText = [&](int64_t column, int64_t valueHash) -> const char*
	{
		auto textPair = mergedText.get(valueHash);
		
		// if it is already cached return the cached pointer
		if (textPair)
			return textPair->second;

		// if no column is found we can't look in the AttributeBlob, so
		// return NA_TEXT
		if (column == NULLCELL)
			return NA_TEXT;

		// look in the blob
		auto text = blob->getValue(
			column,
			valueHash);

		// if we got some text, then return it
		if (text)
		{
			mergedText.set(valueHash, text);
			return text;
		}

		// nothing found, NA_TEXT
		return NA_TEXT;
	};

	auto tableColumns = table->getColumns();

	RowKey lastKey;
	lastKey.clear();

	// we are going to move the root down a node
	auto current = doc->pushArray();
	current->setName("_");
	auto root = current;

	auto maxDepth = 0;
	for (auto &r : rows)
	{
		auto depth = r.first.getDepth();
		if (depth > maxDepth)
			maxDepth = depth;
	}

	auto count = -1;
	for (auto& r : rows)
	{
		++count;

		// currentKey is r.first if that makes reading this easier :)
		auto &currentKey = r.first;

		// key is narrower than last, so close objects... } ] etc.
		if (currentKey.getDepth() < lastKey.getDepth())
		{
			auto diff = lastKey.getDepth() - currentKey.getDepth();

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
		auto depth = r.first.getDepth() - 1;

		// set group - this could be text... so, lets see if we cached it (all text 
		// stored by script will be cached)
		auto text = getText(NULLCELL, currentKey.key[depth]);

		if (text != NA_TEXT)
			entry->set("g", text);
		else
			entry->set("g", currentKey.key[depth]);

		// set columns
		auto array = entry->pushArray();
		array->setName("c");

		auto colDepth = -1;
		for (auto& g : macros.vars.columnVars) // WAS TABLE VARS
		{
			++colDepth;

			// Is this a null, a double, a string or anything else (ints)
			// sorry for all the {} but it makes it easier for me to follow
 			// sorry for all the {} but it makes it easier for me to follow
			if (r.second->columns[colDepth].value == NULLCELL)
			{
				array->pushNull();
			}
			else 
			switch (g.modifier)
			{
				case query::modifiers_e::sum: 
				case query::modifiers_e::min: 
				case query::modifiers_e::max: 
				case query::modifiers_e::avg: 
					if (g.schemaType == db::columnTypes_e::doubleColumn)
						array->push(r.second->columns[colDepth].value / 10000.0);
					else
						array->push(r.second->columns[colDepth].value);
				break;
				case query::modifiers_e::count: 
					array->push(r.second->columns[colDepth].value);
				break;
				case query::modifiers_e::value: 
					if (g.schemaType == db::columnTypes_e::textColumn)
						array->push(getText(g.schemaColumn, r.second->columns[colDepth].value));
					else if (g.schemaType == db::columnTypes_e::doubleColumn)
						array->push(r.second->columns[colDepth].value / 10000.0);
					else
						array->push(r.second->columns[colDepth].value);
					break;
				case query::modifiers_e::var: 
				{
					auto columnText = getText(NULLCELL, r.second->columns[colDepth].value);

					// TODO - figure out some smart way to say this was a floating point number

					if (columnText != NA_TEXT)
						array->push(columnText);
					else
						array->push(r.second->columns[colDepth].value);
				}
				break;

				case query::modifiers_e::second_number:
				case query::modifiers_e::second_date:
				case query::modifiers_e::minute_number:
				case query::modifiers_e::minute_date:
				case query::modifiers_e::hour_number:
				case query::modifiers_e::hour_date:
				case query::modifiers_e::day_date: 
				case query::modifiers_e::day_of_week:
				case query::modifiers_e::day_of_month:
				case query::modifiers_e::day_of_year: 
				case query::modifiers_e::week_date: 
				case query::modifiers_e::month_date:
				case query::modifiers_e::month_number:
				case query::modifiers_e::quarter_number:
				case query::modifiers_e::quarter_date:
				case query::modifiers_e::year_number:
				case query::modifiers_e::year_date:
				default: 
					array->push(r.second->columns[colDepth].value);
			}

		}

		// check to see if the next row is wider (rows[count+1].first is next key)
		// if it is, lets add a nesting level and set current to that level
		if (count < rows.size()-1 && rows[count+1].first.getDepth() > currentKey.getDepth())
		{
			current = entry->pushArray();
			current->setName("_");
		}		

		lastKey = r.first;
	}

	
	doc->recurseSort("_", [&](const cjson* left, const cjson* right) -> bool
	{
		auto colLeft = left->xPath("/c");
		auto colRight = right->xPath("/c");

		for (auto &o : macros.vars.sortOrder)
		{
			if (colLeft->at(o.column)->getInt() == colRight->at(o.column)->getInt())
				continue;
			if (o.order == openset::query::sortOrder_e::ascending)
				return (colLeft->at(o.column)->getInt() < colRight->at(o.column)->getInt());
			else
				return (colLeft->at(o.column)->getInt() > colRight->at(o.column)->getInt());
		}

		return false;
	});

	if (macros.isSegment)
	{
		// lock the globals while we modify them
		csLock gLock(*table->getGlobalsLock());

		//
		auto tableGlobals = table->getGlobalsPtr();

		if (!tableGlobals->contains("segment"))
		{
			(*tableGlobals)["segment"] = cvar{};
			(*tableGlobals)["segment"].dict();
		}

		auto resultNodes = doc->xPath("/_");

		if (resultNodes)
			for (auto n : resultNodes->getNodes()) // _ is an array
			{
				auto segmentName = n->xPathString("/g", "");
				auto columns = n->xPath("/c");
				if (columns && segmentName.length())
					(*tableGlobals)["segment"][segmentName] = columns->at(0)->getInt();
			}			
	}
}


