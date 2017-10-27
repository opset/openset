#include "attributes.h"
#include "sba/sba.h"

using namespace openset::db;

void Attr_s::addChange(const int32_t linearId, const bool state)
{
	// using placement new here into a POOL buffer
	const auto change =
		new(PoolMem::getPool().getPtr(sizeof(Attr_changes_s)))
		Attr_changes_s(
			linearId, (state) ? 1 : 0, changeTail);

	changeTail = change;
}

IndexBits* Attr_s::getBits()
{
	auto bits = new IndexBits();

	bits->mount(index, ints, linId);

	auto t = changeTail;

	// non-destructively apply any dirty bits to the bit index
	while (t)
	{
		if (t->state)
			bits->bitSet(t->linId);
		else
			bits->bitClear(t->linId);
		t = t->prev;
	}

	return bits;
}

Attributes::Attributes(const int partition, AttributeBlob* attributeBlob, Columns* columns) :
	blob(attributeBlob),
	columns(columns),
	partition(partition)
{
}

Attributes::~Attributes()
{}

Attributes::ColumnIndex* Attributes::getColumnIndex(const int32_t column)
{
	const auto colRingPair = columnIndex.find(column);

	if (colRingPair != columnIndex.end())
		return colRingPair->second;

	const auto newRing = new ColumnIndex(ringHint_e::lt_compact);
	columnIndex[column] = newRing;

	return newRing;
}

Attr_s* Attributes::getMake(const int32_t column, const int64_t value)
{
	auto columnIndex = getColumnIndex(column);
	
	if (auto attrPair = columnIndex->get(value); attrPair == nullptr)
	{
		const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
		attrPair = columnIndex->set(value, attr);
		return attrPair->second;
	}
	else
	{
		return attrPair->second;
	}
}

Attr_s* Attributes::getMake(const int32_t column, const string value)
{
	auto columnIndex = getColumnIndex(column);
	const auto valueHash = MakeHash(value);

	if (auto attrPair = columnIndex->get(valueHash); attrPair == nullptr)
	{
		const auto attr = new(PoolMem::getPool().getPtr(sizeof(Attr_s)))Attr_s();
		//attr->text = 
		blob->storeValue(column, value);
		attrPair = columnIndex->set(valueHash, attr);
		return attrPair->second;
	}
	else
	{
		return attrPair->second;
	}
}

Attr_s* Attributes::get(const int32_t column, const int64_t value)
{
	const auto columnIndex = getColumnIndex(column);
	
	if (const auto attrPair = columnIndex->get(value); attrPair != nullptr)
		return attrPair->second;

	return nullptr;
}

Attr_s* Attributes::get(const int32_t column, const string value)
{
	const auto columnIndex = getColumnIndex(column);

	if (const auto attrPair = columnIndex->get(MakeHash(value)); attrPair != nullptr)
		return attrPair->second;

	return nullptr;
}

void Attributes::setDirty(const int32_t linId, const int32_t column, const int64_t value, Attr_s* attrInfo)
{
	dirty.insert(attr_key_s::makeKey(column, value));
	attrInfo->addChange(linId, true);
}

void Attributes::clearDirty()
{
	IndexBits bits;
	AttrPair* attrPair;

	for (auto& d : dirty)
	{
		const auto columnIndex = getColumnIndex(d.column);

		if ((attrPair = columnIndex->get(d.value)) == nullptr)
			continue;

		const auto attr = attrPair->second;

		bits.mount(attr->index, attr->ints, attr->linId);

		auto t = attr->changeTail;

		while (t)
		{
			if (t->state)
				bits.bitSet(t->linId);
			else
				bits.bitClear(t->linId);
			const auto prev = t->prev;
			PoolMem::getPool().freePtr(t);
			t = prev;
		}

		attr->changeTail = nullptr;

		int64_t compBytes = 0; // OUT value via reference
		int32_t linId;

		// compress the data, get it back in a pool ptr
		const auto compData = bits.store(compBytes, linId);
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
		destAttr->comp = compBytes;
		destAttr->linId = linId;

		// if we made a new destination, we have to update the 
		// index to point to it, and free the old one up.
		// update the Attr pointer directly in the index
		attrPair->second = destAttr;
		PoolMem::getPool().freePtr(attr);

	}
	dirty.clear();
}

void Attributes::swap(const int32_t column, const int64_t value, IndexBits* newBits)
{
	AttrPair* attrPair;

	const auto columnIndex = getColumnIndex(column);

	if ((attrPair = columnIndex->get(value)) == nullptr)
		return;

	const auto attr = attrPair->second;

	int64_t compBytes = 0; // OUT value
	int32_t linId = -1;

	// compress the data, get it back in a pool ptr, size returned in compBytes
	const auto compData = newBits->store(compBytes, linId);
	const auto destAttr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + compBytes));

	// copy header
	memcpy(destAttr, attr, sizeof(Attr_s));
	if (compData)
	{
		memcpy(destAttr->index, compData, compBytes);
		// return work buffer from bits.store to the pool
		PoolMem::getPool().freePtr(compData);
	}

	destAttr->ints = newBits->ints;//asList ? 0 : newBits->ints;
	destAttr->comp = compBytes;
	destAttr->linId = linId;

	// if we made a new destination, we have to update the 
	// index to point to it, and free the old one up.
	// update the Attr pointer directly in the index
	attrPair->second = destAttr;
	PoolMem::getPool().freePtr(attr);
}

AttributeBlob* Attributes::getBlob() const
{
	return blob;
}

Attributes::AttrList Attributes::getColumnValues(const int32_t column, const listMode_e mode, const int64_t value)
{
	Attributes::AttrList result;
	Attr_s* attr;

	switch (mode)
	{
		// so.. NEQ is handled outside of this function
		// in query indexing
	case listMode_e::NEQ:
	case listMode_e::EQ:
		attr = get(column, value);
		if (attr)
			result.push_back(attr);
		return result;
	case listMode_e::PRESENT:
		attr = get(column, NONE);
		if (attr)
			result.push_back(attr);
		return result;
		default: ;
	}

	const auto columnIndex = getColumnIndex(column);

	for (auto &kv : *columnIndex)
	{
		switch (mode)
		{
		case listMode_e::GT:
			if (kv.first > value)
				result.push_back(kv.second);
			break;
		case listMode_e::GTE:
			if (kv.first >= value)
				result.push_back(kv.second);
			break;
		case listMode_e::LT:
			if (kv.first < value)
				result.push_back(kv.second);
			break;
		case listMode_e::LTE:
			if (kv.first <= value)
				result.push_back(kv.second);
			break;
		default:
			// never happens
			break;
		}
	}

	return result;
}

void Attributes::serialize(HeapStack* mem)
{
	// grab 8 bytes, and set the block type at that address 
	*recast<serializedBlockType_e*>(mem->newPtr(sizeof(int64_t))) = serializedBlockType_e::attributes;

	// grab 8 more bytes, this will be the length of the attributes data within the block
	const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
	(*sectionLength) = 0;

	for (auto& col : columnIndex)
	{
		for (auto &item : *col.second)
		{
			// add a header to the HeapStack
			const auto blockHeader = recast<serializedAttr_s*>(mem->newPtr(sizeof(serializedAttr_s)));

			// fill in the header
			blockHeader->column = col.first;
			blockHeader->hashValue = item.first;
			blockHeader->ints = item.second->ints;
			auto text = this->blob->getValue(col.first, item.first);
			blockHeader->textSize = text ? strlen(text) : 0;
			//blockHeader->textSize = item.second->text ? strlen(item.second->text) : 0;
			blockHeader->compSize = item.second->comp;

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
				memcpy(blockData, item.second->index, blockHeader->compSize);
			}

			(*sectionLength) +=
				sizeof(serializedAttr_s) +
				blockHeader->textSize +
				blockHeader->compSize;
		}
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

		// get or make a column index
		auto columnIndex = getColumnIndex(blockHeader->column);

		char* blobPtr = nullptr;

		// is there text? Lets add this to the blob and use this pointer after to set the
		// text member of attr
		if (blockHeader->textSize)
			blobPtr = blob->storeValue(blockHeader->column, std::string{ textPtr, static_cast<size_t>(blockHeader->textSize) });

		// create an attr_s object
		const auto attr = recast<Attr_s*>(PoolMem::getPool().getPtr(sizeof(Attr_s) + blockHeader->compSize));
		//attr->text = blobPtr;
		attr->ints = blockHeader->ints;
		attr->comp = blockHeader->compSize;
		attr->changeTail = nullptr;

		// copy the data in
		memcpy(attr->index, dataPtr, blockHeader->compSize);

		// add it to the index
		columnIndex->set(blockHeader->hashValue, attr);

		// next block please
		read += blockLength;
	}

	return blockSize + 16;
}