#include "attributes.h"
#include "sba/sba.h"

using namespace openset::db;

attr_s::attr_s() :
	changeTail(nullptr),
	text(nullptr),
	ints(0),
	comp(0)
{}

void attr_s::addChange(int32_t LinearId, bool State)
{
	// using placement new here into a POOL buffer
	auto change =
		new(PoolMem::getPool().getPtr(sizeof(attr_changes_s)))
		attr_changes_s(
			LinearId, (State) ? 1 : 0, changeTail);

	changeTail = change;
}

IndexBits* attr_s::getBits()
{
	auto bits = new IndexBits();
	bits->mount(index, ints);

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

Attributes::Attributes(int partition, AttributeBlob* attributeBlob, Columns* columns) :
	blob(attributeBlob),
	columns(columns),
	partition(partition)
{
}

Attributes::~Attributes()
{}

Attributes::ColumnIndex* Attributes::getColumnIndex(int32_t column)
{
	auto colRingPair = columnIndex.find(column);

	if (colRingPair != columnIndex.end())
		return colRingPair->second;

	auto newRing = new ColumnIndex(ringHint_e::lt_compact);

	columnIndex[column] = newRing;

	return newRing;
}

attr_s* Attributes::getMake(int32_t column, int64_t value)
{

	auto columnIndex = getColumnIndex(column);
	AttrPair* attrPair;

	if ((attrPair = columnIndex->get(value)) == nullptr)
	{
		auto attr = new(PoolMem::getPool().getPtr(sizeof(attr_s)))attr_s();
		attrPair = columnIndex->set(value, attr);
	}

	return attrPair->second;
}

attr_s* Attributes::getMake(int32_t column, string value)
{
	auto columnIndex = getColumnIndex(column);
	AttrPair* attrPair;

	auto valueHash = MakeHash(value);

	if ((attrPair = columnIndex->get(valueHash)) == nullptr)
	{
		auto attr = new(PoolMem::getPool().getPtr(sizeof(attr_s)))attr_s();

		attr->text = blob->storeValue(column, value);
		attrPair = columnIndex->set(valueHash, attr);
	}

	return attrPair->second;
}

attr_s* Attributes::get(int32_t column, int64_t value)
{
	auto columnIndex = getColumnIndex(column);
	AttrPair* attrPair;

	if ((attrPair = columnIndex->get(value)) != nullptr)
		return attrPair->second;

	return nullptr;
}

attr_s* Attributes::get(int32_t column, string value)
{
	auto columnIndex = getColumnIndex(column);
	AttrPair* attrPair;
	auto valueHash = MakeHash(value);

	if ((attrPair = columnIndex->get(valueHash)) != nullptr)
		return attrPair->second;

	return nullptr;
}

void Attributes::setDirty(int32_t linId, int32_t column, int64_t value, attr_s* attrInfo)
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

		auto columnIndex = getColumnIndex(d.column);

		if ((attrPair = columnIndex->get(d.value)) == nullptr)
			continue;

		auto attr = attrPair->second;

		bits.mount(attr->index, attr->ints);

		auto t = attr->changeTail;

		while (t)
		{
			if (t->state)
				bits.bitSet(t->linId);
			else
				bits.bitClear(t->linId);
			auto prev = t->prev;
			PoolMem::getPool().freePtr(t);
			t = prev;
		}

		attr->changeTail = nullptr;

		int64_t compBytes = 0; // OUT value via reference

							   // compress the data, get it back in a pool ptr
		auto compData = bits.store(compBytes);
		auto destAttr = recast<attr_s*>(PoolMem::getPool().getPtr(sizeof(attr_s) + compBytes));

		// copy header
		memcpy(destAttr, attr, sizeof(attr_s));
		memcpy(destAttr->index, compData, compBytes);

		// return work buffer from bits.store to the pool
		PoolMem::getPool().freePtr(compData);

		destAttr->ints = bits.ints;
		destAttr->comp = compBytes;

		// if we made a new destination, we have to update the 
		// index to point to it, and free the old one up.
		// update the Attr pointer directly in the index
		attrPair->second = destAttr;
		PoolMem::getPool().freePtr(attr);

	}
	dirty.clear();
}

void Attributes::swap(int32_t column, int64_t value, IndexBits* newBits)
{
	AttrPair* attrPair;

	auto columnIndex = getColumnIndex(column);

	if ((attrPair = columnIndex->get(value)) == nullptr)
		return;

	auto attr = attrPair->second;

	int64_t compBytes = 0; // OUT value via reference

						   // compress the data, get it back in a pool ptr
	auto compData = newBits->store(compBytes);
	auto destAttr = recast<attr_s*>(PoolMem::getPool().getPtr(sizeof(attr_s) + compBytes));

	// copy header
	memcpy(destAttr, attr, sizeof(attr_s));
	memcpy(destAttr->index, compData, compBytes);

	// return work buffer from bits.store to the pool
	PoolMem::getPool().freePtr(compData);

	destAttr->ints = newBits->ints;
	destAttr->comp = compBytes;

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

Attributes::AttrList Attributes::getColumnValues(int32_t column, listMode_e mode, int64_t value)
{
	Attributes::AttrList result;
	attr_s* attr;

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
		break;
	case listMode_e::PRESENT:
		attr = get(column, NULLCELL);
		if (attr)
			result.push_back(attr);
		return result;
		break;
	}

	auto columnIndex = getColumnIndex(column);

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
	auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
	(*sectionLength) = 0;

	for (auto& col : columnIndex)
	{
		for (auto &item : *col.second)
		{
			// add a header to the HeapStack
			auto blockHeader = recast<serializedAttr_s*>(mem->newPtr(sizeof(serializedAttr_s)));

			// fill in the header
			blockHeader->column = col.first;
			blockHeader->hashValue = item.first;
			blockHeader->ints = item.second->ints;
			blockHeader->textSize = item.second->text ? strlen(item.second->text) : 0;
			blockHeader->compSize = item.second->comp;

			// copy a text/blob value if any
			if (blockHeader->textSize)
			{
				auto textData = recast<char*>(mem->newPtr(blockHeader->textSize));
				memcpy(textData, item.second->text, blockHeader->textSize);
			}

			// copy the compressed data
			if (blockHeader->compSize)
			{
				auto blockData = recast<char*>(mem->newPtr(blockHeader->compSize));
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

	auto blockSize = *recast<int64_t*>(read);

	if (blockSize == 0)
	{
		Logger::get().info("no attributes to deserialize for partition " + to_string(partition));
		return 16;
	}

	read += sizeof(int64_t);

	// end is the length of the block after the 16 bytes of header
	auto end = read + blockSize;

	while (read < end)
	{
		// pointer to block
		auto blockHeader = recast<serializedAttr_s*>(read);
		auto blockLength = sizeof(serializedAttr_s) + blockHeader->textSize + blockHeader->compSize;

		auto textPtr = read + sizeof(serializedAttr_s);
		auto dataPtr = textPtr + blockHeader->textSize;

		// get or make a column index
		auto columnIndex = getColumnIndex(blockHeader->column);

		char* blobPtr = nullptr;

		// is there text? Lets add this to the blob and use this pointer after to set the
		// text member of attr
		if (blockHeader->textSize)
			blobPtr = blob->storeValue(blockHeader->column, std::string{ textPtr, static_cast<size_t>(blockHeader->textSize) });

		// create an attr_s object
		auto attr = recast<attr_s*>(PoolMem::getPool().getPtr(sizeof(attr_s) + blockHeader->compSize));
		attr->text = blobPtr;
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