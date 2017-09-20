#include "attributeblob.h"
#include "sba/sba.h"

openset::db::AttributeBlob::AttributeBlob():
	attributesBlob(ringHint_e::lt_compact)
{}

openset::db::AttributeBlob::~AttributeBlob()
{}

bool openset::db::AttributeBlob::isAttribute(int32_t column, int64_t valueHash) 
{
	csLock lock(cs);
	return attributesBlob.count(attr_key_s::makeKey(column, valueHash));
}

bool openset::db::AttributeBlob::isAttribute(int32_t column, string value) 
{
	// TODO iterate for collisions
	auto valueHash = MakeHash(value);

	csLock lock(cs);
	return attributesBlob.count(attr_key_s::makeKey(column, valueHash));
}

char* openset::db::AttributeBlob::storeValue(int32_t column, string value)
{
	auto valueHash = MakeHash(value);
	char* blob = nullptr;

	auto key = attr_key_s::makeKey(column, valueHash);

	csLock lock(cs);

	if (!attributesBlob.get(key, blob))
	{
		// not found let's make it!
		auto len = value.length();
		blob = cast<char*>(PoolMem::getPool().getPtr(len + 1));
		strcpy(blob, value.c_str());
		attributesBlob.set(key, blob);
	}

	return blob;
}

char* openset::db::AttributeBlob::getValue(int32_t column, int64_t valueHash)
{
	char* blob = nullptr;
	auto key = attr_key_s::makeKey(column, valueHash);

	csLock lock(cs);
	attributesBlob.get(key, blob);
	return blob;
}
