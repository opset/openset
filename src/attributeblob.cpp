#include "attributeblob.h"
#include "sba/sba.h"

openset::db::AttributeBlob::AttributeBlob()
{}

openset::db::AttributeBlob::~AttributeBlob()
{}

bool openset::db::AttributeBlob::isAttribute(const int32_t column, const int64_t valueHash)
{
	csLock lock(cs);
	return attributesBlob.count(attr_key_s::makeKey(column, valueHash));
}

bool openset::db::AttributeBlob::isAttribute(const int32_t column, const string& value)
{
	// TODO iterate for collisions
	const auto valueHash = MakeHash(value);

	csLock lock(cs);
	return attributesBlob.count(attr_key_s::makeKey(column, valueHash));
}

char* openset::db::AttributeBlob::storeValue(const int32_t column, const string& value)
{
	const auto valueHash = MakeHash(value);
	char* blob = nullptr;

	const auto key = attr_key_s::makeKey(column, valueHash);

	csLock lock(cs);

	if (!attributesBlob.get(key, blob))
	{
		// not found let's make it!
		const auto len = value.length();
		blob = mem.newPtr(len + 1);//cast<char*>(PoolMem::getPool().getPtr(len + 1));
		strcpy(blob, value.c_str());
		attributesBlob.set(key, blob);
	}

	return blob;
}

char* openset::db::AttributeBlob::getValue(const int32_t column, const int64_t valueHash)
{
	char* blob = nullptr;
	const auto key = attr_key_s::makeKey(column, valueHash);

	csLock lock(cs);
	attributesBlob.get(key, blob);
	return blob;
}
