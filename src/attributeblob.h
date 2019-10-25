#pragma once

#include "common.h"
#include "dbtypes.h"

#include "threads/locks.h"
//#include "mem/bigring.h"
#include "heapstack/heapstack.h"

#include "robin_hood.h"

using namespace std;

namespace openset
{
	namespace db
	{
		class AttributeBlob
		{
		public:
			robin_hood::unordered_map<attr_key_s, char*, robin_hood::hash<attr_key_s>> attributesBlob;
			HeapStack mem;

			CriticalSection cs;
			AttributeBlob() = default;
			~AttributeBlob() = default;

			bool isAttribute(const int32_t propIndex, const int64_t valueHash);
			bool isAttribute(const int32_t propIndex, const string& value);
			char* storeValue(const int32_t propIndex, const string& value);

			char* getValue(const int32_t propIndex, const int64_t valueHash);
		};
	};
};
