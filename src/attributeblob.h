#pragma once

#include "common.h"
#include "dbtypes.h"

#include "threads/locks.h"
#include "mem/bigring.h"
#include "heapstack/heapstack.h"

using namespace std;

namespace openset
{
	namespace db
	{
		class AttributeBlob
		{
		public:
			bigRing<attr_key_s, char*> attributesBlob{ ringHint_e::lt_1_million };
			HeapStack mem;

			CriticalSection cs;
			AttributeBlob() = default;
			~AttributeBlob() = default;

			bool isAttribute(const int32_t column, const int64_t valueHash);
			bool isAttribute(const int32_t column, const string& value);
			char* storeValue(const int32_t column, const string& value);

			char* getValue(const int32_t column, const int64_t valueHash);
		};
	};
};
