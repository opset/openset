#pragma once

#include "common.h"
#include "dbtypes.h"

#include "threads/locks.h"
#include "mem/bigring.h"

using namespace std;

namespace openset
{
	namespace db
	{
		class AttributeBlob
		{
			bigRing<attr_key_s, char*> attributesBlob;
		public:

			CriticalSection cs;
			AttributeBlob();
			~AttributeBlob();

			bool isAttribute(int32_t column, int64_t valueHash);
			bool isAttribute(int32_t column, string value);
			char* storeValue(int32_t column, string value);

			char* getValue(int32_t column, int64_t valueHash);
		};
	};
};
