#pragma once

#include "common.h"
#include "logger.h"
#include "person.h"
#include "mem/bigring.h"
#include "grid.h"

#include <vector>

using namespace std;

namespace openset
{
	namespace db
	{
		struct PersonData_s;

		class People
		{
		public:
			bigRing<int64_t, int32_t> peopleMap; // probably delete this!
			vector<PersonData_s*> peopleLinear;
			int partition;
		public:
			explicit People(int partition);
			~People();

			PersonData_s* getPersonByID(int64_t userId);
			PersonData_s* getPersonByID(string userIdString);
			PersonData_s* getPersonByLIN(const int64_t linId);

			// will return a "found" person if one exists
			// or create a new one
			PersonData_s* getmakePerson(string userIdString);

			void replacePersonRecord(PersonData_s* newRecord)
			{
				if (newRecord)
					peopleLinear[newRecord->linId] = newRecord;
			}

			int64_t peopleCount() const;

			void serialize(HeapStack* mem);
			int64_t deserialize(char* mem);
		};
	};
};
