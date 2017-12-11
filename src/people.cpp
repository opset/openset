#include "people.h"
#include "heapstack/heapstack.h"
#include "sba/sba.h"

using namespace openset::db;

People::People(const int partition) :	
	partition(partition)
{}

People::~People()
{}

PersonData_s* People::getPersonByID(int64_t userId)
{
	int32_t linId;

	if (peopleMap.get(userId, linId))
		return getPersonByLIN(linId);

	return nullptr;
}

PersonData_s* People::getPersonByID(string userIdString)
{
	auto hashId = MakeHash(userIdString);

	while (true)
	{
		auto person = getPersonByID(hashId);

		if (!person)
			return nullptr;

		// check for match/collision
		if (person->getIdStr() == userIdString)
			return person;

		hashId++; // keep incrementing until we hit.
	}
}

PersonData_s* People::getPersonByLIN(const int64_t linId)
{
	// check ranges
	if (linId < 0 || linId >= peopleLinear.size())
		return nullptr;

	return peopleLinear[linId];
}

PersonData_s* People::getmakePerson(string userIdString)
{
    auto idLen = userIdString.length();
    if (idLen > 64)
    {
        idLen = 64;
        userIdString.erase(userIdString.begin() + idLen);
    }
	auto hashId = MakeHash(userIdString);	

	while (true)
	{
	    const auto person = getPersonByID(hashId);

		if (!person) // not found, lets create
		{
			auto newUser = recast<PersonData_s*>(PoolMem::getPool().getPtr(sizeof(PersonData_s) + idLen));

			//newUser->idstr = cast<char*>(PoolMem::getPool().getPtr(idLen + 1));
			//strcpy(newUser->idstr, idCstr);

			newUser->id = hashId;
			newUser->linId = static_cast<int32_t>(peopleLinear.size());
			newUser->idBytes = 0;
			newUser->propBytes = 0;
			newUser->flagRecords = 0;
			newUser->bytes = 0;
			newUser->comp = 0;
			newUser->setIdStr(userIdString);

			peopleLinear.push_back(newUser);

			peopleMap.set(hashId, newUser->linId);

			return newUser;
		}

		// check for match/collision
		if (person->getIdStr() == userIdString)
			return person;

		// keep incrementing until we miss.
		hashId++;
	}
}

int64_t People::peopleCount() const
{
	return static_cast<int64_t>(peopleLinear.size());
}

void People::serialize(HeapStack* mem)
{
	// grab 8 bytes, and set the block type at that address 
	*recast<serializedBlockType_e*>(mem->newPtr(sizeof(int64_t))) = serializedBlockType_e::people;

	// grab 8 more bytes, this will be the length of the attributes data within the block
	auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
	(*sectionLength) = 0;

	for (auto person : peopleLinear)
	{
		auto serializedPerson = recast<PersonData_s*>(mem->newPtr(person->size()));
		memcpy(serializedPerson, person, person->size());
		*sectionLength += person->size();
	}
}

int64_t People::deserialize(char* mem)
{
	auto read = mem;

	if (*recast<serializedBlockType_e*>(read) != serializedBlockType_e::people)
		return 0;

	read += sizeof(int64_t);

	auto blockSize = *recast<int64_t*>(read);

	if (blockSize == 0)
	{
		Logger::get().error("no people to deserialize for partition " + to_string(partition));
		return 16;
	}

	read += sizeof(int64_t);

	// end is the length of the block after the 16 bytes of header
	auto end = read + blockSize;

	while (read < end)
	{
		auto streamPerson = recast<PersonData_s*>(read);
		auto streamPersonLen = streamPerson->size();

		auto person = recast<PersonData_s*>(PoolMem::getPool().getPtr(streamPersonLen));
		memcpy(person, streamPerson, streamPersonLen);

		// grow if a record was excluded during serialization 
		while (peopleLinear.size() <= person->linId)
			peopleLinear.push_back(nullptr);

		// index this person
		peopleLinear[person->linId] = person;
		peopleMap.set(person->id, person->linId);
		
		// next block please
		read += streamPersonLen;
	}

	return blockSize + 16;
}
