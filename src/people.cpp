#include "people.h"
#include "heapstack/heapstack.h"
#include "sba/sba.h"

using namespace openset::db;

People::People(const int partition) :	
	peopleMap(ringHint_e::lt_5_million),
    partition(partition)
{}

People::~People()
{
    for (const auto &person: peopleLinear)
        PoolMem::getPool().freePtr(person);
}

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
		const auto person = getPersonByID(hashId);

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

        auto isReuse = false;
        auto linId = static_cast<int32_t>(peopleLinear.size());

        if (!reuse.empty())
        {
            linId = reuse.back();
            reuse.pop_back();
            isReuse = true;
        }

		if (!person) // not found, lets create
		{
			auto newUser = recast<PersonData_s*>(PoolMem::getPool().getPtr(sizeof(PersonData_s) + idLen));

			newUser->id = hashId;
			newUser->linId = linId;
			newUser->idBytes = 0;
			newUser->flagRecords = 0;
			newUser->bytes = 0;
			newUser->comp = 0;
            newUser->props = nullptr;
			newUser->setIdStr(userIdString);

            if (!isReuse)
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

void People::drop(const int64_t userId)
{
    const auto info = getPersonByID(userId);

    if (!info)
        return;

    peopleMap.erase(userId);

    peopleLinear[info->linId] = nullptr;

    reuse.push_back(info->linId);

    PoolMem::getPool().freePtr(info);
}

void People::serialize(HeapStack* mem)
{
	// grab 8 bytes, and set the block type at that address 
	*recast<serializedBlockType_e*>(mem->newPtr(sizeof(int64_t))) = serializedBlockType_e::people;

	// grab 8 more bytes, this will be the length of the attributes data within the block
	const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
	(*sectionLength) = 0;

	for (auto person : peopleLinear)
	{
        if (!person)
            continue;

        const auto size = person->size();
		const auto serializedPerson = mem->newPtr(size);

		memcpy(serializedPerson, person, size);
		*sectionLength += size;
	}
}

int64_t People::deserialize(char* mem)
{
	auto read = mem;

	if (*recast<serializedBlockType_e*>(read) != serializedBlockType_e::people)
		return 0;

	read += sizeof(int64_t);

    const auto sectionLength = *recast<int64_t*>(read);
	read += sizeof(int64_t);

	if (sectionLength == 0)
	{
		Logger::get().error("no people to deserialize for partition " + to_string(partition));
		return 16;
	}

    peopleMap.clear();
    peopleLinear.clear();
    peopleLinear.reserve(sectionLength);
    reuse.clear();

	// end is the length of the block after the 16 bytes of header
    const auto end = read + sectionLength;

	while (read < end)
	{
		const auto streamPerson = recast<PersonData_s*>(read);
		const auto size = streamPerson->size();

		const auto person = recast<PersonData_s*>(PoolMem::getPool().getPtr(size));
		memcpy(person, streamPerson, size);

		// grow if a record was excluded during serialization 
		while (static_cast<int>(peopleLinear.size()) <= person->linId)
			peopleLinear.push_back(nullptr);

		// index this person
		peopleLinear[person->linId] = person;
		peopleMap.set(person->id, person->linId);
		
		// next block please
		read += size;
	}

    for (auto i = 0; i < static_cast<int>(peopleLinear.size()); ++i)
    {
        if (!peopleLinear[i])
            reuse.push_back(i);
    }


	return sectionLength + 16;
}
