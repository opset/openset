#include "customers.h"
#include "heapstack/heapstack.h"
#include "sba/sba.h"

using namespace openset::db;

Customers::Customers(const int partition) :
    //customerMap(ringHint_e::lt_5_million),
    partition(partition)
{}

Customers::~Customers()
{
    for (const auto &person: customerLinear)
        PoolMem::getPool().freePtr(person);
}

PersonData_s* Customers::getCustomerByID(int64_t userId)
{
    int32_t linId;

    if (const auto entry = customerMap.find(userId); entry != customerMap.end())
        return getCustomerByLIN(entry->second);

    return nullptr;
}

PersonData_s* Customers::getCustomerByID(const string& userIdString)
{
    auto hashId = MakeHash(userIdString);

    while (true)
    {
        const auto person = getCustomerByID(hashId);

        if (!person)
            return nullptr;

        // check for match/collision
        if (person->getIdStr() == userIdString)
            return person;

        hashId++; // keep incrementing until we hit.
    }
}

PersonData_s* Customers::getCustomerByLIN(const int64_t linId)
{
    // check ranges
    if (linId < 0 || linId >= customerLinear.size())
        return nullptr;

    return customerLinear[linId];
}

PersonData_s* Customers::createCustomer(string userIdString)
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
        const auto person = getCustomerByID(hashId);

        auto isReuse = false;
        auto linId = static_cast<int32_t>(customerLinear.size());

        if (!person && !reuse.empty())
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
            newUser->bytes = 0;
            newUser->comp = 0;
            newUser->props = nullptr;
            newUser->setIdStr(userIdString);

            if (!isReuse)
                customerLinear.push_back(newUser);

            customerMap[hashId] = newUser->linId;

            return newUser;
        }

        // check for match/collision
        if (person->getIdStr() == userIdString)
            return person;

        // keep incrementing until we miss.
        hashId++;
    }
}

void Customers::replaceCustomerRecord(PersonData_s* newRecord)
{
    if (newRecord && customerLinear[newRecord->linId] != newRecord)
        customerLinear[newRecord->linId] = newRecord;
}

int64_t Customers::customerCount() const
{
    return static_cast<int64_t>(customerLinear.size());
}

void Customers::drop(const int64_t userId)
{
    const auto info = getCustomerByID(userId);

    if (!info)
        return;

    //customerMap.erase(userId);

    customerLinear[info->linId] = nullptr;

    reuse.push_back(info->linId);

    PoolMem::getPool().freePtr(info);
}

void Customers::serialize(HeapStack* mem)
{
    // grab 8 bytes, and set the block type at that address
    *recast<serializedBlockType_e*>(mem->newPtr(sizeof(int64_t))) = serializedBlockType_e::people;

    // grab 8 more bytes, this will be the length of the attributes data within the block
    const auto sectionLength = recast<int64_t*>(mem->newPtr(sizeof(int64_t)));
    (*sectionLength) = 0;

    for (auto person : customerLinear)
    {
        if (!person)
            continue;

        const auto size = person->size();
        const auto serializedPerson = mem->newPtr(size);

        memcpy(serializedPerson, person, size);
        *sectionLength += size;
    }
}

int64_t Customers::deserialize(char* mem)
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

    customerMap.clear();
    customerLinear.clear();
    customerLinear.reserve(sectionLength);
    reuse.clear();

    // end is the length of the block after the 16 bytes of header
    const auto end = read + sectionLength;

    while (read < end)
    {
        const auto streamPerson = recast<PersonData_s*>(read);
        const auto size = streamPerson->size();

        const auto customer = recast<PersonData_s*>(PoolMem::getPool().getPtr(size));
        memcpy(customer, streamPerson, size);

        // grow if a record was excluded during serialization
        while (static_cast<int>(customerLinear.size()) <= customer->linId)
            customerLinear.push_back(nullptr);

        // index this customer
        customerLinear[customer->linId] = customer;
        customerMap[customer->id] = customer->linId;

        // next block please
        read += size;
    }

    for (auto i = 0; i < static_cast<int>(customerLinear.size()); ++i)
    {
        if (!customerLinear[i])
            reuse.push_back(i);
    }


    return sectionLength + 16;
}
