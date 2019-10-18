#include <regex>

#include "properties.h"
#include "config.h"

using namespace openset::db;

Properties::Properties()
{
    // so when addressed by pointer we get the index
    for (auto i = 0; i < MAX_PROPERTIES; i++)
        properties[i].idx = i;
}

Properties::~Properties() = default;

Properties::Property_s* Properties::getProperty(const int propIndex)
{
    return &properties[propIndex];
}

bool Properties::isCustomerProperty(const string& name)
{
    csLock _lck(lock);

    return customerPropertyMap.count(name) != 0;
}

bool Properties::isEventProperty(const string& name)
{
    csLock _lck(lock);

    if (const auto iter = nameMap.find(name); iter != nameMap.end() && iter->second->isCustomerProperty == false)
        return true;

    return false;
}

bool Properties::isSet(const string& name)
{
    csLock _lck(lock);

    if (const auto iter = nameMap.find(name); iter != nameMap.end() && iter->second->isSet)
        return true;

    return false;
}

Properties::Property_s* Properties::getProperty(const string& name)
{
    csLock _lck(lock);

    if (const auto iter = nameMap.find(name); iter != nameMap.end())
        return iter->second;

    return nullptr;
}

void Properties::deleteProperty(Property_s* propInfo)
{
    propInfo->deleted = Now();
    nameMap.erase(propInfo->name);

    if (customerPropertyMap.count(propInfo->name))
        customerPropertyMap.erase(propInfo->name);

    propInfo->name = "___deleted";
}

int Properties::getPropertyCount() const
{
    return propertyCount;
}

void Properties::setProperty(
    const int index,
    const string& name,
    const PropertyTypes_e type,
    const bool isSet,
    const bool isCustomerProp,
    const bool deleted)
{
    csLock _lck(lock);

    if (properties[index].name.size())
        nameMap.erase(properties[index].name);

    if (nameMap.find(name) != nameMap.end())
    {
        auto oldRecord = nameMap[name];
        oldRecord->name.clear();
        oldRecord->type = PropertyTypes_e::freeProp;
    }

    // update the map
    nameMap[name] = &properties[index];

    // update the property
    properties[index].name = name;
    properties[index].type = type;
    properties[index].isSet = isSet;
    properties[index].isCustomerProperty = isCustomerProp;
    properties[index].deleted = deleted;

    if (!isCustomerProp && customerPropertyMap.count(name))
        customerPropertyMap.erase(name);

    if (isCustomerProp)
        customerPropertyMap.emplace(name, &properties[index]);

    propertyCount = 0;
    for (const auto& c : properties)
        if (c.type != PropertyTypes_e::freeProp)
            ++propertyCount;
}

bool Properties::validPropertyName(const std::string& name)
{
    static std::regex nameCapture(R"(^([^ 0-9][a-z0-9_]+)$)");

    std::smatch matches;
    regex_match(name, matches, nameCapture);

    if (matches.size() != 2)
        return false;

    return matches[1] == name;
}
