#include "customer_props.h"
#include "table.h"
#include "properties.h"
#include "dbtypes.h"

void openset::db::CustomerProps::reset()
{
    mem.reset();
    propsChanged = false;
    // setting to nil/none faster than erasing them
    for (auto& prop : props)
        prop.second = NONE;

     oldValues.clear();
     newValues.clear();
}

char* openset::db::CustomerProps::encodeCustomerProps(openset::db::Table* table)
{
    mem.reset();

    auto tableProps = table->getProperties();

    const auto count = mem.newInt32();
    *count = 0;

    for (auto& prop : props)
    {
        auto info = tableProps->getProperty(prop.first);

        if (!info ||
            !info->isCustomerProperty ||
            info->type == openset::db::PropertyTypes_e::freeProp ||
            info->type ==  openset::db::PropertyTypes_e::runTimeTypeProp)
            continue;

        auto& var = prop.second;

        if (var.isPod())
        {
            // sip nil/none values
            if (var.getInt64() == NONE)
                continue;

            // if this is POD and we want a set, skip
            if (info->isSet)
                continue;
        }
        else // is a container
        {
            // skip incorrect types (must be set)
            if (var.typeOf() != cvar::valueType::SET)
               continue;

            // skip if table prop is not a set
            if (!info->isSet)
                continue;

            // skip nil/none values
            if (var.len() == 0)
                continue;
        }

        // store column index
        *mem.newInt32() = static_cast<int>(info->idx);
        // store column type
        *mem.newInt32() = static_cast<int>(info->type);

        // placeholder size
        const auto size = mem.newInt32();

        const auto startOffset = mem.getBytes();

        switch (info->type)
        {
        case openset::db::PropertyTypes_e::intProp:
            if (info->isSet)
            {
                // store number of elements
                *mem.newInt32() = prop.second.len();
                for (auto& item : *var.getSet())
                    *mem.newInt64() = item.getInt64();
            }
            else
            {
                *mem.newInt64() = var.getInt64(); // copy the union in cvar
            }
            break;
        case openset::db::PropertyTypes_e::doubleProp:
            if (info->isSet)
            {
                // store number of elements
                *mem.newInt32() = prop.second.len();
                for (auto& item : *var.getSet())
                    *mem.newInt64() = round(item.getDouble() * 10000);
            }
            else
            {
                *mem.newInt64() = round(var.getDouble() * 10000); // copy the union in cvar
            }
            break;
        case openset::db::PropertyTypes_e::boolProp:
            if (info->isSet)
            {
                // store number of elements
                *mem.newInt32() = prop.second.len();
                for (auto& item : *var.getSet())
                    *mem.newInt64() = item.getBool() ? 1 : 0;
            }
            else
            {
                *mem.newInt64() = var.getBool() ? 1 : 0; // copy the union in cvar
            }
            break;
        case openset::db::PropertyTypes_e::textProp:
            if (info->isSet)
            {
                // store number of elements
                *mem.newInt32() = prop.second.len();
                for (auto& item : *var.getSet())
                {
                    const auto text = item.getString();
                    *mem.newInt32() = text.length();
                    const auto buffer = mem.newPtr(text.length());
                    memcpy(buffer, text.c_str(), text.length());
                }
            }
            else
            {
                const auto text = var.getString();
                *mem.newInt32() = text.length();
                const auto buffer = mem.newPtr(text.length());
                memcpy(buffer, text.c_str(), text.length());
            }
            break;
        }

        // update size of data
        *size = mem.getBytes() - startOffset;

        ++(*count);
    }

    return mem.flatten();
};

void openset::db::CustomerProps::decodeCustomerProps(openset::db::Table* table, char* data)
{
    reset();

    if (!data)
        return;

    auto tableProps = table->getProperties();
    const auto count = static_cast<int32_t>(*data);
    data += sizeof(int32_t);

    for (auto i = 0; i < count; ++i)
    {
        const auto propIndex = *reinterpret_cast<int32_t*>(data);
        data += sizeof(int32_t);
        const auto propType = *reinterpret_cast<openset::db::PropertyTypes_e*>(data);
        data += sizeof(int32_t);
        const auto recordSize = *reinterpret_cast<int32_t*>(data);
        data += sizeof(int32_t);

        const auto info = tableProps->getProperty(propIndex);

        // skip if something has changed (dropped or redefined column?)
        if (!info->isCustomerProperty || info->type != propType)
        {
            data += recordSize;
            continue;
        }

        switch (propType)
        {
        case openset::db::PropertyTypes_e::intProp:
            if (info->isSet)
            {
                const auto elements = *reinterpret_cast<int32_t*>(data);
                data += sizeof(int32_t);

                cvar set;
                set.set();

                for (auto e = 0; e < elements; ++e)
                {
                    set += *reinterpret_cast<int64_t*>(data);
                    data += sizeof(int64_t);
                }

                props[propIndex] = std::move(set);
            }
            else
            {
                props[propIndex] = *reinterpret_cast<int64_t*>(data);
                data += sizeof(int64_t);
            }
            break;
        case openset::db::PropertyTypes_e::doubleProp:
            if (info->isSet)
            {
                const auto elements = *reinterpret_cast<int32_t*>(data);
                data += sizeof(int32_t);

                cvar set;
                set.set();

                for (auto e = 0; e < elements; ++e)
                {
                    set += (static_cast<double>(*reinterpret_cast<int64_t*>(data)) / 10000.0);
                    data += sizeof(int64_t);
                }

                props[propIndex] = std::move(set);
            }
            else
            {
                props[propIndex] = (static_cast<double>(*reinterpret_cast<int64_t*>(data)) / 10000.0);
                data += sizeof(int64_t);
            }
            break;
        case openset::db::PropertyTypes_e::boolProp:
            if (info->isSet)
            {
                const auto elements = *reinterpret_cast<int32_t*>(data);
                data += sizeof(int32_t);

                cvar set;
                set.set();

                for (auto e = 0; e < elements; ++e)
                {
                    set += *reinterpret_cast<int64_t*>(data) ? true : false;
                    data += sizeof(int64_t);
                }

                props[propIndex] = std::move(set);
            }
            else
            {
                props[propIndex] = *reinterpret_cast<int64_t*>(data) ? true : false;
                data += sizeof(int64_t);
            }
            break;
        case openset::db::PropertyTypes_e::textProp:
            if (info->isSet)
            {
                const auto elements = *reinterpret_cast<int32_t*>(data);
                data += sizeof(int32_t);

                cvar set;
                set.set();

                for (auto e = 0; e < elements; ++e)
                {
                    const auto textLength = *reinterpret_cast<int32_t*>(data);
                    data += sizeof(int32_t);
                    set += std::string(data, textLength);
                    data += textLength;
                }

                props[propIndex] = std::move(set);
            }
            else
            {
                const auto textLength = *reinterpret_cast<int32_t*>(data);
                data += sizeof(int32_t);
                props[propIndex] = std::string(data, textLength);
                data += textLength;
            }
            break;
        }
    }
}

int64_t cvarToDB(openset::db::PropertyTypes_e type, const cvar& value)
{

    switch (type)
    {
    case openset::db::PropertyTypes_e::intProp:
        return value.getInt64();
    case openset::db::PropertyTypes_e::doubleProp:
        return value.getDouble() * 10000;
    case openset::db::PropertyTypes_e::boolProp:
        return value.getBool() ? 1 : 0;
    case openset::db::PropertyTypes_e::textProp:
        return MakeHash(value.getString());
    default:
        return NONE;
    }
}

void listFix(cvar& value)
{
    if (value.typeOf() == cvar::valueType::DICT)
    {
        cvar set;
        set.set();

        for (auto& item : *value.getDict())
            set += std::move(item.first);

        value = set;
        return;
    }
    if (value.typeOf() == cvar::valueType::LIST)
    {
        cvar set;
        set.set();

        for (auto& item : *value.getList())
            set += std::move(item);

        value = set;
        return;
    }
}

void openset::db::CustomerProps::setProp(openset::db::Table* table, int propIndex, cvar& value)
{
    const auto propInfo = table->getProperties()->getProperty(propIndex);

    if (!propInfo || !propInfo->isCustomerProperty)
        return;

    if (propInfo->isSet)
        listFix(value);

    if (auto& iter = props.find(propIndex); iter != props.end())
    {
        if (propInfo->isSet)
        {
            if (iter->second.typeOf() == cvar::valueType::SET)
            {
                for (auto& element : *iter->second.getSet())
                {
                    if (!value.contains(element) && element != NONE)
                    {
                        oldValues.emplace_back(propIndex, cvarToDB(propInfo->type, element));
                        propsChanged = true;
                    }
                }

                for (auto& element : *value.getSet())
                {
                    if (!iter->second.contains(element))
                    {
                        newValues.emplace_back(propIndex, cvarToDB(propInfo->type, element));
                        propsChanged = true;
                    }
                }
            }

            iter->second = value;
        }
        else if (iter->second != value)
        {
            propsChanged = true;
            oldValues.emplace_back(propIndex, cvarToDB(propInfo->type, iter->second));
            newValues.emplace_back(propIndex, cvarToDB(propInfo->type, value));
            iter->second = value;
        }
    }
    else
    {
        propsChanged = true;
        if (propInfo->isSet)
        {
            if (value.typeOf() == cvar::valueType::SET)
            {
                for (auto& element : *value.getSet())
                    newValues.emplace_back(propIndex, cvarToDB(propInfo->type, element));
                props[propIndex] = value;
            }
            else
            {
                props[propIndex] = NONE;
            }
        }
        else
        {
            props[propIndex] = value;
            newValues.emplace_back(propIndex, cvarToDB(propInfo->type, value));
        }

        newValues.emplace_back(propIndex, NONE);
    }
}

void openset::db::CustomerProps::setProp(openset::db::Table* table, std::string& name, cvar& value)
{
    const auto propInfo = table->getProperties()->getProperty(name);

    if (!propInfo || !propInfo->isCustomerProperty)
        return;

    setProp(table, propInfo->idx, value);
}

cvar openset::db::CustomerProps::getProp(openset::db::Table* table, int propIndex)
{
    for (auto& prop : props)
    {
        if (prop.first == propIndex)
            return prop.second;
    }

    return NONE;
}

openset::db::CustomerPropMap* openset::db::CustomerProps::getCustomerProps()
{
    return &props;
}
