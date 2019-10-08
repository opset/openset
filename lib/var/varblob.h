#pragma once

#include "var.h"
#include "../heapstack/heapstack.h"
#include "xxhash.h"

class varBlob
{
public:

#pragma pack(push, 1)

    struct overlay_s
    {
        cvar::valueType type;
    };

    struct overlayBasic_s
    {
        friend class cvar;
        cvar::valueType type;
        cvar::dataUnion data;
    };

    struct overlayString_s
    {
        cvar::valueType type;
        int16_t textLen;
        char text[1];

        size_t getSize() const
        {
            return sizeof(type) + sizeof(textLen) + textLen;
        }
    };

    static const size_t overlayString_s_size = { sizeof(overlayString_s) - 1 };

    struct overlayListDictSet_s
    {
        cvar::valueType type;
        int16_t members;
        // followed by relevant overlay type
    };

#pragma pack(pop)

private:

    static void serializeRecursive(const cvar& var, HeapStack& mem)
    {
        switch (var.type)
        {
        case cvar::valueType::INT32:
        case cvar::valueType::INT64:
        case cvar::valueType::FLT:
        case cvar::valueType::DBL:
        case cvar::valueType::BOOL:
        {
            auto overlay  = reinterpret_cast<overlayBasic_s*>(mem.newPtr(sizeof(overlayBasic_s)));
            overlay->type = var.type;
            overlay->data = var.value;
        }
            break;
        case cvar::valueType::STR:
        {
            const auto strLen = var.valueString.length();
            const auto bytes  = overlayString_s_size + strLen;
            auto overlay      = reinterpret_cast<overlayString_s*>(mem.newPtr(bytes));
            overlay->type     = var.type;
            overlay->textLen  = strLen;
            memcpy(overlay->text, var.valueString.c_str(), strLen);
        }
        break;
        case cvar::valueType::LIST:
        {
            auto overlay     = reinterpret_cast<overlayListDictSet_s*>(mem.newPtr(sizeof(overlayListDictSet_s)));
            overlay->type    = var.type;
            overlay->members = (*var.listValue).size();

            // go recursive for the members of the list
            for (auto& item : *var.listValue)
                serializeRecursive(item, mem);
        }
        break;
        case cvar::valueType::DICT:
        {
            auto overlay     = reinterpret_cast<overlayListDictSet_s*>(mem.newPtr(sizeof(overlayListDictSet_s)));
            overlay->type    = var.type;
            overlay->members = (*var.dictValue).size();

            // go recursive for the members of the list
            for (auto& pair : *var.dictValue)
            {
                serializeRecursive(pair.first, mem);  // serialize the key
                serializeRecursive(pair.second, mem); // serialize the value
            }
        }
        break;
        case cvar::valueType::SET:
        {
            auto overlay     = reinterpret_cast<overlayListDictSet_s*>(mem.newPtr(sizeof(overlayListDictSet_s)));
            overlay->type    = var.type;
            overlay->members = (*var.setValue).size();

            // go recursive for the members of the list
            for (auto& item : *var.setValue)
                serializeRecursive(item, mem);
        }
        break;
        default:
            break;
        }
    };

    // this was prettier before it got faster, but this recursive function
    // unpacks serialized cvar objects recursively using move semantics to
    // help minimize the number of objects created and copied.
    static cvar deserializeRecursive(char* & read)
    {
        switch (reinterpret_cast<overlay_s*>(read)->type)
        {
        case cvar::valueType::INT32:
        case cvar::valueType::INT64:
        case cvar::valueType::FLT:
        case cvar::valueType::DBL:
        case cvar::valueType::BOOL:
        {
            cvar result(reinterpret_cast<overlayBasic_s*>(read)->type);
            result.value = reinterpret_cast<overlayBasic_s*>(read)->data;
            read += sizeof(overlayBasic_s);
            return result;
        }
        case cvar::valueType::STR:
        {
            cvar result(cvar::valueType::STR);
            result.valueString.assign(
                reinterpret_cast<overlayString_s*>(read)->text,
                reinterpret_cast<overlayString_s*>(read)->textLen);
            read += reinterpret_cast<overlayString_s*>(read)->getSize();
            return result;
        }
        case cvar::valueType::SET:
        {
            auto members = static_cast<int>(reinterpret_cast<overlayListDictSet_s*>(read)->members);
            read += sizeof(overlayListDictSet_s);

            cvar result(cvar::valueType::SET);
            while (members)
            {
                result.setValue->emplace(deserializeRecursive(read));
                --members;
            }
            return result;
        }
        case cvar::valueType::LIST:
        {
            auto members = static_cast<int>(reinterpret_cast<overlayListDictSet_s*>(read)->members);
            read += sizeof(overlayListDictSet_s);

            cvar result(cvar::valueType::LIST);
            while (members)
            {
                result.listValue->emplace_back(deserializeRecursive(read));
                --members;
            }
            return result;
        }
        case cvar::valueType::DICT:
        {
            auto members = static_cast<int>(reinterpret_cast<overlayListDictSet_s*>(read)->members);
            read += sizeof(overlayListDictSet_s);

            cvar result(cvar::valueType::DICT);
            cvar key;
            while (members)
            {
                key = deserializeRecursive(read);
                // can't pass deserializeRecursive to both because evaluation order is not guaranteed.
                result.dictValue->emplace(std::make_pair(key, deserializeRecursive(read)));
                --members;
            }
            return result;
        }
        default:
            return cvar(cvar::valueType::INT32);
        }
    }

    static void hashRecursive(const cvar& var, int64_t& hash)
    {
        // hash in the type
        hash = XXH64(reinterpret_cast<const void*>(&var.type), 1, hash);

        // hash in the values recursively
        switch (var.type)
        {
        case cvar::valueType::INT32:
        case cvar::valueType::INT64:
        case cvar::valueType::FLT:
        case cvar::valueType::DBL:
        case cvar::valueType::BOOL:
        {
            // value is a 64bit union (8 bytes)
            if (var.getInt64() != NONE)
                hash = XXH64(reinterpret_cast<const void*>(&var.value), 8, hash);
        }
            break;
        case cvar::valueType::STR:
        {
            hash = XXH64(reinterpret_cast<const void*>(var.valueString.c_str()), var.valueString.length(), hash);
        }
        break;
        case cvar::valueType::LIST:
        {
            // go recursive for the members of the list
            for (auto& item : *var.listValue)
                hashRecursive(item, hash);
        }
        break;
        case cvar::valueType::DICT:
        {
            // go recursive for the members of the list
            for (auto& pair : *var.dictValue)
            {
                if (!pair.second.isContainer() && pair.second.getInt64() == NONE)
                    continue;
                hashRecursive(pair.first, hash);  // serialize the key
                hashRecursive(pair.second, hash); // serialize the value
            }
        }
        break;
        case cvar::valueType::SET:
        {
            // go recursive for the members of the list
            for (auto& item : *var.setValue)
                hashRecursive(item, hash);
        }
        break;
        default:
            break;
        }
    };

public:
    static void serialize(HeapStack& mem, cvar& var)
    {
        mem.reset();
        serializeRecursive(var, mem);
    }

    static void deserialize(cvar& outputVar, char* blobPtr)
    {
        outputVar.clear();
        outputVar = deserializeRecursive(blobPtr);
    }

    static int64_t hash(cvar& var)
    {
        int64_t hash = 0xFACEFEEDDEADBEEFLL;
        hashRecursive(var, hash);
        return hash;
    }
};
