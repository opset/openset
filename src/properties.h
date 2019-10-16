#pragma once

#include "common.h"

#include <unordered_map>
#include <unordered_set>

#include "threads/locks.h"
#include "dbtypes.h"

using namespace std;

namespace openset
{
    namespace db
    {
        static const unordered_set<std::string> PropertyTypes = {
            { "int" },
            { "double" },
            { "text" },
            { "bool" }
        };

        class Properties
        {
        public:

            struct Property_s
            {
                string name;
                int32_t idx{ 0 };
                PropertyTypes_e type{ PropertyTypes_e::freeProp };
                bool isSet{ false };
                bool isCustomerProperty{ false };
                bool deleted{ false };
            };

            using PropsMap = unordered_map<string, Property_s*>;

            // shared lock (uses spin locks)
            CriticalSection lock;

            Property_s properties[MAX_PROPERTIES];
            PropsMap nameMap;
            PropsMap customerPropertyMap;
            int propertyCount{ 0 };

            Properties();
            ~Properties();

            // get a property record, this will always return existing or new
            Property_s* getProperty(const int column);
            Property_s* getProperty(const string& name);

            // helpers - don't use in tight loops
            bool isEventProperty(const string& name);
            bool isCustomerProperty(const string& name);
            bool isSet(const string& name);


            void deleteProperty(Property_s* propInfo);

            int getPropertyCount() const;

            void setProperty(
                const int index,
                const string& name,
                const PropertyTypes_e type,
                const bool isSet,
                const bool isCustomerProp = false,
                const bool deleted = false);

            static bool validPropertyName(const std::string& name);

        };
    };
};
