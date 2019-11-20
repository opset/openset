#pragma once
#include "robin_hood.h"
#include "heapstack/heapstack.h"
#include "var/var.h"

namespace openset
{
    namespace db
    {
        class Table;

        using CustomerPropMap = robin_hood::unordered_map<int,cvar, robin_hood::hash<int>>;
        using CustomerPropChange = std::pair<int, int64_t>;
        using CustomerPropChangeList = std::vector<CustomerPropChange>;

        class CustomerProps
        {
            HeapStack mem;
            CustomerPropMap props;

            bool propsChanged {false};

            CustomerPropChangeList oldValues;
            CustomerPropChangeList newValues;

        public:

            CustomerProps() = default;
            ~CustomerProps() = default;

            void reset();

            char* encodeCustomerProps(openset::db::Table* table);
            void decodeCustomerProps(openset::db::Table* table, char* data);

            void setProp(openset::db::Table* table, int propIndex, cvar& value);
            void setProp(openset::db::Table* table, std::string& name, cvar& value);

            cvar getProp(openset::db::Table* table, int propIndex);

            bool havePropsChanged() const
            {
                return propsChanged;
            }

            CustomerPropChangeList& getOldValues()
            {
               return oldValues;
            }

            CustomerPropChangeList& getNewValues()
            {
               return newValues;
            }

            CustomerPropMap* getCustomerProps();
        };
    };
};