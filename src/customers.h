#pragma once

#include "common.h"
#include "logger.h"
#include "customer.h"
//#include "mem/bigring.h"
#include "robin_hood.h"
#include "mem/blhash.h"
#include "grid.h"

#include <vector>

using namespace std;

namespace openset
{
    namespace db
    {
        struct PersonData_s;

        class Customers
        {
        public:
            //bigRing<int64_t, int32_t> customerMap; // probably delete this!
            robin_hood::unordered_map<int64_t, int32_t, robin_hood::hash<int64_t>> customerMap;
            vector<PersonData_s*> customerLinear;
            vector<int32_t> reuse;
            int partition;
        public:
            explicit Customers(int partition);
            ~Customers();

            PersonData_s* getCustomerByID(int64_t userId);
            PersonData_s* getCustomerByID(const string& userIdString);
            PersonData_s* getCustomerByLIN(const int64_t linId);

            // will return a "found" customer if one exists
            // or create a new one
            PersonData_s* createCustomer(string userIdString);

            void replaceCustomerRecord(PersonData_s* newRecord);

            int64_t customerCount() const;

            void drop(const int64_t userId);

            void serialize(HeapStack* mem);
            int64_t deserialize(char* mem);
        };
    };
};
