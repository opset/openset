#pragma once

#include "common.h"
#include "logger.h"
#include "customer.h"
#include "mem/blhash.h"
#include "grid.h"

#include <vector>
#include "mem/segmented_list.h"

using namespace std;

namespace openset
{
    namespace db
    {
        struct PersonData_s;

        class Customers
        {
        public:
            BinaryListHash<int64_t, int32_t> customerMap;
            SegmentedList<PersonData_s*, 512> customerLinear;
            int partition;

            explicit Customers(int partition);
            ~Customers();

            PersonData_s* getCustomerByID(int64_t userId);
            PersonData_s* getCustomerByID(const string& userIdString);
            PersonData_s* getCustomerByLIN(const int64_t linId);

            // will return a "found" customer if one exists
            // or create a new one
            PersonData_s* createCustomer(int64_t userId);
            PersonData_s* createCustomer(string userIdString);

            void replaceCustomerRecord(PersonData_s* newRecord);

            int64_t customerCount() const;

            void drop(const int64_t userId);

            void serialize(HeapStack* mem);
            int64_t deserialize(char* mem);
        };
    };
};
