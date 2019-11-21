#include "customer_index.h"

openset::db::CustomerIndexList openset::db::CustomerPropIndex::serialize(
    int64_t startCustomer,
    int64_t startValue,
    int limit,
    const std::function<bool(SortKeyOneProp_s*, int*)>& filterCallback)
{
    SortKeyOneProp_s startKey(startCustomer, startValue);
    return index.serialize(startKey, limit, filterCallback);
}
