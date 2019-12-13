#include "customer_index.h"

openset::db::CustomerIndexList openset::db::CustomerPropIndex::serialize(
    bool descending,
    int limit,
    const std::function<bool(SortKeyOneProp_s*, int*)>& filterCallback)
{
    return index.serialize(descending, limit, filterCallback);
}
