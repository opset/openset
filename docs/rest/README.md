# Cluster

## PUT /v1/cluster/init?partitions={#}

Initializes a cluster (a cluster with just **one** node will still need initializing).

This turns an unassigned/waiting node into a cluster sentinel. A node that is part of a cluster can invite other nodes to join that cluster.

`partitions` is specifies the number of shards the data will be divided into. Pick a number that is roughly 10-20% higher than the totals number of cores your envision being part of your final cluster.

Returns a 200 or 400 status code.

> :pushpin:the ideal partition size is the lowest possible number that will fit the size of your cluster in the long run. There is overhead incurred with each partition, but you also want to pick a number that will allow you to grow. Picking a number less than the number of processor cores in your cluster will **not** allow you to reach peak performance.

## PUT /v1/cluster/join?host={host|ip}&port={port}

**query_params:**

-   host={name | ip}
-   port={port} (optional, default is 8080)

This will issue a `/v1/internode/is_cluster_member` request, and if that returns `false` a `/v1/internode/join_to_cluster` request.

Returns a 200 or 400 status code.

## Table

## POST /v1/table/{table} (create table)

Create a table by passing a JSON array of desired table properties and types.

A property at minimum requires a name and type.

- `name` - properties can have lowercase alphanumeric names as long as they don't start with a number or contain spaces (`_` is valid, other symbols are not).
- `type` - valid types are `text`, `int`, `double`,  and `bool`.
- `is_set` - if provided and `true`, this property will be a collection of values, rather than single value (think product tags i.e. 'red', 'big', 'kitchen')
- `is_customer` - If provided and `true` this is property is a special customer property. Customer Properties unlike regular properties are associated with the customer rather than events in their history. Facts about a customer. These might be values like `age` or `country` or created by an ML model.

```
{
    "properties": [
        {
            "name": "{prop_name}",
            "type": "{text|int|double|bool}",
            "is_set": {optional: true|false},
            "is_customer": {optional: true|false},
        },
        {
            "name": "{prop_name}",
            "type": "{text|int|double|bool}",
            "is_set": {optional: true|false},
            "is_customer": {optional: true|false},
        },
        //etc
    ],
    "event_order": [
        "{event_name}",
        "{event_name}",
        //etc
    ]
}
```

Returns a 200 or 400 status code.

## GET /v1/table/{table} (describe table)

Returns JSON describing the table.

> :bulb: properties marked as `is_set` and/or `is_customer` will be identified in the property list.

```json
{
    "table": "highstreet",
    "properties": [
        {
            "name": "product_name",
            "type": "text"
        },
        {
            "name": "product_price",
            "type": "double"
        },
        {
            "name": "product_shipping",
            "type": "double"
        },
        {
            "name": "shipper",
            "type": "text"
        },
        {
            "name": "total",
            "type": "double"
        },
        {
            "name": "shipping",
            "type": "double"
        },
        {
            "name": "product_tags",
            "type": "text",
            "is_set": true
        },
        {
            "name": "product_group",
            "type": "text"
        },
        {
            "name": "cart_size",
            "type": "int"
        },
        {
            "name": "age",
            "type": "int",
            "is_customer": true
        }
    ]
}
```

-   `type` can be `text|int|double|bool`.
-   `name` can be any string consisting of lowercase letters `a-z`, numbers `0-9`, or the `_`. Properties cannot start with number.

Returns a 200 or 400 status code.

## PUT /v1/table/{table}/property/{prop_name}?type={type}&is_set={is_set}

Adds a property to an existing table.

-   `type` can be `text|int|double|bool`.
-   `is_set` (optional) can be `true|false` and indicates that the property can contain multiple values per row.
-   `prop_name` can be any string consisting of lowercase letters `a-z`, numbers `0-9`, or the `_`. Properties cannot start with number.

Returns a 200 or 400 status code.

## DELETE /v1/table/{table}/property/{prop_name}

Removes a property from the table.

-   `prop_name` can be any string consisting of lowercase letters `a-z`, numbers `0-9`, or the `_`. Properties cannot start with number.

Returns a 200 or 400 status code.

## PUT /v1/trigger/{table}/{segment_name}/{subscriber_name}

To subscribe to segment changes, the segment must already exist.

The subscriber_name should be unique to it's purpose. You may have multiple independent subscriptions to
the same segment changes, each will receive it's own independent feed.

Your PUT body must contain information about your web-hook.

```
{
    "host": "host or ip",
    "port": 80,
    "path": "/"
}
```

> **Note**: Messages can use a lot of memory, the default retention period is 3 hours, but you can change this
> by passing `"retention": {milliseconds_to_retain}` in the PUT body.

> **Note 2**: OpenSet will test your connection sending an empty `"messages"` reply. Please be sure your web-hook is running at the time of subscription or it will be rejected.

A payload with data will provide CGI parameters telling you about
the size of the payload, which segment it originated from, what the subscriber name was and lastly how many messages are in the backlog.

Example URL showing parameters passed to your web-hook after the `?`:

```
/?remaining=0&segment=products_outdoor&subscriber=sub1&count=2
```

Example body for web-hook call:

```
{
    "messages": [
        {
            "stamp": 1557088307114,
            "stamp_iso": "2019-05-05T20:31:47.114Z",
            "uid": "klara",
            "state": "entered"
        },
        {
            "stamp": 1557088307124,
            "stamp_iso": "2019-05-05T20:31:47.124Z",
            "uid": "kyle",
            "state": "entered"
        }
    ]
}
```

# DELETE /v1/trigger/{table}/{subscriber_name}

Delete a segment trigger subscription.

# Queries

## POST /v1/query/{table}/event

Analytics are generated by calling the `event` endpoint. 

This will perform an event scanning query by executing the provided `OSL` script in the POST body as `text/plain`. The result will be in JSON and contain results or any errors produced by the query.

**query parameters:**

| param             | values            | note                                                                                                                                   |
| ----------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------|
| `debug=`          | `true/false`      | will return the assembly for the query rather than the results                                                                         |
| `segments=`       | `segment,segment` | comma separted segment list. Segment must be created with a `/segment` query (see next section). The segment `*` represents all people.|
| `sort=`           | `prop_name`       | sort by `select` property name or `as name` if specified. specifying `sort=group`, will sort the result set by using grouping names.     |
| `order=`          | `asc/desc`        | default is descending order.                                                                                                           |
| `trim=`           | `# limit`         | clip long branches at a certain count. Root nodes will still include totals for the entire branch.                                     |
| `str_{var_name}`  | `text`            | populates variable of the same name in the params block with a string value                                                            |
| `int_{var_name}`  | `integer`         | populates variable of the same name in the params block with a integer value                                                           |
| `dbl_{var_name}`  | `double`          | populates variable of the same name in the params block with a double value                                                            |
| `bool_{var_name}` | `true/false`      | populates variable of the same name in the params block with a boolean value                                                           |

**result**

200 or 400 status with JSON data or error.

## POST /v1/query/{table}/segment

This will perform an index counting query by executing the provided `OSL` script in the POST body as `text/plain`. The result will be in JSON and contain results or any errors produced by the query.

Unlike the `/events` query, segments created in by the `/counts` query are named and cached and can be used in subsequent `/counts` queries as well as to filter `/events` queries.

A single counts query can contain multiple sections to create multiple segments in one step.

**query parameters:**

| param    | values       | note                                                           |
| -------- | ------------ | -------------------------------------------------------------- |
| `debug=` | `true/false` | will return the assembly for the query rather than the results |

**post body:**

The post body can include multiple sections. The `@` decorator is used to define sections. The example below is using the sample `high_street` sample data to create two segments named `products_home` and `products_outdoor`.

The `params` on the `@segment` definition tell OpenSet to not-recalculate the segment if it's within the TTL, and that it's ok to use a cached version. It also tells OpenSet to refresh this segment about every 300 seconds.

| @segment param | values     | note                                                                                                                                                                                                                                                                        |
| -------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ttl=           | seconds    | The number of seconds a segment will exist (not implemented)                                                                                                                                                                                                                |
| refresh=       | seconds    | The number of seconds to wait before refreshing a segment (should be smaller than TTL)                                                                                                                                                                                      |
| use_cached=    | True/False | It's ok to return the last calculated value if it's within the refresh window (very fast)                                                                                                                                                                                   |
| on_insert=     | True/False | Evaluate segment the moment data is inserted. This is useful if you have a need for real-time notification and are using triggers. on_insert evaluation can slow down insert performance.                                                                                   |
| z_index=       | number     | Default value is 100. This sets the order which segments are evaluated relative to each other. If you are making new segments from old segment using commands like `intersection` or `union` you may want to specify a higher `z_index` than 100 to have them evaluate later|

> :bulb: Some segments can be fully derived by counting indexes without having to access customer row sets, these segments are nearly instantaneous. Adding sequence,  time constraints, or row level iteration will result in the segment generator executing the script against the event set (OpenSet will automatically determine if this is required). Event based segments will be slower if they cover a large cross-section of people in the database. 

Once segments are created they can be used by name in the `segments=` parameter of other types of queries.

The following creates two segments, which 

```ruby
@segment products_home use_cached=false refresh=5_minutes on_insert=true

# match one of these
if product_group.ever(any ['basement', 'garage', 'kitchen', 'bedroom', 'bathroom'])
  return(true)
end

@segment products_yard use_cached=True refresh=5_minutes on_insert=true

# match one of these
if product_group.ever(contains 'basement') || product_group.ever(contains 'garage')
  return(true)
end
```

**result:**

200 or 400 status with JSON data or error.

## GET /v1/query/{table}/property/{prop_name}

The property query allows you to query all the values within a named property in a table as well as perform searches and numeric grouping.

**query parameters:**

| param             | values               | note                                                                                                                                                             |
| ----------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `segments=`       | `segment,segment`    | comma separted segment list. Segment must be created with a `counts` query. The segment `*` represents all people.                                               |
| `order=`          | `asc/desc`           | default is descending order.                                                                                                                                     |
| `trim=`           | `# limit`            | clip long branches at a certain count. Root nodes will still include totals for the entire branch.                                                               |
| `gt=`             | `#`                  | return values greater than `#`                                                                                                                                   |
| `gte=`            | `#`                  | return values greater than or equal to `#`                                                                                                                       |
| `lt=`             | `#`                  | return values less than `#`                                                                                                                                      |
| `lte=`            | `#`                  | return values less than or equal to `#`                                                                                                                          |
| `eq=`             | `#`                  | return value equal to `#`                                                                                                                                        |
| `between=` `and=` | `#` and `#`          | return value greater than or equal to `#` and less than the second number provided by `and=`                                                                     |
| `rx=`             | `regular expression` | return values matching an`ECMAScript` style regex expression. Check this great [cheet sheet](http://cpprocks.com/regex-cheatsheet/).                             |
| `sub=`            | `text`               | return value containing the search string in `sub`                                                                                                               |
| `bucket=`         | `#`                  | cluster values by `#`, all user counts will remain distinct for each group. Useful for creating histograms or condensing results (i.e. values to nearest dollar) |

**result**

200 or 400 status with JSON data or error.

## GET /v1/query/{table}/customer

Returns the event sequence for an individual customer.

> :pushpin: If events contain complex data (i.e. sub values), OpenSet will re-condense the data by folding up data permeations generated on insert. The folded row may be grouped differently than the one provided to `/insert` but will be logically identical.

**query parameters:**

| param  | values   | note                                                  |
| ------ | -------- | ----------------------------------------------------- |
| `sid=` | `string` | If you are using textual IDs use the `sid=` parameter |
| `id=`  | `number` | If you are using numeric IDs use the `id=` parameter  |

**result**

200 or 400 status with JSON data or error.

## POST /v1/query/{table}/histogram/{name}

This will generate a histogram using`OSL` script in the POST body as `text/plain`. The result will be in JSON and contain results or any errors produced by the query.

If `bucket=` is provided in the query, then missing values will be zero-filled. The default is to fill between the min and max value in the set. `min=` can be used to override minimum fill. `max=` can be used to override maximum fill. If `max=` is provided, all values over the max will be totaled into the value of max (this helps eliminate long tails).

**URL**

-   `table` is the name of the table to query
-   `name` is the name of the dataset in the resulting JSON.

**body**

Body is a valid OSL script, with no `select` section, and using `return(#)` to return the calculated value to the histogram.

```ruby
# return number of weeks since last event
return( to_weeks(now - last_stamp) )
```

**query parameters:**

|  param             | values            | note                                                                                                              |
| ------------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------|
|  `debug=`          | `true/false`      | will return the assembly for the query rather than the results                                                    |
|  `segments=`       | `segment,segment` | comma separted segment list. Segment must be created with a `counts` query. The segment `*` represents all people.|
|  `order=`          | `asc/desc`        | default is descending order.                                                                                      |
|  `trim=`           | `# limit`         | clip long branches at a certain count. Root nodes will still include totals for the entire branch.                |
|  `str_{var_name}`  | `text`            | populates variable of the same name in the params block with a string value                                       |
|  `int_{var_name}`  | `integer`         | populates variable of the same name in the params block with a integer value                                      |
|  `dbl_{var_name}`  | `double`          | populates variable of the same name in the params block with a double value                                       |
|  `bool_{var_name}` | `true/false`      | populates variable of the same name in the params block with a boolean value                                      |
|  `bucket=`         | `#`               | cluster values by `#`, all user counts                                                                            |
|  `min=`            | `#`               | set histogram fill to `min=#`. This will create zero counted branches back to the min value.                      |
|  `max=`            | `#`               | clip histogram fill at `max=#`. The value in max will contain the sum of all nodes `>=` to the `max=` value.      |
|  `foreach=`        | `property name`   | calls provided OSL repeatedly filling the script variable `each_value` with each value in the property.             |

**result**

200 or 400 status with JSON data or error.

## POST /v1/query/{table}/batch (experimental)

Run multiple segment, property and histogram queries at once, generate a single result. Including `foreach` on histograms.

Example post data using `highstreet` sample data:

```ruby
@segment products_home use_cached=false refresh=5_minutes on_insert=true

# match one of these
if product_group.ever(any ['basement', 'garage', 'kitchen', 'bedroom', 'bathroom'])
  return(true)
end

@segment products_yard use_cached=True refresh=5_minutes on_insert=true

# match one of these
if product_group.ever(contains 'basement') || product_group.ever(contains 'garage')
  return(true)
end

@use products_home products_yard

@property product_name

@property product_group

@property product_price bucket=50

@histogram customer_value bucket=50
  return(sum(total) where event.is(== "purchase"))

@histogram days_since
  return( to_day(now - last_event) )

@histogram total_by_shipper foreach=shipper bucket=100 min=0 max=1000
  return( sum(total) where shipper.is(== each_value) )

```

# Internode (internode node chatter)

Don't call these from client code.
The `/v1/internode` REST interface is used internally to maintain a proper functioning cluster.

## GET /v1/cluster/is_member

This will return a JSON object informing if the node is already part of a cluster

```
{
    "part_of_cluster": {true|false}
}
```

## POST /v1/internode/join_to_cluster

Joins an empty node to the cluster. This originates with the `/v1/cluster/join` endpoint. `/v1/cluster/join` will issue a `/v1/interndoe/is_cluster_member` and verify the certificate before this endpoint (`/v1/internode/join_to_cluster`) is called.

This endpoint transfers information about tables, triggers, and partition mapping.

## POST /v1/internode/add_node

Dispatched to all nodes by `/v1/cluster/join` to inform all nodes in the cluster that a new node has been joined to the cluster. Nodes receiving `add_node` will adjust their node mapping.

At this point the node will be empty. The `sentinel` for the elected node will start balancing to this node shortly after this dispatch.

## POST /v1/internode/map_change

Dispatched by `sentinel` when node mapping and membership have changed. This is the basic mechanism that keeps cluster topology in sync.

## PUT /v1/internode/transfer?partition={partition_id}&node={dest_node_name}

This initiates a partition transfer. The node containing the partition to transfer is contacted directly. It is provided the `partition_id` to transfer and the `dest_node_name` to send it to.

This will result in potentially several transfers, one for each table using `POST /v1/internode/transfer`. The recipient receives `partition_id` and `table_name` for each block.

After a successful transfer the `sentinel` will send a `POST /v1/internode/map_change` request to tell the cluster that the partition is available.

## POST /v1/internode/transfer?partition={partition_id}&table={table_name}

Transfers packed `binary` data for partition. Partition is `partition_id` is passed in URL as an integer.

# Other

## GET /ping

If the node is runing, this will respond with 200 OK and JSON:

```
{
  "pong": true
}
```

## GET /status

returns information about cluster state and fault tolerance.