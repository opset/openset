## Cluster

#### PUT /v1/cluster/init?partitions={#}

Initializes a cluster (even a cluster of 1 needs initializing). 

This turns an unassigned/waiting node into a cluster sentinel. A node that is part of a cluster can invite other nodes to join that cluster.

`partitions` is specifies the number of shards the data will be divided into. Pick a number that is roughly 10-20% higher than the totals number of cores your envision being part of your final cluster. 

> :pushpin:the ideal partition size is the lowest possible number that will fit the size of your cluster in the long run. There is overhead incurred with each partition, but you also want to pick a number that will allow you to grow. Picking a number less than the number of processor cores in your cluster will __not__ allow you to reach peak performance.

#### PUT /v1/cluster/join?host={host|ip}&port={port}

__query_params:__
- host={name | ip}
- port={port} (optional, default is 8080)

This will issue a `/v1/internode/is_cluster_member` request, and if that returns `false` a `/v1/internode/join_to_cluster` request.


## Table

#### POST /v1/table/{table} (create table)

```JSON
{
    "columns": [
        {
            "name": "{column_name}",
            "type": "{text|int|double|bool}"
        },
        ...
    ],
    "z-order": [
        "{event_name}",
        "{event_name}",
        ...
    ]
}
```

#### GET /v1/table/{table} (describe table)

Returns JSON describing the table

#### PUT /v1/table/{table}/column/{column}:{type} (add column)

Adds a column to an existing table. Type can be `text|int|double|bool`

#### DELETE /v1/table/{table}/column/{column} (drop column)

Removes a column from the table. 

#### GET /v1/table/{table}/revent/{revent_name}

Describe a re-eventing trigger

#### POST /v1/table/{table}/revent/{revent_name}

Create a re-eventing trigger.

#### DELETE /v1/table/{table}/revent/{revent_name}

Describe a re-eventing trigger

## Query

#### POST /v1/query/{table}/events

This will perform an event scanning query by executing the provided `PyQL` script in the POST body as `text/plain`. The result will be in JSON and contain results or any errors produced by the query.

Available query strings options include:
- `debug=true` will return the assembly for the query rather than the results

#### POST /v1/query/{table}/counts

This will perform an index counting query by executing the provided `PyQL` script in the POST body as `text/plain`. The result will be in JSON and contain results or any errors produced by the query.

Unlike the `/events` query, segments created in by the `/counts` query are named and cached and can be used in subsequent `/counts` queries as well as to filter `/events` queries.

Available query strings options include:
- `debug=true` will return the assembly for the query rather than the results

## Internode (internode node chatter)

> :pushpin: Don't call these from client code. 

The `/v1/internode` REST interface is used internally to maintain a proper functioning cluster. 

#### GET /v1/cluster/is_member

This will return a JSON object informing if the node is already part of a cluster

```
{
    "part_of_cluster": {true|false}
}
```

#### POST /v1/internode/join_to_cluster

Joins an empty node to the cluster. This originates with the `/v1/cluster/join` endpoint. `/v1/cluster/join` will issue a `/v1/interndoe/is_cluster_member` and verify the certificate before this endpoint (`/v1/internode/join_to_cluster`) is called.

This endpoint transfers information about tables, triggers, and partition mapping.

#### POST /v1/internode/add_node

Dispatched to all nodes by ` /v1/cluster/join` to inform all nodes in the cluster that a new node has been joined to the cluster. Nodes receiving `add_node` will adjust their node mapping. 

At this point the node will be empty. The `sentinel` for the elected node will start balancing to this node shortly after this dispatch.

#### POST /v1/internode/map_change

Dispatched by `sentinel` when node mapping and membership have changed. This is the basic mechanism that keeps cluster topology in sync.

#### PUT /v1/internode/transfer?partition={partition_id}&node={dest_node_name}

This initiates a partition transfer. The node containing the partition to transfer is contacted directly. It is provided the `partition_id` to transfer and the `dest_node_name` to send it to. 

This will result in potentially several transfers, one for each table using `POST /v1/internode/transfer`. The recipient receives `partition_id` and `table_name` for each block.

After a successful transfer the `sentinel` will send a `POST /v1/internode/map_change` request to tell the cluster that the partition is available. 

#### POST /v1/internode/transfer?partition={partition_id}&table={table_name}

Transfers packed `binary` data for partition. Partition is `partition_id` is passed in URL as an integer.




