#pragma once

#include "rpc_global.h"

namespace openset::comms
{

    class RpcInternode
    {
    public:
        // POST /v1/internode/join_to_cluster
        static void join_to_cluster(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // GET /v1/cluster/is_member
        static void is_member(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // GET /v1/cluster/is_member
        static void add_node(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/internode/map_change
        static void map_change(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // PUT /v1/internode/transfer?partition={partition_id}&node={node_name}
        static void transfer_init(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/internode/transfer?partition={partition_id}&table={table_name}
        static void transfer_receive(const openset::web::MessagePtr& message, const RpcMapping& matches);
    };
}