#pragma once

#include "rpc_global.h"
#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
    class RpcQuery
    {
    public:
        // POST /v1/query/{table}/event
        static void event(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/query/{table}/segment
        static void segment(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/query/{table}/property/{name}?{various optional query params}
        static void property(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // GET /v1/query/{table}/customer?{id|idstr}={user_id_key}
        static void customer(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/query/{table}/histogram/{name}
        static void histogram(const openset::web::MessagePtr& message, const RpcMapping& matches);
        // POST /v1/query/{table}/batch
        static void batch(const openset::web::MessagePtr& message, const RpcMapping& matches);
    };
}