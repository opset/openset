#pragma once
#include "rpc_global.h"

#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
    class RpcRevent
    {
    public:
        // POST /v1/revent/{table}/trigger/{revent_name}
        static void revent_create(const openset::web::MessagePtr message, const RpcMapping& matches);
        // GET /v1/revent/{table}/trigger/{revent_name}
        static void revent_describe(const openset::web::MessagePtr message, const RpcMapping& matches);
        // DELETE /v1/revent/{table}/trigger/{revent_name}
        static void revent_drop(const openset::web::MessagePtr message, const RpcMapping& matches);
        // POST /v1/revent/{table}/trigger/{revent_name}/sub/{sub_name}
        static void revent_sub(const openset::web::MessagePtr message, const RpcMapping& matches);
    };
}