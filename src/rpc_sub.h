#pragma once
#include "rpc_global.h"

#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
    class RpcSub
    {
    public:
        // POST /v1/revent/{table}/trigger/{revent_name}
        //static void revent_create(const openset::web::MessagePtr message, const RpcMapping& matches);
        // GET /v1/revent/{table}/trigger/{revent_name}
        //static void revent_describe(const openset::web::MessagePtr message, const RpcMapping& matches);
        //
        // DELETE /v1/subscription/{table}/{segment_name}/{sub_name}
        static void sub_delete(const openset::web::MessagePtr message, const RpcMapping& matches);
        // POST /v1/subscription/{table}/{segment_name}/{sub_name}
        static void sub_create(const openset::web::MessagePtr message, const RpcMapping& matches);
    };
}