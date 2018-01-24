#pragma once

#include "rpc_global.h"
#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
    class RpcTable
    {
    public:
        // POST /v1/table/{table}
        static void table_create(const openset::web::MessagePtr message, const RpcMapping& matches);
        // GET /v1/table/{table}
        static void table_describe(const openset::web::MessagePtr message, const RpcMapping& matches);
        // PUT /v1/table/{table}/column/{name}
        static void column_add(const openset::web::MessagePtr message, const RpcMapping& matches);
        // DELETE /v1/table/{table}/column/{name}
        static void column_drop(const openset::web::MessagePtr message, const RpcMapping& matches);
    };
}