#pragma once

#include "rpc_global.h"

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
    class RpcStatus
    {
    public:
        // GET /v1/status
        static void status(const openset::web::MessagePtr& message, const RpcMapping& matches);
    };
}