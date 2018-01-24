#pragma once

#include "rpc_global.h"
#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

/*
Control - This class is responsible for mapping
communications from the usvserver to jobs (Cells)
within the DB.

This is basically the big sandwich between three different
parts of the database:

1. Comms (OpenSet::server)
2. Multitasking (OpenSet::async)
3. DB Engine (OpenSet::db)
4. Triggers (OpenSet::trigger)

These classes ensure that when comms happen, cells are created
in the multitasking engine for the correct partitions and that
DB engine objects are mapped to these cells, and that partition
data is created(*).

Most of this work is done with cell factories and shuttle objects.
Cells are assigned to loops which are assigned to partitions to provide
thread separation. Shuttles are used to gather results, and relay
responses back down the originating connection (comms).

* The database mirrors the concept of partitions with the multi-tasking
engine. Partition X in one is also Partition X in another when it comes
to thread isolation. However, functionally they are completely separate.
*/

using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{

    class RpcCluster
    {
    public:
        // PUT /v1/cluster/init?partitions={#}
        static void init(const openset::web::MessagePtr message, const RpcMapping& matches);
        // PUT /v1/cluster/join?host={host|ip}&port={port}
        static void join(const openset::web::MessagePtr message, const RpcMapping& matches);
    };

}