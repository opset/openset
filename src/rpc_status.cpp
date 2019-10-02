#include "rpc_status.h"

#include "cjson/cjson.h"

#include "common.h"
#include "rpc_global.h"
#include "sentinel.h"
#include "database.h"
#include "internoderouter.h"
#include "http_serve.h"

void openset::comms::RpcStatus::status(const openset::web::MessagePtr & message, const RpcMapping & matches)
{
    auto doc = openset::globals::sentinel->getPartitionStatus();
    auto tables = openset::globals::database->getTableNames();

    auto statusNode = doc.setObject("status");
    statusNode->set("init", globals::running->state == openset::config::NodeState_e::active);
    statusNode->set("cluster_complete", openset::globals::sentinel->isClusterComplete());
    statusNode->set("redundancy", openset::globals::sentinel->getRedundancyLevel());
    statusNode->set("tolerance", openset::globals::sentinel->getFailureTolerance());
    statusNode->set("balanced", openset::globals::sentinel->isBalanced());
    statusNode->set("sentinel", globals::mapper->getRouteName(openset::globals::sentinel->getSentinel()));
    statusNode->set("tables", static_cast<int>(tables.size()) );

    auto tableNode = doc.setArray("tables");

    for (auto &t : tables)
        tableNode->push(t);

    message->reply(http::StatusCode::success_ok, doc);
}
