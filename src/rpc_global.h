#pragma once

#include "shuttle.h"
#include "database.h"
#include "http_serve.h"

enum class ForwardStatus_e : int
{
    dispatched,
    alreadyForwarded,
    error
};

namespace openset::comms
{
    using RpcMapping = std::unordered_map<std::string, std::string>;
    using RpcHandler = std::function<void(const openset::web::MessagePtr, const RpcMapping&)>;
    // method, regex, handler function, regex capture index to RpcMapping container
    using RpcMapTuple = std::tuple<const std::string, const std::regex, const RpcHandler, const std::vector<std::pair<int, string>>>;

    void RpcError(openset::errors::Error error, const openset::web::MessagePtr message);
    ForwardStatus_e ForwardRequest(const openset::web::MessagePtr message);
}