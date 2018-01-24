#include "rpc_global.h"
#include "../test/test_zorder.h"

using namespace openset::comms;


void openset::comms::RpcError(openset::errors::Error error, const openset::web::MessagePtr message)
{
    message->reply(openset::http::StatusCode::client_error_bad_request, error.getErrorJSON());
}

ForwardStatus_e openset::comms::ForwardRequest(const openset::web::MessagePtr message)
{
    if (!openset::globals::mapper->routes.size())
        return ForwardStatus_e::error;

    if (message->getParamBool("forwarded"))
        return ForwardStatus_e::alreadyForwarded;

    auto newParams = message->getQuery();
    newParams.emplace("forwarded"s, "true"s);

    // broadcast to the cluster
    auto result = openset::globals::mapper->dispatchCluster(
        message->getMethod(),
        message->getPath(),
        newParams,
        message->getPayload(),
        message->getPayloadLength(),
        true);

    /*
    * If it's not an error we reply with the first response received by the cluster
    * as they are going to be all the same
    */
    if (!result.routeError)
    {
        cjson response;
        cjson::Parse(
            string{
                result.responses[0].data,
                result.responses[0].length
            },
            &response
        );

        message->reply(openset::http::StatusCode::success_ok, response);
    }
    else // if it's an error, reply with generic "something bad happened" type error
    {

        auto nodeFail = false;

        // try to capture a json error that has perculated up from the forked call.
        if (result.responses[0].data &&
            result.responses[0].length &&
            result.responses[0].data[0] == '{')
        {
            cjson error(std::string(result.responses[0].data, result.responses[0].length), result.responses[0].length);

            if (error.xPath("/error"))
                message->reply(openset::http::StatusCode::client_error_bad_request, error);
            else
                nodeFail = true;
        }
        else
            nodeFail = true;

        if (nodeFail)
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::route_error,
                    "potential node failure - please re-issue the request" },
                    message);
        }
    }

    openset::globals::mapper->releaseResponses(result);

    return (result.routeError) ? ForwardStatus_e::error : ForwardStatus_e::dispatched;
}
