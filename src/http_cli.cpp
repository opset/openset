#include "http_cli.h"
#include <utility>
#include <string_view>
#include "logger.h"
#include "sba/sba.h"
#include "threads/locks.h"

string openset::web::Rest::makeParams(const QueryParams& params) 
{
    string result;

    // encode params
    auto first = true;
    for (auto& p : params)
    {
        if (first)
        {
            result += "?";
            first = false;
        }
        else
        {
            result += "&";
        }

        result += p.first + "=" + http::Percent::encode(p.second);
    }

    return result;
}

openset::web::Rest::Rest(const int64_t routeId, const std::string& server):
    client(server),
    host(server),
    routeId(routeId)
{
    //client.io_service = globals::global_io_service;
    client.config.timeout_connect = 2; // two seconds to connect or fail
    client.config.timeout = 30; // 30 seconds to connect or fail
}

void openset::web::Rest::request(const string& method, const string& path, const QueryParams& params,
                                 char* payload, const size_t length, const RestCbJson& cb)
{
    const SimpleWeb::string_view buffer(payload, length);
    const auto url = path + makeParams(params);

    client.request(
        method, 
        url, 
        buffer, 
        {},
        [cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
        {
            const auto length = response->content.size();
            const auto data = static_cast<char*>(PoolMem::getPool().getPtr(length));
            response->content.read(data, length);

            const auto status = response->status_code.length() && response->status_code[0] == '2'
                ? http::StatusCode::success_ok
                : http::StatusCode::client_error_bad_request;
            const auto isError = (status != http::StatusCode::success_ok || ec);
            cb(status, isError, cjson(data, length));

            PoolMem::getPool().freePtr(data);
        }
    );

    //client.io_service->reset();   
    if (client.io_service->stopped())
        client.io_service->restart();
    client.io_service->run();
}

void openset::web::Rest::request(const string& method, const string& path, const QueryParams& params,
                                 char* payload, const size_t length, const RestCbBin& cb)
{
    const SimpleWeb::string_view buffer(payload, length);
    const auto url = path + makeParams(params);

    client.request(
        method, 
        url, 
        buffer, 
        {},
        [cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
        {
            const auto length = response->content.size();
            const auto data = length ? static_cast<char*>(PoolMem::getPool().getPtr(length)) : nullptr;
            response->content.read(data, length);

            const auto status = response->status_code.length() && response->status_code[0] == '2'
                ? http::StatusCode::success_ok
                : http::StatusCode::client_error_bad_request;
            const auto isError = (status != http::StatusCode::success_ok || ec);

            cb(status, isError, data, length);
        }
    );

    //client.io_service->reset();
    if (client.io_service->stopped())
        client.io_service->restart();
    client.io_service->run();
}
