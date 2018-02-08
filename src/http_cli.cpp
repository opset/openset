#include "http_cli.h"
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

void openset::web::Rest::request(const string& method, const string& path, const QueryParams& params,
                                 const char* payload, const size_t length, RestCbJson cb)
{
    csLock lock(cs);

    stringstream buffer(stringstream::in | stringstream::out | stringstream::binary);
    if (payload && length)
        buffer.write(payload, length);

    const auto url = path + makeParams(params);

    Logger::get().debug("http://" + host + url);

    client.request(
        method, 
        url, 
        buffer, 
        {},
        [cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
        {
            auto length = response->content.size();
            auto data = static_cast<char*>(PoolMem::getPool().getPtr(length));
            response->content.read(data, length);

            auto status = response->status_code.length() && response->status_code[0] == '2'
                ? http::StatusCode::success_ok
                : http::StatusCode::client_error_bad_request;
            auto isError = (status != http::StatusCode::success_ok || ec);
            cb(status, isError, cjson(string{data, length}, cjson::Mode_e::string));

            PoolMem::getPool().freePtr(data);
        }
    );

    client.io_service->reset();
    client.io_service->run();
}

void openset::web::Rest::request(const string& method, const string& path, const QueryParams& params,
                                 const char* payload, const size_t length, RestCbBin cb)
{
    csLock lock(cs);

    stringstream buffer(stringstream::in | stringstream::out | stringstream::binary);
    if (payload && length)
        buffer.write(payload, length);

    const auto url = path + makeParams(params);

    Logger::get().debug("http://" + host + url);

    client.request(
        method, 
        url, 
        buffer, 
        {},
        [cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
        {
            auto length = response->content.size();
            auto data = length ? static_cast<char*>(PoolMem::getPool().getPtr(length)) : nullptr;
            response->content.read(data, length);
            auto status = response->status_code.length() && response->status_code[0] == '2'
                ? http::StatusCode::success_ok
                : http::StatusCode::client_error_bad_request;
            auto isError = (status != http::StatusCode::success_ok || ec);
            cb(status, isError, data, length);
        }
    );

    client.io_service->reset();
    client.io_service->run();
}
