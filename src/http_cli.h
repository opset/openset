#pragma once

#include "common.h"
#include "http_serve.h"
#include "client_http.hpp"
#include "utility.hpp"
#include "cjson/cjson.h"
#include "threads/spinlock.h"

namespace openset
{
    namespace http = SimpleWeb;
}

namespace openset::web
{
	using RestCbJson = std::function<void(const http::StatusCode, const bool, const cjson)>;
	using RestCbBin = std::function<void(const http::StatusCode, const bool, char*, const size_t)>;
    using HttpClient = SimpleWeb::Client<http::HTTP>;
    using QueryParams = http::CaseInsensitiveMultimap;

    class Rest
    {
        CriticalSection cs;
        HttpClient client;
        string host;
        int64_t routeId;

        static std::string makeParams(const QueryParams& params);

    public:

        Rest(int64_t routeId, const std::string& server);;

        ~Rest() = default;

        int64_t getRouteId() const
        {
            return routeId;
        }

        void request(const std::string& method, const std::string& path, const QueryParams& params,
                     char* payload, size_t length, const RestCbJson& cb);

        void request(const std::string& method, const std::string& path, const QueryParams& params,
                     char* payload, size_t length, const RestCbBin& cb);
    };

    using RestPtr = shared_ptr<Rest>;
};
