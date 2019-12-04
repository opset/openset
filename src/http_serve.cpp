#include <memory>
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>

#include "threads/locks.h"
#include "server_http.hpp"

#include "logger.h"
#include "http_serve.h"
#include "http_cli.h"
#include "rpc.h"

using namespace std::string_literals;

namespace openset
{
    namespace http = SimpleWeb;
}

namespace openset
{
    namespace globals
    {
        std::shared_ptr<asio::io_service> global_io_service;
    }
}

namespace openset::web
{

    /* MakeMessage - magic
     *
     * This anonomizes the request objects (which might be HTTP or HTTPS objects)
     *
     * by making a message and attaching a callback (cb) with closures to the correct objects.
     */
    template<typename TRes, typename TReq>
    std::shared_ptr<Message> MakeMessage(TRes response, TReq request)
    {
        auto queryParts = request->parse_query_string();
        auto length = request->content.size();
        auto data = static_cast<char*>(PoolMem::getPool().getPtr(length));
        request->content.read(data, length);
        request->content.clear();

        auto reply = [request, response](http::StatusCode status, const char* data, size_t length)
        {
            http::CaseInsensitiveMultimap header;
            header.emplace("Content-Length", to_string(length));
            header.emplace("Content-Type", "application/json");
            header.emplace("Access-Control-Allow-Origin", "*");
            response->write(status, header);

            if (data)
                response->write(data, length);
        };

        return make_shared<Message>(request->header, queryParts, request->method, request->path, request->query_string, data, length, reply);
    }

    void webWorker::runner()
    {
        while (true)
        {
            // wait on accept handler

            std::shared_ptr<Message> message;

            { // scope for lock
                // wait on a job to appear, verify it's there, and run it.
                unique_lock<std::mutex> waiter(server->readyLock);
                if (server->messagesQueued == 0)
                    server->messageReady.wait(waiter,
                        [&]()
                { // oh yeah a lambda!
                    return static_cast<int32_t>(server->messagesQueued) != 0;
                });

                message = server->getQueuedMessage();
                if (!message)
                    continue;

            } // unlock out of scope

            ++server->jobsRun;

            openset::comms::Dispatch(message);
        }
    }

    void HttpServe::queueMessage(std::shared_ptr<Message> message)
    {
        csLock lock(messagesLock);
        ++messagesQueued;
        messages.emplace(message);
        messageReady.notify_one();
    }

    std::shared_ptr<Message> HttpServe::getQueuedMessage()
    {
        csLock lock(messagesLock);

        if (messages.empty())
            return nullptr;

        --messagesQueued;

        auto result = messages.front();
        messages.pop();
        return result;
    }

    template<typename T>
    void HttpServe::mapEndpoints(T& server)
    {

        using SharedResponseT = std::shared_ptr<typename T::Response>;
        using SharedRequestT = std::shared_ptr<typename T::Request>;

        server.resource["^/v1/.*$"]["GET"] = [&](SharedResponseT response, SharedRequestT request) {
            queueMessage(std::move(MakeMessage(response, request)));
        };

        server.resource["^/v1/.*$"]["POST"] = [&](SharedResponseT response, SharedRequestT request) {
            queueMessage(std::move(MakeMessage(response, request)));
        };

        server.resource["^/v1/.*$"]["PUT"] = [&](SharedResponseT response, SharedRequestT request) {
            queueMessage(std::move(MakeMessage(response, request)));
        };

        server.resource["^/v1/.*$"]["DELETE"] = [&](SharedResponseT response, SharedRequestT request) {
            queueMessage(std::move(MakeMessage(response, request)));
        };

        server.resource["^/ping$"]["GET"] = [&](SharedResponseT response, SharedRequestT request) {
            http::CaseInsensitiveMultimap header;
            header.emplace("Content-Type", "application/json");
            header.emplace("Access-Control-Allow-Origin", "*");

            response->write("{\"pong\":true}", header);
        };
    }

    void HttpServe::makeWorkers()
    {
        const auto workerCount = 8; // TODO make a switch std::thread::hardware_concurrency();

        workers.reserve(workerCount);
        threads.reserve(workerCount);

        for (auto i = 0; i < static_cast<int>(workerCount); i++)
        {
            workers.emplace_back(std::make_shared<webWorker>(this, i));
            threads.emplace_back(thread(&webWorker::runner, workers[i]));
        }

        Logger::get().info(to_string(workerCount) + " HTTP REST workers created.");

        // detach these threads, let them do their thing in the background
        for (auto i = 0; i < workerCount; i++)
            threads[i].detach();
    }

    void HttpServe::serve(const std::string& ip, const int port)
    {
        using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

        HttpServer server;

        server.config.port = port;
        server.config.address = ip;
        server.config.reuse_address = false; // we want an error if this is already going

        mapEndpoints(server);
        makeWorkers();

        server.default_resource["GET"] = [](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
            response->write("{\"error\":\"unknown request\""s);
        };

        server.on_error = [](shared_ptr<HttpServer::Request> /*request*/, const SimpleWeb::error_code & /*ec*/) {
            // Handle errors here
            // Note that connection timeouts will also call this handle with ec set to SimpleWeb::errc::operation_canceled
          };


        // Start server
        thread server_thread([&server]() {
            // Start server
            server.start();
        });

        Logger::get().info("HTTP REST server listening on "s + ip + ":"s + to_string(port) + "."s);

        ThreadSleep(250);

        // wait here forever
        server_thread.join();
    }
}