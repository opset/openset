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

		auto reply = [request, response](const char* data, size_t length)
		{
			http::CaseInsensitiveMultimap header;
			header.emplace("Content-Length", to_string(length));
			header.emplace("Content-Type", "application/json");
			response->write(http::StatusCode::success_ok, header);
			
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
				{
					cout << "!!! empty queue" << endl;
					continue;
				}

			} // unlock out of scope

			++server->jobsRun;

			openset::comms::Dispatch(message);
		}
	}

	HttpServe::HttpServe()
	{}

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
			response->write("{\"pong\":true}");
		};

		// default
		server.resource["^/status$"]["GET"] = [](SharedResponseT response, SharedRequestT request) {
			response->write("{\"status\":\"OK\"}");
		};
	}

	void HttpServe::makeWorkers()
	{
		const auto workerCount = std::thread::hardware_concurrency();

		workers.reserve(workerCount);
		threads.reserve(workerCount);

		Logger::get().info("Creating " + to_string(workerCount) + " conduits...");
		// make vWorker instances and start their threads
		for (auto i = 0; i < workerCount; i++)
		{
			workers.emplace_back(std::make_shared<webWorker>(this, i));
			threads.emplace_back(thread(&webWorker::runner, workers[i]));
		}

		// detach these threads, let them do their thing
		for (auto i = 0; i < workerCount; i++)
			threads[i].detach();
	}

	void HttpServe::serve(const std::string ip, const int port)
	{
		using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

		HttpServer server;
		server.config.port = port;
		server.config.address = ip;
		server.config.reuse_address = false; // we want an error if this is already going

		mapEndpoints(server);
		makeWorkers();

		server.default_resource["GET"] = [](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
			auto size = request->content.size();
			char buffer[1024];
			request->content.read(buffer, size);
			response->write("{\"error\":\"unknown request\""s);
		};

		std::thread server_thread([&server]() {
			// Start server
			std::cout << "Started" << std::endl;
			server.start(); // blocks
		});

		// make a client object for our Rest class (http_cli.h)		
		openset::globals::global_io_service = std::make_shared<asio::io_service>();
		
		Logger::get().info("REST server listening on "s + ip + ":"s + to_string(port) + "."s);

		server_thread.join();
	}
}