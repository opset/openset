#pragma once

#include <queue>
#include "server_http.hpp"
#include "sba/sba.h"
#include "cjson/cjson.h"
#include "threads/spinlock.h"
#include "asio.hpp"

namespace openset
{
	namespace globals
	{
		extern std::shared_ptr<asio::io_service> global_io_service;
	}
}

namespace openset
{
	namespace http = SimpleWeb;
}

namespace openset::web
{
	using ReplyCB = std::function<void(const http::StatusCode status, const char*, const size_t)>;

	class Message
	{
		http::CaseInsensitiveMultimap header;
		http::CaseInsensitiveMultimap query;
		std::string method;
		std::string path;
		std::string queryString;
		char* payload;
		size_t payloadLength;
		ReplyCB cb;
	public:
		Message(
			http::CaseInsensitiveMultimap header,
			http::CaseInsensitiveMultimap query,
			const std::string method,
			const std::string path,
			const std::string queryString,
			char* payload,
			const size_t payloadLength,
			const ReplyCB cb) :
			header(std::move(header)),
			query(std::move(query)),
			method(std::move(method)),
			path(std::move(path)),
			queryString(std::move(queryString)),
			payload(payload),
			payloadLength(payloadLength),
			cb(cb)
		{};

		~Message()
		{
			if (payload)
				PoolMem::getPool().freePtr(payload);
			payload = nullptr;
		}

		char* getPayload() const
		{
			return payload;
		}

		size_t getPayloadLength() const
		{
			return payloadLength;
		}

		const std::string& getMethod() const
		{
			return method;
		}

		const std::string& getPath() const
		{
			return path;
		}

		const std::string& getQueryString() const
		{
			return queryString;
		}

		const http::CaseInsensitiveMultimap& getQuery() const
		{
			return query;
		}

        bool isParam(const std::string name)
		{
            if (const auto found = query.find(name); found != query.end())
                return true;
            return false;
		}

		std::string getParamString(const std::string name, const std::string defaultValue = ""s)
		{
			if (const auto found = query.find(name); found != query.end())
				return found->second;
			return defaultValue;
		}

		bool getParamBool(const std::string name, const bool defaultValue = false)
		{
			if (const auto found = query.find(name); found != query.end())
				return (found->second == "1" || std::tolower(found->second[0]) == 't') ? true : false;
			return defaultValue;
		}

		int64_t getParamInt(const std::string name, const int64_t defaultValue = 0)
		{
			if (const auto found = query.find(name); found != query.end())
				return std::stoll(found->second);
			return defaultValue;
		}

		double getParamDouble(const std::string name, const double defaultValue = 0)
		{
			if (const auto found = query.find(name); found != query.end())
				return std::stod(found->second);
			return defaultValue;
		}

		cjson getJSON() const
		{
            if (!payload || !payloadLength)
            {
                cjson t;
                return t;
            }

			cjson json(string{ payload, payloadLength }, cjson::Mode_e::string);
			return json;
		}

		void reply(const http::StatusCode status, const char* replyData, const size_t replyLength) const
		{
			if (cb)
				cb(status, replyData, replyLength);
		}

		void reply(const http::StatusCode status, const std::string& message) const
		{
			if (cb)
				cb(status, &message[0], message.length());
		}

		void reply(const http::StatusCode status, const cjson& message) const
		{
			if (cb)
			{
				int64_t length;
				const auto buffer = cjson::stringifyCstr(&message, length, false);
				cb(status, buffer, length);
				cjson::releaseStringifyPtr(buffer);
			}
		}
	};

	using MessagePtr = const shared_ptr<openset::web::Message>;

	class HttpServe;

	class webWorker
	{
		HttpServe* server;
		int instance;
	public:
		webWorker(HttpServe* server, const int instance) :
			server(server),
			instance(instance)
		{};
		void runner();
	};

	class HttpServe
	{	
	public:
		atomic<int> messagesQueued{ 0 };
		atomic<int64_t> jobsRun{ 0 };
		CriticalSection messagesLock;
		queue<shared_ptr<Message>> messages;

		mutex readyLock;
		condition_variable messageReady;

		// worker pools
		vector<shared_ptr<webWorker>> workers;
		vector<thread> threads;

		HttpServe();

		void queueMessage(std::shared_ptr<Message> message);
		std::shared_ptr<Message> getQueuedMessage();

		template<typename T>
		void mapEndpoints(T& server);
		void makeWorkers();
		void serve(const std::string ip, const int port);
	};
}

