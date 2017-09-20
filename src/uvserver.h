#pragma once

#include "common.h"

#include <mutex>
#include <queue>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>

#include "internodecommon.h"
#include "internodemessage.h"
#include "heapstack/heapstack.h"

#include <uv.h>

namespace openset
{
	namespace comms {

		class InboundConnection;
		class uvServer;
	}
}

namespace openset
{
	namespace comms {

		/*
		 *  InboundConnectionWorkers are bound to threads.
		 *
		 *  They are reusable and essentially make up a pool
		 *  of workers to process InboundConnections served up by
		 *  the uvServer.
		 *  
		 *  in uvServer Class there are:
		 *      handlerQueue, recycleQueue and runningQueue
		 *      
		 *  these contain connections throughout their lifecycle
		 *  where they can migrate between these queues.
		 *  		 
		 */
		class InboundConnectionWorkers
		{
		public:
			uvServer* server;
			InboundConnection* handler;
			condition_variable ready;
			int instance;

			explicit InboundConnectionWorkers(uvServer* serverObj, int instance);
			~InboundConnectionWorkers();

			//! wait is the thread worker.
			void threadWorker() noexcept;
		};

		class InboundConnection
		{
		public:

			HeapStack heap;
			uv_stream_t* client;

			uvServer* server;
			RouteHeader_s requestHead;
			RouteHeader_s responseHead;

			char* responseBuffer;
			int32_t responseLength;

			HeapStack* responseHeap;

			bool dropped; // has this handler been closed/errored
			bool holdDropped; // don't recycle on error (at least not until this is set false)

			uv_async_t* asyn;

			bool pushed;
			bool isEOF;

			InboundConnection(uvServer* uvSrv, uv_stream_t* uvClient);
			~InboundConnection();

			void reset();
			void reuse(uv_stream_t* uvClient);

			static void done_send(uv_write_t* req, int status);
			void read(char* data, int len);

			std::string getValue(openset::mapping::rpc_e& channel);

			// returns a copy of the data in the uvConnection object
			char* getData(RouteHeader_s& header);
			//Block_s getValue();

			RouteHeader_s getResponseHeader() const
			{
				return responseHead;
			}

			static void on_close(uv_handle_t* client);
			static void on_handle_close(uv_handle_t* handle);
			static void on_shutdown(uv_shutdown_t* sd, int status);

			void shutdown() const;

			static void asyncSend(uv_async_t* handle);

			void respond(comms::RouteHeader_s routing, char* data);
			void respond(comms::RouteHeader_s routing, const string& message);
			void respond(comms::RouteHeader_s routing, HeapStack* heapStack);
		};

		using RpcCallBack = function<void(openset::comms::Message*)>;

		class uvServer
		{
		public:
			unordered_map<openset::mapping::rpc_e, RpcCallBack> handlers;

			//vector<thread>     threads;

			mutex handlerLock;
			condition_variable jobReady;

			atomic<int32_t> queueSize;
			atomic<int32_t> runs;
			atomic<int32_t> available;

			// where active connections with
			std::queue<InboundConnection*> availableQueue;
			std::queue<InboundConnection*> recycleQueue;
			std::unordered_set<InboundConnection*> runningQueue;

			uvServer();
			~uvServer();

			void handler(openset::mapping::rpc_e handlerType, RpcCallBack cb);

			InboundConnection* newConnection(uv_stream_t* client);

			static void onNewConnection(uv_stream_t* server, int status);
			static void onConnectClose(uv_handle_t* client);
			static void onData(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf);
			static void uvFree(const uv_buf_t* buf);
			static void uvAlloc(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);

			void serve(string IP, int32_t port, int32_t workerPool);
		};
	};

	namespace globals
	{
		extern openset::comms::uvServer* server;
	};
};


