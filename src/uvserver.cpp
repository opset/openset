#include <thread>
#include "logger.h"
#include "uvserver.h"
#include "heapstack/heapstack.h"
#include "sba/sba.h"
#include "internoderouter.h"
#include "sentinel.h"

namespace openset
{
	namespace globals
	{
		openset::comms::uvServer* server;
	};
};

using namespace openset::comms;
using namespace std;

uv_loop_t* loop;
uvServer* __serverHandler;
queue<char*> uv_server_buffers;

InboundConnection* toHandler(uv_stream_t* client)
{
	return static_cast<InboundConnection*>(client->data);
}

// our worker thread
InboundConnectionWorkers::InboundConnectionWorkers(uvServer* serverObj, int instance) :
	server(serverObj),
	instance(instance),
	handler(nullptr)
{}

InboundConnectionWorkers::~InboundConnectionWorkers()
{}

void InboundConnectionWorkers::threadWorker() noexcept
{

	while (true)
	{
		// wait on accept handler

		{ // scope for lock

			// this worker is available
			++server->available;

			// wait on a job to appear, verify it's there, and run it.
			unique_lock<std::mutex> waiter(server->handlerLock);
			if (server->queueSize == 0)
				server->jobReady.wait(waiter,
					      [&]()
					      { // oh yeah a lambda!
						      return static_cast<int32_t>(server->queueSize) != 0;
					      });

			--server->queueSize;

			if (server->availableQueue.size() == 0)
			{
				// soft error
				cout << "!!! empty queue" << endl;
				continue;
			}

			handler = server->availableQueue.front();
			server->availableQueue.pop();
		} // unlock out of scope

		--server->available;
		++server->runs;

		//rpc_e channel
		
		//if (handler->requestHead.route &&
			//globals::mapper->getMessage(handler->requestHead.))

		//route, slotNumber
		mapping::MessageID messageID = { handler->requestHead.route, handler->requestHead.slot };
		auto message = globals::mapper->getMessage(messageID);

		// PING/PONG - this is the keep-alive. InternodeOutbound objects
		// will make PING requests on idle channels to test for dropped connections
		// this will reply with a PONG
		if (!message && 
			handler->requestHead.route == 0 && 
			handler->requestHead.rpc == static_cast<int32_t>(openset::mapping::rpc_e::inter_node_healthcheck) &&
			handler->requestHead.length == 13)
		{
			if (openset::globals::sentinel->isSentinel() &&
				openset::globals::sentinel->isDeadRoute(handler->requestHead.replyTo))
			{
				handler->requestHead.replyTo = 0;
				RouteHeader_s tempHead;
				tempHead.rpc = 500;
				handler->respond(tempHead, std::string("{\"pong\":false}"));
			}
			else
			{
				handler->requestHead.replyTo = 0;
				RouteHeader_s tempHead;
				handler->respond(tempHead, std::string("{\"pong\":true}"));
			}
			continue;
		}

		// Here we check to see if this message is from an SDK client or if it's
		// from another node. messageID.first will have a value if its from another node
		//
		// If it's from another node we reply ACK immediately to the remote InternodeOutbound
		// object, and process the message. The response will be send back out of this node
		// on a local InternodeOutbound Object (this frees up the remote InternodeOutbound
		// object to route more messages).
		if (message && messageID.first != 0)
		{			
			RouteHeader_s tempHead;
			message->onResponse(handler->getData(tempHead), handler->requestHead.length);
			handler->respond(tempHead, std::string("{\"ack\":true}"));
			continue;
		}

		message = new openset::comms::Message(globals::mapper, handler);

		if (!message)
			continue;
		
		// map the channel to the handler
		auto iter = server->handlers.find(message->getRPC());
		if (iter != server->handlers.end())
		{
			// call back the handler
			auto cb = iter->second;
			cb(message);
		}
		else
		{
			handler->respond(RouteHeader_s{}, std::string("{\"error\":\"no handler\"}"));
		}

		if (handler->isEOF)
			cout << "IS EOF" << endl;

		//server->jobReady.notify_one();
	}
};

InboundConnection::InboundConnection(uvServer* uvSrv, uv_stream_t* uvClient) :
	client(uvClient),
	server(uvSrv),
	responseBuffer(nullptr),
	responseLength(0), 
	responseHeap(nullptr),
	dropped(false),
	holdDropped(false),
	asyn(nullptr),
	pushed(false),
	isEOF(false)
{};

InboundConnection::~InboundConnection()
{
	if (responseBuffer)
	{
		PoolMem::getPool().freePtr(responseBuffer);
		responseBuffer = nullptr;
	}
}

void InboundConnection::reset()
{
	heap.reset();
	dropped = false;
	pushed = false;
	isEOF = false;
}

void InboundConnection::reuse(uv_stream_t* uvClient)
{
	client = uvClient;
	reset();

	if (responseBuffer)
	{
		PoolMem::getPool().freePtr(responseBuffer);
		responseBuffer = nullptr;
		responseLength = 0;
	}

	if (responseHeap)
	{
		delete responseHeap;
		responseHeap = nullptr;
	}

}

void InboundConnection::on_close(uv_handle_t* client)
{
	auto handler = static_cast<InboundConnection*>(client->data);
	free(client);

	handler->dropped = true;

	if (handler->responseBuffer)
	{
		PoolMem::getPool().freePtr(handler->responseBuffer);
		handler->responseBuffer = nullptr;
		handler->responseLength = 0;
	}

	if (handler->responseHeap)
	{
		delete handler->responseHeap;
		handler->responseHeap = nullptr;
	}
}

void InboundConnection::on_handle_close(uv_handle_t* handle)
{
	free(handle);
}

void InboundConnection::on_shutdown(uv_shutdown_t* sd, int status)
{
	uv_close(reinterpret_cast<uv_handle_t*>(sd->handle), on_close);
	free(sd);
}

void InboundConnection::shutdown() const
{
	auto sd = static_cast<uv_shutdown_t*>(malloc(sizeof(uv_shutdown_t)));
	uv_shutdown(sd, client, InboundConnection::on_shutdown);
}

// returns a copy of the data in the uvConnection object
char* InboundConnection::getData(RouteHeader_s& header)
{
	auto tBuff = heap.flatten();

	header.length = 0;

	if (!tBuff)
		return nullptr;
	
	header = *reinterpret_cast<RouteHeader_s*>(tBuff);
	auto resultBuffer = recast<char*>(PoolMem::getPool().getPtr(header.length+1));

	// TODO - make a smarter version of flatten that takes some offset to avoid this double copy
	memcpy(resultBuffer, tBuff + sizeof(RouteHeader_s), header.length);

	resultBuffer[header.length] = 0; // null terminate
	PoolMem::getPool().freePtr(tBuff);

	heap.reset();

	return resultBuffer;
}

std::string InboundConnection::getValue(openset::mapping::rpc_e& channel)
{
	// TODO - optimize this if no flatten required
	auto tBuff = heap.flatten();

	if (!tBuff)
	{
		cout << "no data error" << endl;
		return "";
	}

	responseHead = *reinterpret_cast<RouteHeader_s*>(tBuff);
	channel = static_cast<openset::mapping::rpc_e>(responseHead.rpc);
	std::string message(tBuff + sizeof(RouteHeader_s), responseHead.length);
	PoolMem::getPool().freePtr(tBuff);

	heap.reset();

	return message;
}

void InboundConnection::read(char* data, int len)
{
	if (!len)
		return;

	auto write = heap.newPtr(len);
	memcpy(write, data, len);

	// do we have a header?
	if (heap.getBytes() >= sizeof(RouteHeader_s))
	{
		auto head = reinterpret_cast<RouteHeader_s*>(heap.getHeadPtr());

		// we have a fully formed block.
		if (!pushed && heap.getBytes() == head->length + sizeof(RouteHeader_s))
		{
			requestHead = *head; // this is what external classes will access for routing
			
			asyn = static_cast<uv_async_t*>(malloc(sizeof(uv_async_t)));
			uv_async_init(loop, asyn, InboundConnection::asyncSend);
			pushed = true;
			{
				lock_guard<std::mutex> lock(server->handlerLock);
				server->availableQueue.push(this);
				++server->queueSize;
			}
			server->jobReady.notify_one();
		}
	}
};

void InboundConnection::asyncSend(uv_async_t* handle)
{
	auto handler = static_cast<InboundConnection*>(handle->data);

	if (handler->isEOF)
	{
		uv_close(reinterpret_cast<uv_handle_t*>(handle), on_handle_close);
		return;
	}

	if (!handler->responseBuffer && !handler->responseHeap)
	{
		uv_close(reinterpret_cast<uv_handle_t*>(handle), on_handle_close);
		return;
	}

	if (uv_is_closing(reinterpret_cast<uv_handle_t *>(handler->client)))
	{
		handler->dropped = true;
		uv_close(reinterpret_cast<uv_handle_t*>(handle), on_handle_close);
		return;
	}

	auto response = static_cast<uv_write_t*>(malloc(sizeof(uv_write_t)));
	response->data = handler;

	if (handler->responseBuffer)
	{
		uv_buf_t buf[2];

		handler->requestHead.length = handler->responseLength;

		buf[0].base = reinterpret_cast<char*>(&handler->requestHead);
		buf[0].len = sizeof(RouteHeader_s);

		buf[1].base = handler->responseBuffer;
		buf[1].len = handler->responseLength;


		uv_write(response, handler->client, buf, 2, InboundConnection::done_send);
	}
	else
	{
		auto buf = recast<uv_buf_t*>(malloc( sizeof(uv_buf_t) * (handler->responseHeap->getBlocks() + 1)));

		handler->requestHead.length = handler->responseHeap->getBytes();
		
		buf[0].base = reinterpret_cast<char*>(&handler->requestHead);
		buf[0].len = sizeof(RouteHeader_s);

		auto iter = handler->responseHeap->firstBlock();

		auto idx = 1;

		while (iter)
		{
			buf[idx].base = iter->data;
			buf[idx].len = iter->endOffset;
	
			++idx;

			iter = iter->nextBlock;
		}

		uv_write(response, handler->client, buf, idx, InboundConnection::done_send);

		free(buf);
	}

	// we are free the async handle here, not the connection
	uv_close(reinterpret_cast<uv_handle_t*>(handle), on_handle_close);
}

void InboundConnection::respond(RouteHeader_s routing, char* data)
{
	if (isEOF)
		return;

	if (responseBuffer)
	{
		//delete[]responseBuffer;
		PoolMem::getPool().freePtr(responseBuffer);
		responseBuffer = nullptr;
	}

	if (responseHeap)
	{
		delete responseHeap;
		responseHeap = nullptr;
	}

	heap.reset();
	pushed = false;

	responseBuffer = data;
	responseLength = routing.length;

	requestHead = routing;

	// pointer to this object and this payload
	asyn->data = this;

	// wakes up the event loop, causes registered (in uv_async_init )static member 
	// async send to be called from the main loop
	uv_async_send(asyn); 
}

void InboundConnection::respond(RouteHeader_s routing, const string& message)
{
	int32_t length = message.length();
	auto messageBuffer = static_cast<char*>(PoolMem::getPool().getPtr(length));
	memcpy(messageBuffer, message.c_str(), length);
	routing.length = length;
	respond(routing, messageBuffer);
}

void InboundConnection::respond(RouteHeader_s routing, HeapStack* heapStack)
{

	if (responseBuffer)
	{
		PoolMem::getPool().freePtr(responseBuffer);
		responseBuffer = nullptr;
	}

	if (responseHeap)
	{
		delete responseHeap;
		responseHeap = nullptr;
	}

	responseLength = 0;
	responseHeap = heapStack;

	heap.reset();
	pushed = false;

	// set these so they go out with asyncSend
	requestHead = routing;

	// pointer to this object and this payload
	asyn->data = this;

	// wakes up the event loop, causes registered (in uv_async_init )static member 
	// async send to be called from the main loop
	uv_async_send(asyn);
}

void InboundConnection::done_send(uv_write_t* req, int status)
{
	InboundConnection* handler = toHandler(req->handle);
	free(req);

	if (handler->responseBuffer)
	{
		//delete[]handler->responseBuffer;
		PoolMem::getPool().freePtr(handler->responseBuffer);
		handler->responseBuffer = nullptr;
	}

	if (handler->responseHeap)
	{
		delete handler->responseHeap;
		handler->responseHeap = nullptr;
	}

	if (status == UV_ECANCELED)
	{
		cout << "set EOF in send" << endl;
		handler->isEOF = true;
		handler->shutdown();
	}
}

/*
	uvServer
	- creates a pool of worker threads
	- starts libuv
	- creates uvConnection(s) when connection accepts are made and
	  associates them with the data member of the uv_stream_t
	  for the new connection
	- maintains recycled memory (static members, global pool)
	- maintains recycled uvHandlers
*/

uvServer::uvServer() :
	queueSize(0),
	runs(0),
	available(0)
{
	__serverHandler = this;
	globals::server = this;
}

uvServer::~uvServer()
{}

void uvServer::handler(openset::mapping::rpc_e handlerType, RpcCallBack cb)
{
	handlers[handlerType] = cb;
}

InboundConnection* uvServer::newConnection(uv_stream_t* client)
{
	InboundConnection* conn;

	// look for running handlers that have been "dropped", and
	// recycle them.
	for (auto c = runningQueue.begin(); c != runningQueue.end();)
	{
		if ((*c)->dropped && !(*c)->holdDropped)
		{
			recycleQueue.push(*c);
			c = runningQueue.erase(c);
		}
		else
		{
			++c;
		}
	}

	if (!recycleQueue.empty())
	{
		conn = recycleQueue.front();
		recycleQueue.pop();
		conn->reuse(client);
		runningQueue.insert(conn);
	}
	else
	{
		conn = new InboundConnection(this, client);
		runningQueue.insert(conn);
	}

	return conn;
}

void uvServer::uvFree(const uv_buf_t* buf)
{
	if (buf->base)
		uv_server_buffers.push(buf->base);
}

void uvServer::uvAlloc(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	if (!uv_server_buffers.empty())
	{
		buf->base = uv_server_buffers.front();
		buf->len = suggested_size;
		uv_server_buffers.pop();
	}
	else
	{
		buf->base = new char[suggested_size];
		buf->len = suggested_size;
	}
}

void uvServer::onNewConnection(uv_stream_t* server, int status)
{
	if (status < 0)
	{
		fprintf(stderr, "New connection error %s\n", uv_strerror(status));
		// TODO error!
		return;
	}

	auto client = static_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
	uv_tcp_init(loop, client);
	if (uv_accept(server, reinterpret_cast<uv_stream_t*>(client)) == 0)
	{
		auto handler = __serverHandler->newConnection(reinterpret_cast<uv_stream_t*>(client));
		client->data = reinterpret_cast<char*>(handler);
		uv_tcp_nodelay(client, 1);
		uv_tcp_keepalive(client, 1, 1);
		uv_read_start(reinterpret_cast<uv_stream_t*>(client), uvServer::uvAlloc, uvServer::onData);
	}
	else
	{
		cout << "accept error" << endl;
		uv_close(reinterpret_cast<uv_handle_t*>(client), uvServer::onConnectClose);
	}
}

void uvServer::onData(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf)
{
	if (!client)
		return;

	auto handler = static_cast<InboundConnection*>(client->data);

	if (nread < 0)
	{
		/* usually just ECONRESET from dropped connections
		if (nread != UV_EOF)
			fprintf(stderr, "Read error %s\n", uv_err_name(nread));
		*/

		uvServer::uvFree(buf);
		handler->isEOF = true;
		handler->shutdown();

		return;
	}

	if (nread)
		toHandler(client)->read(buf->base, nread);

	uvServer::uvFree(buf);
}

void uvServer::onConnectClose(uv_handle_t* client)
{
	free(client);
}

/*
	this function will never return, but it will call all your
	mapped callbacks added with .handler.
*/
void uvServer::serve(string IP, int32_t port, int32_t workerPool)
{
	
	auto workerCount = std::thread::hardware_concurrency();

	vector<shared_ptr<InboundConnectionWorkers>> workers;
	vector<thread> threads;
	workers.reserve(workerCount);
	threads.reserve(workerCount);
		
	Logger::get().info("Creating " + to_string(workerCount) + " conduits...");
	// make vWorker instances and start their threads
	for (auto i = 0; i < workerCount; i++)
	{
		workers.emplace_back(std::make_shared<InboundConnectionWorkers>(this, i));
		threads.emplace_back(thread(&InboundConnectionWorkers::threadWorker, workers[i]));
	}

	// detach these threads, let them do their thing
	for (auto i = 0; i < workerCount; i++)
		threads[i].detach();
	

	/*
	auto worker = new uvWorker(this);
	thread messageHandler(&uvWorker::wait, worker);
	messageHandler.detach();
	*/

	loop = uv_default_loop();

	uv_tcp_t server;
	uv_tcp_init(loop, &server);
	sockaddr_in bind_addr;
	uv_ip4_addr(IP.c_str(), port, &bind_addr);

	// bind
	uv_tcp_bind(&server, reinterpret_cast<const sockaddr*>(&bind_addr), 0);
	uv_tcp_nodelay(&server, 1);
	uv_tcp_keepalive(&server, 1, 1);

	// listen
	auto r = uv_listen(
		reinterpret_cast<uv_stream_t*>(&server),
		200,
		uvServer::onNewConnection);

	if (r)
	{
		Logger::get().info("Could not start server on " + IP + ":" + port);
		return;
	}

	ThreadSleep(1000);
	Logger::get().info("Server listening on " + IP + ":" + port + ".");
	Logger::get().info("Waiting...");

	// Loop forever... never give up
	uv_run(loop, UV_RUN_DEFAULT);
}
