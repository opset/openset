#include <thread>
#include "internodeoutbound.h"
#include "internodecommon.h"
#include "internoderouter.h"
#include "config.h"
#include "uvserver.h"

openset::mapping::DNS dnsCache;

openset::mapping::OutboundClient::OutboundClient(int64_t destRoute, std::string host, int32_t port, bool direct) :
	isDirect(direct),
	routingTo(destRoute),
	host(host),
	port(port),
	connected(false),
	isLocalLoop(false),
	inDestroy(false),
	isDestroyed(false),
	backlogSize(0),
	lastRx(Now())
{
	if (!isDirect)
		startRoute();
}

void openset::mapping::OutboundClient::teardown()
{
	inDestroy = true;
	queueReady.notify_one();
}

void openset::mapping::OutboundClient::request(openset::comms::Message* message)
{
	csLock lock(cs); // ad a message within the lock
	backlog.emplace_back(message);
	++backlogSize;
	queueReady.notify_one();
}

void openset::mapping::OutboundClient::runLocalLoop()
{
	Logger::get().info("Created local pump for " +  globals::mapper->getRouteName(routingTo) + ".");

	while (true)
	{

		if (backlogSize == 0) // wait only if the queue is empty
		{
			unique_lock<std::mutex> lock(queueLock);
			queueReady.wait_for(lock, 500ms, [&]() {
				return backlogSize != 0;
			});
		}

		openset::comms::Message* message;

		{ // scoped lock - pop a message within the lock
			csLock lock(cs);

			if (!backlog.size())
				continue;

			message = backlog.front();
			backlog.pop_front();
			--backlogSize;
		}

		// TODO call the RPC handler...
		auto iter = globals::server->handlers.find(message->getRPC());
		if (iter != globals::server->handlers.end())
		{
			// call back the handler
			auto cb = iter->second;
			cb(message);
		}
		else
		{
			const char* err = "{\"error\":\"no handler\"}";
			message->onResponse(err, strlen(err));
		}
	}
}

void openset::mapping::OutboundClient::runRemote()
{
	Logger::get().info("Created remote pump for " + globals::mapper->getRouteName(routingTo) + " @ " + host + ":" + to_string(port));

	openConnection();

	auto retryCount = 0;

	lastRx = Now();

	while (true)
	{

		if (backlogSize == 0) // wait only if the queue is empty
		{
			unique_lock<std::mutex> lock(queueLock);
			queueReady.wait_for(lock, 500ms, [&]()
			{
				return backlogSize != 0;
			});
		}

		if (inDestroy)
		{
			closeConnection();
			isDestroyed = true;
			return;
		}

		while (!isOpen())
		{
			if (inDestroy) // check for to see if this connection is being terminated
			{
				closeConnection();
				isDestroyed = true;
				return;
			}

			if (retryCount == 3)
				continue; // don't try to reopoen it.. let it die

			if (openConnection())
			{
				retryCount = 0;
				idleConnection();
			}
			else
			{
				++retryCount;
				Logger::get().error("connect/retry node " + globals::mapper->getRouteName(routingTo) + " @ " + host + ":" + to_string(port) + " (try " + to_string(retryCount) + ")");
				ThreadSleep(100);				
			}

			if (isOpen())
				break;
		}

		

		if (!backlogSize)
		{
			idleConnection();
			continue;
		}

		openset::comms::Message* message;

		{ // scoped lock - pop a message within the lock
			csLock lock(cs);

			if (!backlog.size())
				continue;

			message = backlog.front();
			backlog.pop_front();
			--backlogSize;
		}

		openset::comms::RouteHeader_s header;
		header.route = message->routingId.first;
		header.slot = message->routingId.second;
		header.replyTo = globals::running->nodeId;
		header.rpc = cast<int32_t>(message->rpc);
		header.length = message->length;

		directRequest(header, message->data);

		// wait for an ACK... -1 in RPC means NAK of some sort
		char* data = nullptr;
		auto ack = waitDirectResponse(data);

		if (ack.isError()) // hmmm.... this is vague
		{
			{ // scoped lock
				csLock lock(cs);
				// TODO - what do we do?
				backlog.push_front(message); // re-queue this request - prolly?
				++backlogSize;
			}

			closeConnection();
		}
		else
		{
			// send is only called on messages with remote_origins when
			// reply is called, so, that means we are done with the message
			if (message->mode == openset::comms::SlotType_e::remote_origin)
				message->dispose();
		}
	}

	closeConnection();
}

void openset::mapping::OutboundClient::startRoute()
{

	// either start a local loop handler or a remote handler
	if (routingTo == globals::running->nodeId)
	{
		isLocalLoop = true;
		std::thread runner(&openset::mapping::OutboundClient::runLocalLoop, this);
		runner.detach();
	}
	else
	{
		std::thread runner(&openset::mapping::OutboundClient::runRemote, this);
		runner.detach();
	}
}

bool openset::mapping::OutboundClient::isDead() const
{
	if (isLocalLoop)
		return false;

	if (inDestroy || isDestroyed)
		return true;

	return (lastRx + 1500 < Now());

}

bool openset::mapping::OutboundClient::openConnection()
{
	if (connected)
		return true;

	this->sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

	if (this->sock == INVALID_SOCKET)
		return false;

	std::string ip;
	if (!dnsCache.lookup(host, port, ip))
	{
		Logger::get().error("could not resolve host '" + host + "'");
		return false;
	}
	
	auto flag = 1;
	sockaddr_in address;
	address.sin_family = AF_INET;
	inet_pton(AF_INET, ip.c_str(), &address.sin_addr.s_addr);
	address.sin_port = htons(port);

	if (connect(this->sock, recast<sockaddr*>(&address), sizeof(sockaddr)))
		return false;

	setsockopt(
		this->sock,
		IPPROTO_TCP,
		TCP_NODELAY,
		recast<char*>(&flag),
		sizeof(int32_t));

	connected = true;

	return true;
}

void openset::mapping::OutboundClient::closeConnection()
{
	dnsCache.remove(host);

	if (this->sock == 0)// || !this->_IsConnected )
		return;

	shutdown(this->sock, SD_BOTH);

#ifdef _MSC_VER
	closesocket(this->sock);
#else
	::close(this->sock);
#endif

	this->sock = 0;
	this->connected = false;
}

void openset::mapping::OutboundClient::idleConnection()
{
	if (!isOpen())
		return;

	char* data = nullptr;

	comms::RouteHeader_s pingHeader;
	pingHeader.length = 13;
	pingHeader.rpc = static_cast<int32_t>(rpc_e::inter_node_healthcheck);
	pingHeader.replyTo = openset::globals::running->nodeId;

	if (directRequest(pingHeader, pingBuffer) == -1 || !isOpen())
	{
		closeConnection();
		return;
	}

	auto response = waitDirectResponse(data, 1);

	if (response.rpc == 500)
	{
		Logger::get().fatal(false, "this node is no longer part of this cluster - reset this node");
		closeConnection();
	}

	if (!response.length)
		closeConnection();
	else
		lastRx = Now();

	if (data)
		delete[] data;
}

int32_t openset::mapping::OutboundClient::directRequest(comms::RouteHeader_s routing, const char* buffer)
{
	auto len = routing.length;

	if (!this->connected)
		return -1;

	if ((buffer == nullptr) && (len != 0))
		return -1;

	const char* headerPtr = recast<char*>(&routing);
	auto headerLen = sizeof(comms::RouteHeader_s);
	auto sent = 0;
	auto total = 0;

	routing.length = len;

	while (total < headerLen)
	{
		sent = send(this->sock, headerPtr, headerLen - total, MSG_NOSIGNAL);

		if (sent == SOCKET_ERROR)
		{
			this->closeConnection();
			return -1;
		}

		total += sent;
		headerPtr += sent;
	}

	if (buffer == nullptr)
		return 0;

	const char* bufferPtr = buffer;
	total = 0;

	while (total < len)
	{
		sent = send(this->sock, bufferPtr, len - total, MSG_NOSIGNAL);

		lastRx = Now();

		if (sent == SOCKET_ERROR)
		{
			this->closeConnection();
			return -1;
		}

		total += sent;
		bufferPtr += sent;
	}

	return total;

}

openset::comms::RouteHeader_s openset::mapping::OutboundClient::waitDirectResponse(char*& data, int64_t toSeconds)
{
	comms::RouteHeader_s result;

	int64_t start = Now();

	data = nullptr;

	if (!this->sock || !this->connected)
	{
		result.rpc = -1;
		return result;
	}

	/* set a timeout */

	if (!toSeconds)
		toSeconds = 15;

#ifdef _MSC_VER
	DWORD timeout = toSeconds * 1000;
#else
	const struct timeval timeout = { .tv_sec = toSeconds, .tv_usec = 0 };
#endif
	setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, recast<const char*>(&timeout), sizeof(timeout));

	auto headerPtr = reinterpret_cast<char*>(&result);

	auto headerLen = sizeof(comms::RouteHeader_s);
	auto total = 0;
	auto received = 0;

	while (total < headerLen)
	{
		received = recv(sock, headerPtr, headerLen - total, 0);

		lastRx = Now();

		if (received < 0)
		{
			this->closeConnection();
			delete[] data;
			data = nullptr;
			result.rpc = -1;
			return result;
		}

		total += received;
		headerPtr += received;
	}

	if (result.length)
	{
		data = new char[result.length];
		char* offset = data;
		total = 0;

		while (total < result.length)
		{
			received = recv(sock, offset, result.length - total, 0);

			if (received <= 0)
			{
				this->closeConnection();
				delete[] data;
				data = nullptr;
				result.rpc = -1;
				return result;
			}

			total += received;
			offset += received;
		}
	}

	return result;
}


