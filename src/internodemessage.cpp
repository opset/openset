#include "internodemessage.h"
#include "internoderouter.h"
#include "uvserver.h"
#include "config.h"

namespace openset
{
	namespace comms
	{
		atomic<int64_t> msgsCreated{ 0 };
		atomic<int64_t> msgsDestroyed{ 0 };
	}
}

openset::comms::Message::Message() :
	mode(SlotType_e::none),
	routingId(openset::mapping::MessageID{ 0,0 }),
	replyRoute(0),
	rpc(openset::mapping::rpc_e::none),
	data(nullptr),
	length(0),
	stamp(Now()),
	mailbox(openset::globals::mapper),
	clientConnection(nullptr)
{
	++msgsCreated;
}

openset::comms::Message::Message(int64_t route, openset::mapping::rpc_e rpc, char* data, int64_t length, ReadyCB ready_cb) :
	Message()
{
	dispatch(route, rpc, data, length, ready_cb);
}

openset::comms::Message::Message(openset::mapping::Mapper* mailbox, InboundConnection* connection) :
	Message()
{
	if (!connection->requestHead.route) // no route
		clientConnection = connection;

	RouteHeader_s header;
	auto data = connection->getData(header);

	if (data)
		onMessage(header.route, header.replyTo, header.slot, cast<openset::mapping::rpc_e>(header.rpc), data, header.length);

	if (!clientConnection)
		connection->respond(connection->requestHead, std::string("{\"ack\":true}"));

	if (!data)
	{
		cout << "***************** bad routes" << endl;
		connection->respond(connection->requestHead, std::string("{\"nack\":true}"));
	}
}

openset::comms::Message::~Message()
{
	++msgsDestroyed;
	if (mailbox)
		mailbox->dereferenceMessage(routingId);

	if (data)
		PoolMem::getPool().freePtr(data);
	data = nullptr;
}

void openset::comms::Message::clear()
{
	if (data)
		PoolMem::getPool().freePtr(data);//delete[] data;
	data = nullptr;
	length = 0;
}

void openset::comms::Message::dispose() const
{
	this->mailbox->disposeMessage(this->routingId);
}

void openset::comms::Message::onResponse(char* data, int64_t length)
{
	clear(); // free up any resources currently allocated

	this->data = data;
	this->length = length;

	if (ready_cb)
		ready_cb(this);
}

void openset::comms::Message::onResponse(const char* data, int64_t length)
{
	clear(); // free up any resources currently allocated

	auto newData = recast<char*>(PoolMem::getPool().getPtr(length + 1));
	memcpy(newData, data, length);
	newData[length] = 0;

	this->data = newData;
	this->length = length;

	if (ready_cb)
		ready_cb(this);
}


void openset::comms::Message::onMessage(int64_t route, int64_t replyRoute, int64_t slot, openset::mapping::rpc_e rpc, char* data, int64_t length)
{
	clear(); // free up any resources currently allocated

			 // this came inbound from uv_server so it's of remote origin
	mode = SlotType_e::remote_origin;

	// get a message instance number
	if (route == 0 && slot == 0)
		routingId = { route, mailbox->getSlotNumber() };
	else
		routingId = { route, slot };

	this->replyRoute = replyRoute;
	this->data = data;
	this->length = length;
	this->rpc = rpc;

	// register this message in the mailbox
	{
		csLock lock(mailbox->cs); // lock 
		mailbox->messages[routingId] = this;
	}
}

void openset::comms::Message::reply(char* data, int64_t length)
{
	clear(); // free up any resources currently allocated
	
	// Is this the local loop (TO this node, FROM this node)
	if (routingId.first == globals::running->nodeId &&
		replyRoute == globals::running->nodeId)
	{
		clear();

		this->data = data;
		this->length = length;

		if (ready_cb)
			ready_cb(this);

		clear();
		dispose();
	}
	else if (clientConnection) // Is this a connection from the UVServer
	{
		RouteHeader_s header;

		header.route = routingId.first;
		header.slot = routingId.second;
		header.replyTo = replyRoute;
		header.rpc = 200;
		header.length = length;

		clientConnection->respond(header, data);

		// we just transferred ownership of this data to a uvConnection object
		// we will null out data, and length here so we don't accidentally delete
		// this data
		this->data = nullptr;
		this->length = 0;

		dispose(); // we dispose local messages after we `respond`
	}
	else // Is this a routed message
	{
		this->data = data;
		this->length = length;

		auto rt = mailbox->getRoute(replyRoute);

		if (rt)
			rt->request(this);
	}
}

void openset::comms::Message::reply(cjson* doc)
{
	int64_t length = 0;
	auto text = cjson::StringifyCstr(doc, length);
	reply(text, length);
}

void openset::comms::Message::dispatch(int64_t route, openset::mapping::rpc_e rpc, char* data, int64_t length, ReadyCB callback)
{
	clear();

	// this is a message originating on the local node
	mode = SlotType_e::local_origin;

	// get a message instance number
	auto slotNumber = mailbox->getSlotNumber();

	// assign an ID this message
	routingId = { route, slotNumber };

	this->replyRoute = globals::running->nodeId;
	this->rpc = rpc;
	this->data = const_cast<char*>(data);
	this->length = length;
	this->ready_cb = callback;

	auto rt = mailbox->getRoute(route);

	// register this message in the mailbox
	{
		csLock lock(mailbox->cs); // lock 
		mailbox->messages[routingId] = this;
	}

	if (rt)
		rt->request(this);
}

// const version copies block, because we won't own it.
void openset::comms::Message::dispatch(int64_t route, openset::mapping::rpc_e rpc, const char* data, int64_t length, ReadyCB callback)
{

	auto newData = recast<char*>(PoolMem::getPool().getPtr(length + 1));
	memcpy(newData, data, length);
	newData[length] = 0;

	dispatch(route, rpc, newData, length, callback);

}
