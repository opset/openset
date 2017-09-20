#include <thread>
#include <string>
#include "file/file.h"
#include "config.h"

#include "internoderouter.h"
#include "internodemessage.h"
#include "internodeoutbound.h"

#include "uvserver.h"

namespace openset
{
	namespace globals
	{
		openset::mapping::Mapper* mapper;
	};
};

/*
 *  MAILBOX
 */

openset::mapping::Mapper::Mapper():
	slotCounter(1)
{
	globals::mapper = this; // set the global

	// change - load is only peformed on resume
	// loadRoutes();
	//addRoute("startup", globals::running->nodeId, globals::running->host, globals::running->port);
}

openset::mapping::Mapper::~Mapper() 
{}

int64_t openset::mapping::Mapper::getSlotNumber()
{
	++slotCounter;
	return slotCounter;
}

void openset::mapping::Mapper::removeRoute(int64_t routeId)
{
	csLock lock(cs); // lock 

	auto rt = routes.find(routeId);

	// is it missing?
	if (rt != routes.end())
	{
		rt->second->teardown();
		while (rt->first != globals::running->nodeId && !rt->second->isDestroyed)
			ThreadSleep(1);
		routes.erase(rt);

		// clear the name out - will be in dictionary
		names.erase(names.find(routeId));
	}
}

void openset::mapping::Mapper::addRoute(std::string routeName, int64_t routeId, std::string ip, int32_t port)
{
	csLock lock(cs); // lock 

	// name if first
	auto name = names.find(routeId);
	if (name == names.end())
		names.insert({ routeId, routeName }); // new name
	else
		name->second = routeName; // replace name

	auto rt = routes.find(routeId);
	// create it if it doesn't exist
	if (rt == routes.end())
		rt = routes.insert({ routeId, new OutboundClient(routeId, ip, port) }).first; // make it

}

std::string openset::mapping::Mapper::getRouteName(int64_t routeId)
{
	if (names.count(routeId))
		return names.find(routeId)->second;
	else
		return "startup";
}

openset::mapping::OutboundClient* openset::mapping::Mapper::getRoute(int64_t routeId)
{
	csLock lock(cs); // lock 

	auto rt = routes.find(routeId);

	if (rt == routes.end())
		return nullptr;

	return rt->second;
}

// send a message to a destination
openset::comms::Message* openset::mapping::Mapper::dispatchAsync(int64_t route, rpc_e rpc, char* data, int64_t length, openset::comms::ReadyCB callback)
{
	// check if there is a route here
	if (!getRoute(route))
		return nullptr;

	// dispatchAsync sets this to a message we originated (mode = MessageType_e::origin)
	return new openset::comms::Message(route, rpc, data, length, callback);
}

openset::comms::Message* openset::mapping::Mapper::dispatchAsync(int64_t route, rpc_e rpc, const char* data, int64_t length, openset::comms::ReadyCB callback)
{
	// we copy const values so we can manage deletion
	auto dataCopy = recast<char*>(PoolMem::getPool().getPtr(length + 1));
	memcpy(dataCopy, data, length);
	dataCopy[length] = 0;

	return dispatchAsync(route, rpc, dataCopy, length, callback);
}

openset::comms::Message* openset::mapping::Mapper::dispatchAsync(int64_t route, rpc_e rpc, const cjson* doc, openset::comms::ReadyCB callback)
{
	int64_t textLength;
	auto textData = cjson::StringifyCstr(doc, textLength);

	return dispatchAsync(route, rpc, textData, textLength, callback);
}

openset::comms::Message* openset::mapping::Mapper::dispatchSync(int64_t route, rpc_e rpc, char* data, int64_t length)
{

	if (openset::globals::mapper->getRoute(route)->isDead())
		return nullptr;

	mutex nextLock;
	condition_variable nextReady;
	auto ready = false;

	openset::comms::Message* resultMessage = nullptr;

	openset::comms::ReadyCB done_cb = [&ready, &nextReady, &resultMessage](openset::comms::Message* message)
	{
		nextReady.notify_one();
		resultMessage = message;
		ready = true;
	};

	dispatchAsync(route, rpc, data, length, done_cb);

	while (!ready)
	{
		unique_lock<std::mutex> lock(nextLock);
		nextReady.wait_for(lock, 250ms, [&ready, route]()
		{
			return ready;
		});

		auto tRoute = globals::mapper->getRoute(route);

		if (!tRoute || tRoute->isDead())
		{
			Logger::get().info('!', "xfer error.");
			break;
		}
	}

	return resultMessage;
}

openset::comms::Message* openset::mapping::Mapper::dispatchSync(int64_t route, rpc_e rpc, const char* data, int64_t length)
{
	auto dataCopy = recast<char*>(PoolMem::getPool().getPtr(length + 1));
	memcpy(dataCopy, data, length);
	dataCopy[length] = 0;

	return dispatchSync(route, rpc, dataCopy, length);
}

openset::comms::Message* openset::mapping::Mapper::dispatchSync(int64_t route, rpc_e rpc, const cjson* doc)
{
	int64_t textLength;
	auto textData = cjson::StringifyCstr(doc, textLength);

	return dispatchSync(route, rpc, textData, textLength);
}


openset::comms::Message* openset::mapping::Mapper::getMessage(MessageID messageId)
{
	csLock lock(cs); // lock 

	auto message = messages.find(messageId);

	if (message != messages.end())
		return message->second;

	return nullptr;
}

void openset::mapping::Mapper::dereferenceMessage(MessageID messageId)
{
	auto message = messages.end();

	{
		csLock lock(cs); // lock 
		message = messages.find(messageId);
	}

	if (message != messages.end())
	{
		csLock lock(cs);
		messages.erase(messageId);
	}
}

void openset::mapping::Mapper::disposeMessage(MessageID messageId)
{
	auto message = messages.end();
	{
		csLock lock(cs); // lock 
		message = messages.find(messageId);
	}

	if (message != messages.end())
	{
		delete message->second; // message destructor removes message
		//csLock lock(cs);
		//messages.erase(messageId);
	}
}

class dispatchState
{
private:
	int requestCount = 0;
	atomic<int> responseCount{ 0 };
	bool active = true;	
public:
	bool isActive() const
	{
		return active;
	}
	void deactivate()
	{
		active = false;
	}
	bool isComplete() const
	{
		return responseCount >= requestCount;
	}
	void incrRequest()
	{
		++requestCount;
	}
	void incrResponse()
	{
		++responseCount;
	}
};

openset::mapping::Mapper::Responses openset::mapping::Mapper::dispatchCluster(
	rpc_e rpc, 
	const char* data, 
	int64_t length,
	bool internalDispatch)
{
	mutex continueLock;
	condition_variable continueReady;

	CriticalSection responseCS;

	openset::mapping::Mapper::Responses result;

	auto callbackState = new dispatchState;

	openset::comms::ReadyCB done_cb = [callbackState, &responseCS, &result, &continueReady](openset::comms::Message* message)
	{
		if (!callbackState->isActive())
		{
			// Trapped dead callback - if it aborted by error.
			// callbackstate will still be on the heap
			callbackState->incrResponse();
			if (callbackState->isComplete())
				delete callbackState;
		}
		int64_t length = 0;
		auto data = message->transferPayload(length);

		{
			csLock lock(responseCS);
			// store this response in the response object
			result.responses.emplace_back(openset::mapping::Mapper::DataBlock{ data,length });
		}

		callbackState->incrResponse();

		if (callbackState->isComplete())
			continueReady.notify_one();
	};

	std::vector<int64_t> cachedRoutes;

	// dispatchAsync to all our nodes
	for (auto r : routes)
	{
		// don't call this node
		if (!internalDispatch && r.first == globals::running->nodeId)
			continue;

		auto tRoute = globals::mapper->getRoute(r.first);

		if (!tRoute || tRoute->isDead())
			continue;
	
		dispatchAsync(r.first, rpc, data, length, done_cb);
		callbackState->incrRequest();

		cachedRoutes.push_back(r.first);
}

	// we are going to loop and wait until we get all our responses back
	while (!callbackState->isComplete())
	{
		unique_lock<std::mutex> lock(continueLock);
		continueReady.wait_for(lock, 500ms, [callbackState]()
		{
			return callbackState->isComplete();
		});

		for (auto r : cachedRoutes)
			if (!getRoute(r) || getRoute(r)->isDead())
			{
				result.routeError = true;
				cout << "**** TOTAL ERROR ****" << endl;
				break;
			}

		if (result.routeError)
			break;
	}

	// this callback is done
	callbackState->deactivate();
	if (callbackState->isComplete())
		delete callbackState;

	return result;
}

openset::mapping::Mapper::Responses openset::mapping::Mapper::dispatchCluster(rpc_e rpc, cjson* doc, bool internalDispatch)
{
	int64_t textLength;
	auto textData = cjson::StringifyCstr(doc, textLength);
	auto result = dispatchCluster(rpc, textData, textLength, internalDispatch);
	cjson::releaseStringifyPtr(textData);
	return result;
}

void openset::mapping::Mapper::releaseResponses(Responses& responseSet)
{
	for (auto r : responseSet.responses)
		if (r.first)
			PoolMem::getPool().freePtr(r.first);

	responseSet.responses.clear();
}

int64_t openset::mapping::Mapper::getSentinelId() const 
{
	auto lowestId = LLONG_MAX;

	// scope lock the mapper
	csLock lock(cs);

	for (auto& r : routes)
		if (r.first < lowestId)
			lowestId = r.first;

	return lowestId;
}

int openset::mapping::Mapper::countFailedRoutes() const 
{
	auto failedCount = 0;

	// scope lock the mapper
	csLock lock(cs);

	for (auto& r : routes)
		if (r.second->isDead())
			++failedCount;

	return failedCount;
}

int openset::mapping::Mapper::countActiveRoutes() const
{
	auto failedCount = 0;

	// scope lock the mapper
	csLock lock(cs);

	for (auto& r : routes)
		if (r.second->isLocalLoop || !r.second->isDead())
			++failedCount;

	return failedCount;
}

int openset::mapping::Mapper::countRoutes() const
{
	csLock lock(cs);
	return routes.size(); // return total routes including local route
}

std::vector<int64_t> openset::mapping::Mapper::getActiveRoutes() const
{
	std::vector<int64_t> activeRoutes;

	csLock lock(cs);
	for (auto& r : routes)
		if (r.second->isLocalLoop || r.second->isOpen())
			activeRoutes.push_back(r.first);

	sort(activeRoutes.begin(), activeRoutes.end(),
		[](const int64_t a, const int64_t b)
	{
		return a > b;
	});

	return activeRoutes;
}

std::vector<int64_t> openset::mapping::Mapper::getFailedRoutes() const
{
	std::vector<int64_t> deadRoutes;

	csLock lock(cs);
	for (auto& r : routes)
		if (!r.second->isLocalLoop && !r.second->isOpen())
			deadRoutes.push_back(r.first);

	return deadRoutes;
}

std::vector<std::pair<int64_t, int>> openset::mapping::Mapper::getPartitionCountsByRoute(std::unordered_set<NodeState_e> states) const
{
	auto activeRoutes = getActiveRoutes();

	std::vector<std::pair<int64_t, int>> result;

	for (auto r: activeRoutes)
	{
		// start at zero for the count on this route
		auto stats = std::make_pair(r, 0);

		// get a list of partitions assigned to this route/node
		auto partitions = globals::mapper->partitionMap.getPartitionsByNodeId(r);
		
		for (auto p : partitions)
		{
			// call getState on the partition/route, if the value is in `states` then increment
			if (states.count(globals::mapper->partitionMap.getState(p, r)))
				stats.second++;
		}

		result.emplace_back(stats);
	}

	sort(result.begin(), result.end(),
		[](const std::pair<int64_t, int> a, const std::pair<int64_t, int> b)
	{
		return a.second > b.second;
	});

	return result;
}

void openset::mapping::Mapper::changeMapping(
	cjson* config, 
	std::function<void(int)> addPartition_cb,
	std::function<void(int)> deletePartition_cb, 
	std::function<void(string, int64_t, string, int)> addRoute_cb,
	std::function<void(int64_t)> deleteRoute_cb)
{
	auto routesNode = config->xPath("/routes");

	if (routesNode)
	{
		auto routesList = routesNode->getNodes();

		// set used to check for routes that have vanished
		std::unordered_set<int64_t> providedRouteIds;

		for (auto r: routesList)
		{
			auto name = r->xPathString("name", "");
			auto id = r->xPathInt("id", 0);
			auto host = r->xPathString("host", "");
			auto port = r->xPathInt("port", 0);

			if (name.length() && id && host.length() && port)
			{
				providedRouteIds.insert(id);
				if (routes.count(id) == 0)
					addRoute_cb(name, id, host, port);
			}
		}

		std::vector<int64_t> cleanup;
		for (auto r: routes)
			if (!providedRouteIds.count(r.first))
				cleanup.push_back(r.first);

		for (auto r: cleanup)
			deleteRoute_cb(r);
	}

	// change some routes
	partitionMap.changeMapping(
		config->xPath("/cluster"), 
		addPartition_cb,
		deletePartition_cb);

	// TODO add /routes
}

void openset::mapping::Mapper::loadPartitions()
{
	partitionMap.loadPartitionMap();
}

void openset::mapping::Mapper::savePartitions()
{
	partitionMap.savePartitionMap();
}

void openset::mapping::Mapper::serializeRoutes(cjson* doc)
{
	doc->setType(cjsonType::ARRAY);

	for (auto r : routes)
	{
		auto item = doc->pushObject();
		// get the name from the names map
		item->set("name", names.find(r.first)->second);
		item->set("id", r.second->getRoute());
		item->set("host", r.second->getHost());
		item->set("port", r.second->getPort());
	}
}

void openset::mapping::Mapper::deserializeRoutes(cjson* doc)
{
	if (doc->empty())
	{
		Logger::get().info(' ', "no inter-node routes configured.");
		return;
	}


	auto nodes = doc->getNodes();
	auto count = 0;

	for (auto n : nodes) // array of simple table names, nothing fancy
	{
		auto nodeName = n->xPathString("/name", "");
		auto nodeId = n->xPathInt("/id", 0);
		auto ip = n->xPathString("/host", "");
		auto port = n->xPathInt("/port", 0);

		if (!port || !nodeId || !ip.length())
			continue; // TODO - this is likely an error

		++count;

		// create the route (or update it if it's not the same)
		addRoute(nodeName, nodeId, ip, port);
	}
}

void openset::mapping::Mapper::loadRoutes()
{

	if (globals::running->testMode)
		return;

	// see if it exists
	if (!openset::IO::File::FileExists(globals::running->path + "routes.json"))
	{
		saveRoutes();
		return;
	}

	cjson tableDoc(globals::running->path + "routes.json");

	deserializeRoutes(&tableDoc);

	Logger::get().info('+', "loaded routes.");	
}


void openset::mapping::Mapper::saveRoutes()
{
	if (globals::running->testMode)
		return;

	cjson doc;

	serializeRoutes(&doc);

	cjson::toFile(globals::running->path + "routes.json", &doc);
}

void openset::mapping::Mapper::run() const
{
	Logger::get().info('+', "inter-node communications initialized.");

	while (true)
	{
		ThreadSleep(1000);

		// TODO - add route maintenance and checks to this thread
	}
}

void openset::mapping::Mapper::startRouter()
{	
	// make a thread, call the `run` function, monitor nodes
	std::thread runner(&openset::mapping::Mapper::run, this);
	runner.detach();	
}
