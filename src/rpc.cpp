#include <stdexcept>
#include <cinttypes>

#include "rpc.h"
#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"
#include "oloop_query.h"
#include "oloop_count.h"

#include "config.h"
#include "sentinel.h"
#include "querycommon.h"
#include "queryparser.h"
#include "trigger.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "internodemessage.h"
#include "names.h"


using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

enum class internodeFunction_e : int32_t
{
	init_config_node,
	cluster_member,
	node_add,
	transfer,
	map_change,
	cluster_lock,
	cluster_release,
};

static const unordered_map<string, internodeFunction_e> internodeMap = {
	{ "init_config_node", internodeFunction_e::init_config_node },
	{ "cluster_member", internodeFunction_e::cluster_member },
	{ "node_add", internodeFunction_e::node_add},
	{ "transfer", internodeFunction_e::transfer },
	{ "map_change", internodeFunction_e::map_change},
	{ "cluster_lock", internodeFunction_e::cluster_lock },
	{ "cluster_release", internodeFunction_e::cluster_release },
};

void Internode::error(openset::errors::Error error, cjson* response)
{
	cjson::Parse(error.getErrorJSON(), response);
}

void Internode::onMessage(
	Database* database,
	AsyncPool* partitions,
	openset::comms::Message* message)
{
	auto msgText = message->toString();
	cjson request(msgText, msgText.length());
	cjson response;

	const auto command = request.xPathString("/action", "__error__");
	const auto iter = internodeMap.find(command);

	if (iter == internodeMap.end())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"internode function not found" },
			&response);

		message->reply(cjson::Stringify(&response, true));
		return;
	}

	switch (iter->second) // second is type adminFunction_e 
	{
		case internodeFunction_e::init_config_node:
			initConfigureNode(database, partitions, &request, &response);
		break;

		case internodeFunction_e::cluster_member: 
			isClusterMember(database, partitions, &request, &response);
		break;

		case internodeFunction_e::map_change:
			mapChange(database, partitions, &request, &response);
			break;

		case internodeFunction_e::transfer:
			transfer(database, partitions, &request, &response);
			break;

		case internodeFunction_e::node_add:
			nodeAdd(database, partitions, &request, &response);
			break;

		default: ;
	}

	message->reply(cjson::Stringify(&response, true));
}

void Internode::isClusterMember(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	response->set("part_of_cluster", globals::running->state != openset::config::nodeState_e::ready_wait);
}

void Internode::initConfigureNode(Database* database, AsyncPool* asyncEngine, cjson* request, cjson* response)
{
	globals::mapper->removeRoute(globals::running->nodeId);

	const auto nodeName = request->xPathString("/params/node_name", "");
	const auto nodeId = request->xPathInt("/params/node_id", 0);
	const auto partitionMax = request->xPathInt("/params/partition_max", 0);

	cout << endl;
	Logger::get().info("Joining cluster as: '" + nodeName + "'.");
	cout << endl;

	// assign a new node id
	{
		globals::running->updateConfigVersion(request->xPathInt("/params/config_version", 0));

		globals::running->nodeId = nodeId;
		globals::running->state = openset::config::nodeState_e::active;
		globals::running->configVersion = 1;
		globals::running->partitionMax = partitionMax;
	}

	// create the routes
	openset::globals::mapper->deserializeRoutes(request->xPath("/params/routes"));

	// set number of partitions
	asyncEngine->setPartitionMax(partitionMax);
	// set them running - this return right away
	asyncEngine->startAsync();

	// set the partition map
	openset::globals::mapper->getPartitionMap()->deserializePartitionMap(request->xPath("/params/cluster"));
	asyncEngine->mapPartitionsToAsyncWorkers();

	asyncEngine->suspendAsync();
	// create the tables
	auto nodes = request->xPath("/params/tables")->getNodes();
	for (auto n : nodes)
	{
		auto tableName = n->xPathString("/name", "");

		if (!tableName.length())
			continue;

		auto table = database->newTable(tableName);

		table->deserializeTable(n->xPath("/table"));
		table->deserializeTriggers(n->xPath("/triggers"));
	}

	asyncEngine->resumeAsync();

	Logger::get().info("configured for " + to_string(partitionMax) + " partitions.");

	response->set("configured", true);
}

void Internode::nodeAdd(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	const auto nodeName = request->xPathString("/params/node_name", "");
	const auto nodeId = request->xPathInt("/params/node_id", 0);
	auto host = request->xPathString("/params/host", "");
	const auto port = request->xPathInt("/params/port", 0);

	// update - config verison only set on commit
	// globals::running->updateConfigVersion(request->xPathInt("/params/config_version", 0));

	if (host.length() && port && nodeId)
	{
		openset::globals::mapper->addRoute(nodeName, nodeId, host, port);
		Logger::get().info("added route " + globals::mapper->getRouteName(nodeId) + " @" + host + ":" + to_string(port) + ".");
	}
	else
	{
		Logger::get().error("change_cluster:node_add - missing params");
		error(
			openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"change_cluster:node_add missing params" 
			},
			response);
		return;
	}

	response->set("response", "thank you.");
}

void Internode::transfer(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	const auto targetNode = request->xPathInt("/params/target_node", -1);
	const auto partitionId = request->xPathInt("/params/partition", -1);

	std::vector<openset::db::Table*> tables;

	{ // get a list of tables
		csLock lock(database->cs);
		for (auto t : database->tables)
			tables.push_back(t.second);
	}
	
	Logger::get().info("transfer started for partition " + to_string(partitionId) + ".");

	globals::async->suspendAsync();

	for (auto t : tables)
	{
		auto part = t->getPartitionObjects(partitionId);

		if (part)
		{
			char* blockPtr = nullptr;
			int64_t blockSize = 0;

			{
				HeapStack mem;

				// we need to stick a header on this
				// the header needs the partition and the table name
				// the data belongs to.

				// grab 4 bytes and assign the partitionId
				*(recast<int32_t*>(mem.newPtr(sizeof(int32_t)))) = partitionId;

				// grab 4 bytes and set it the length of the table name
				auto tableNameLength = recast<int32_t*>(mem.newPtr(sizeof(int32_t)));
				*tableNameLength = t->getName().length() + 1;

				// grab some bytes for the table name, and copy in the table name
				auto name = mem.newPtr(*tableNameLength);
				strcpy(name, t->getName().c_str());

				// serialize the attributes
				part->attributes.serialize(&mem);

				// serialize the people
				part->people.serialize(&mem);

				blockPtr = mem.flatten();
				blockSize = mem.getBytes();
			}

			auto responseMessage = openset::globals::mapper->dispatchSync(
				targetNode,
				openset::mapping::rpc_e::inter_node_partition_xfer,
				blockPtr,
				blockSize);

			if (!responseMessage)
				Logger::get().error("xfer error " + t->getName() + ".");
			else
				Logger::get().info("transfered " + t->getName() + " on " + openset::globals::mapper->getRouteName(partitionId) + ".");
		}
	}

	globals::async->resumeAsync();

	Logger::get().info("transfer complete on partition " + to_string(partitionId) + ".");

	response->set("response", "thank you.");
}


void Internode::mapChange(Database* database, AsyncPool* asyncEngine, cjson* request, cjson* response)
{
	// These callbacks allow us to clean objects up when the map is altered.
	// The map doesn't have knowledge of these objects (and shouldn't) and these
	// objects are not in that scope so this is a nice tidy way to do this

	const auto addPartition = [&](int partitionId)
	{
		// add this partition to the async pool, it will add it to a loop
		asyncEngine->initPartition(partitionId);

		globals::async->assertAsyncLock(); // dbg - assert we are in a lock

		for (auto t : database->tables)
			t.second->getPartitionObjects(partitionId);
	};

	const auto removePartition = [&](int partitionId)
	{
		// drop this partition from the async engine
		asyncEngine->freePartition(partitionId);

		globals::async->assertAsyncLock();

		// drop this partition from any table objects
		for (auto t : database->tables)
			t.second->releasePartitionObjects(partitionId);
	};

	const auto removeRoute = [&](int64_t nodeId)
	{
		Logger::get().info("removing route via mapping change");
		globals::mapper->removeRoute(nodeId);
	};

	const auto addRoute = [&](std::string name, int64_t nodeId, std::string host, int port)
	{
		Logger::get().info("adding route '" + name + "' via mapping change");
		globals::mapper->addRoute(name, nodeId, host, port);
	};


	globals::async->suspendAsync();
	globals::async->assertAsyncLock();

	// map changes require the full undivided attention of the cluster!
	// nothing executing, means no goofy locks and no bad pointers
	openset::globals::mapper->changeMapping(
		request->xPath("/params"),
		addPartition,
		removePartition,
		addRoute,
		removeRoute);

	globals::async->resumeAsync();

	response->set("response", "thank you.");
}


enum class adminFunction_e : int32_t
{
	none,
	status,
	shutdown,
	init_cluster,	
	invite_node, // invite another node to join the cluster - sent by sentinel
	create_table,
	describe_table,
	add_column,
	drop_column,
	set_trigger,
	describe_triggers,
	drop_trigger
};

static const unordered_map<string, adminFunction_e> adminMap = {
	{"status", adminFunction_e::status},
	{"shutdown", adminFunction_e::shutdown},
	{"init_cluster", adminFunction_e::init_cluster },
	{ "invite_node", adminFunction_e::invite_node },
	{"create_table", adminFunction_e::create_table },
	{"describe_table", adminFunction_e::describe_table },
	{"add_column", adminFunction_e::add_column },
	{"drop_column", adminFunction_e::drop_column },
	{"set_trigger", adminFunction_e::set_trigger },
	{"describe_triggers", adminFunction_e::describe_triggers},
	{"drop_trigger", adminFunction_e::drop_trigger}
};

void Admin::error(openset::errors::Error error, cjson* response)
{
	cjson::Parse(error.getErrorJSON(), response);
}

enum class forwardStatus_e : int
{
	dispatched,
	isForwarded,
	error
};

forwardStatus_e forwardRequest(
	openset::mapping::rpc_e rpc,
	Database* database,
	AsyncPool* partitions,
	cjson* request,
	cjson* response)
{
	if (!openset::globals::mapper->routes.size())
		return forwardStatus_e::error;
	
	if (request->xPathBool("/forwarded", false))
		return forwardStatus_e::isForwarded;

	request->set("forwarded", true);

	auto result = openset::globals::mapper->dispatchCluster(
		rpc,
		request,
		true);

	const auto inError = result.routeError;

	if (!inError)
		cjson::Parse(
			string{
				result.responses[0].first,
				static_cast<size_t>(result.responses[0].second) 
			},
			response
		);

	openset::globals::mapper->releaseResponses(result);

	return (inError) ? forwardStatus_e::error : forwardStatus_e::dispatched;
}

void Admin::onMessage(
	Database* database,
	AsyncPool* partitions,
	//uvConnection* connection,
	openset::comms::Message* message)
{
	auto msgText = message->toString();
	cjson request(msgText, msgText.length());
	cjson response;

	const auto command = request.xPathString("/action", "null");
	const auto iter = adminMap.find(command);

	if (iter == adminMap.end())
	{ 
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing or invalid action (" + command + ")" },
			&response);

		message->reply(&response);

		return;
	}

	auto handled = false;

	switch (iter->second) // second is type adminFunction_e 
	{
	case adminFunction_e::none:
		handled = true;
		break;

	case adminFunction_e::status:
		message->reply("{\"message\":\"hello\"}");
		break;

	case adminFunction_e::shutdown:
		handled = true;
		break;

	case adminFunction_e::init_cluster:
		handled = true;
		initCluster(database, partitions, &request, &response);
		break;

	case adminFunction_e::invite_node:
		handled = true;
		if (!openset::globals::sentinel->isSentinel())
		{
			auto sentinelRoute = openset::globals::sentinel->getSentinel();

			Logger::get().info("forwarding invite to sentinal '" + openset::globals::mapper->getRouteName(sentinelRoute) + "'");

			const auto responseMessage = openset::globals::mapper->dispatchSync(
				sentinelRoute,
				openset::mapping::rpc_e::admin,
				&request
			);

			if (responseMessage)
			{
				cjson::Parse(responseMessage->toString(), &response);
			}
			else
			{
				Logger::get().error("error forwarding node invitiation");
				response.set("error", true);
			}
		}
		else
		{
			inviteNode(database, partitions, &request, &response);
		}
		break;
	};

	if (handled)
	{
		message->reply(&response);
		return;
	}

	switch (forwardRequest(
		openset::mapping::rpc_e::admin,
		database,
		partitions,
		&request,
		&response))
	{
	case forwardStatus_e::error:
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::route_error,
			"potential node failure - please re-issue the request" },
			&response);
		break;

	case forwardStatus_e::isForwarded:
		switch (iter->second) // second is type adminFunction_e 
		{
		case adminFunction_e::create_table:
			createTable(database, partitions, &request, &response);
			break;

		case adminFunction_e::describe_table:
			describeTable(database, partitions, &request, &response);
			break;

		case adminFunction_e::add_column:
			addColumn(database, partitions, &request, &response);
			break;

		case adminFunction_e::drop_column:
			dropColumn(database, partitions, &request, &response);
			break;

		case adminFunction_e::set_trigger:
			setTrigger(database, partitions, &request, &response);
			break;

		case adminFunction_e::describe_triggers:
			describeTriggers(database, partitions, &request, &response);
			break;

		case adminFunction_e::drop_trigger:
			dropTrigger(database, partitions, &request, &response);
			break;

		default:
			// error
			break;
		}
		case forwardStatus_e::dispatched: break;
		default: ;
	};

	message->reply(&response);
	
}

void Admin::initCluster(
	Database* database,
	AsyncPool* partitions, 
	cjson * request, 
	cjson* response)
{
	const auto partitionMax = request->xPathInt("params/partitions", 0);

	if (partitionMax < 1 || partitionMax > 1000)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"partitions must be >= 1 and <= 1000" },
			response);
		return;
	}

	if (partitions->isRunning())
	{
		error(
			openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"This instance is already part of a cluster" },
			response);
		return;
	}

	// remove any existing mapping
	globals::mapper->removeRoute(globals::running->nodeId);

	// update config
	{
		csLock lock(globals::running->cs);
		globals::running->setNodeName(openset::config::createName());
		globals::running->state = openset::config::nodeState_e::active;
		globals::running->partitionMax = partitionMax;		
		Logger::get().info("Initialized as: '" + globals::running->nodeName +"'.");
	}

	openset::globals::mapper->partitionMap.clear();
	for (auto i = 0; i < partitionMax; ++i)
		openset::globals::mapper->partitionMap.setOwner(i, globals::running->nodeId);

	// set number of partitions
	partitions->setPartitionMax(partitionMax);
	// set them running - this return right away
	partitions->startAsync(); 

	partitions->mapPartitionsToAsyncWorkers();

	auto logLine = "configured for " + to_string(partitionMax) + " partitions.";
	Logger::get().info(logLine);
	response->set("message", logLine);	

	// routes are broadcast to nodes, we use the external host and port
	// so that nodes can find each other in containered situations where
	// the container doesn't know it's own IP and ports are mapped
	openset::globals::mapper->addRoute(
		globals::running->nodeName, 
		globals::running->nodeId, 
		globals::running->hostExternal, 
		globals::running->portExternal);
}

void Admin::inviteNode(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{

	if (globals::running->state != openset::config::nodeState_e::active)
	{
		Logger::get().error("node must be initialized to invite other nodes.");
		response->set("class", "config");
		response->set("error", "node_not_initialized");
		return;
	}

	auto host = request->xPathString("/params/host", "");
	auto port = request->xPathInt("/params/port", 0);
	auto newNodeName = openset::config::createName();
	auto newNodeId = MakeHash(newNodeName);

	if (!host.length() || !port)
	{
		Logger::get().error("invite node: missing params.");
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing host or port /params/host, /params/port" },
			response);

		return;
	}

	// Step 0 - Try to open a connection to the remote node
	openset::mapping::OutboundClient client(0, host, port, true);

	if (!client.openConnection())
	{
		Logger::get().error("invite node: could not connect " + host + ":" + to_string(port) + ".");
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::internode_error,
			"could not connect to " + host + ":" + to_string(port)},
			response);

		return;
	}

	// Step 1 - Verify that the remote node exists and is able to join
	{
		Logger::get().info("inviting node " + host + ":" + to_string(port) + ".");

		cjson initRequest;
		initRequest.set("action", "cluster_member"); 
		auto params = initRequest.setObject("params");
		
		params->set("node_name", newNodeName);
		params->set("node_id", newNodeId);
		params->set("config_version", globals::running->configVersion);

		auto rpcJSON = cjson::Stringify(&initRequest);

		openset::comms::RouteHeader_s route;
		route.route = 0; // special case where we set this to zero (new node has no id)
		route.rpc = cast<int32_t>(openset::mapping::rpc_e::inter_node);
		route.replyTo = globals::running->nodeId;
		route.slot = 0; // reply will come back over this channel
		route.length = rpcJSON.length();

		client.directRequest(route, rpcJSON.c_str());

		char* initResponseData;
		auto clientResponseHeader = client.waitDirectResponse(initResponseData);

		if (clientResponseHeader.isError() || !initResponseData)
		{
			Logger::get().info("invited node " + host + ":" + to_string(port) + " could not be reached.");
			response->set("class", "config");
			response->set("error", "verify_node_could_not_be_reached");
			return;
		}

		// copy to string
		auto tStr = std::string(initResponseData);
		delete[] initResponseData; // free buffer

		cjson clientResponseJson(tStr, tStr.length()); // parse from string

		if (clientResponseJson.xPathBool("/part_of_cluster", true))  
		{
			Logger::get().error("invited node " + host + ":" + to_string(port) + " not available to join cluster.");
			response->set("class", "config");
			response->set("error", "verify_node_not_available");
			return;
		}
	}

	// Step 2 - The remote node is open to being configured, lets send it the entire config	
	{
		// TODO glue together the whole config
		cjson configBlock;
		configBlock.set("action", "init_config_node");
		auto params = configBlock.setObject("params");

		params->set("node_name", newNodeName);
		params->set("node_id", newNodeId);
		params->set("partition_max", cast<int64_t>(openset::globals::async->partitionMax));

		// make am array node called tables, push the tables, triggers, columns into the array
		auto tables = params->setArray("tables");

		for (auto n : database->tables)
		{
			auto tableItem = tables->pushObject();

			tableItem->set("name", n.second->getName());

			// here we are just making nodes and passing them to
			// the serialize functions so the data becomes a series of
			// objects within the tables list/array created above
			n.second->serializeTable(tableItem->setObject("table"));
			n.second->serializeTriggers(tableItem->setObject("triggers"));							
		}

		// make a node called routes, serialize the routes (nodes) under it
		openset::globals::mapper->serializeRoutes(params->setObject("routes"));

		// make a node called cluster, serialize the partitionMap under it
		openset::globals::mapper->getPartitionMap()->serializePartitionMap(params->setObject("cluster"));


		auto rpcJSON = cjson::Stringify(&configBlock);

		Logger::get().info("configuring node " + newNodeName + "@" + host + ":" + to_string(port) + ".");

		openset::comms::RouteHeader_s route;
		route.route = 0; // special case where we set this to zero (new node has no id)
		route.rpc = cast<int32_t>(openset::mapping::rpc_e::inter_node);
		route.replyTo = globals::running->nodeId;
		route.slot = 0; // reply will come back over this channel
		route.length = rpcJSON.length();

		client.directRequest(route, rpcJSON.c_str());

		char* initResponseData;
		auto clientResponseHeader = client.waitDirectResponse(initResponseData);

		if (clientResponseHeader.isError() || !initResponseData)
		{
			Logger::get().error("invited node " + host + ":" + to_string(port) + " could not be reached.");
			response->set("class", "config");
			response->set("error", "config_node_could_not_be_reached");
			return;
		}

		// copy to string
		auto tStr = std::string(initResponseData);
		delete[] initResponseData; // free buffer

		cjson clientResponseJson(tStr, tStr.length()); // parse from string

		if (!clientResponseJson.xPathBool("/configured", false))
		{
			Logger::get().info("invited node " + host + ":" + to_string(port) + " could not be configured.");
			response->set("class", "config");
			response->set("error", "config_node_not_configured");
			return;
		}

	}

	// add the new node to the local dispatchAsync list, then
	// fork out the node_add command to any other nodes
	{
		Logger::get().info("broadcasting membership for node " + newNodeName + " @" + host + ":" + to_string(port));

		// add the new route to the local route map, that way it will
		// receive the broadcast in the next step
		openset::globals::mapper->addRoute(newNodeName, newNodeId, host, port);

		// tell all the nodes (including our new node) about
		// the new node.
		cjson newNode;
		newNode.set("action", "node_add");
		auto params = newNode.setObject("params");
		//params->set("config_version", globals::running->updateConfigVersion());
		params->set("node_name", newNodeName);
		params->set("node_id", newNodeId);
		params->set("host", host);
		params->set("port", port);

		auto newNodeJson = cjson::Stringify(&newNode);

		auto addResponses = openset::globals::mapper->dispatchCluster(
			openset::mapping::rpc_e::inter_node, 
			newNodeJson.c_str(), 
			newNodeJson.length());

		openset::globals::mapper->releaseResponses(addResponses);
		// helper?
	}

	// respond to client
	response->set("node_joined", true);
	
}


void Admin::createTable(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{	
	auto tableName = request->xPathString("/params/table", "");

	if (!tableName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);
		return;
	}

	if (database->getTable(tableName))
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table already exists" },
			response);
		return;
	}

	// TODO - look for spaces, check length, look for symbols that aren't - or _ etc.

	const auto sourceColumns = request->xPath("/params/columns");
	if (!sourceColumns)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"column definition required, missing /params/columns" },
			response);

		return;
	}

	const auto sourceZOrder = request->xPath("/params/z_order");

	auto sourceColumnsList = sourceColumns->getNodes();

	globals::async->suspendAsync();
	auto table = database->newTable(tableName);
	auto columns = table->getColumns();

	// set the default required columns
	columns->setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false, 0);
	columns->setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false, 0);
	columns->setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false, 0);
	columns->setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false, 0);
	columns->setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false, 0);

	int64_t columnEnum = 1000;

	for (auto n: sourceColumnsList)
	{
		auto name = n->xPathString("/name", "");
		auto type = n->xPathString("/type", "");

		if (!name.size() || !type.size())
		{
			error(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"primary column name or type" },
				response);

			return;
		}

		auto colType = openset::db::columnTypes_e::intColumn;

		if (type == "text")
			colType = columnTypes_e::textColumn;
		else if (type == "int")
			colType = columnTypes_e::intColumn;
		else if (type == "double")
			colType = columnTypes_e::doubleColumn;
		else if (type == "bool")
			colType = columnTypes_e::boolColumn;
		else
		{
			error(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"invalide column type" },
				response);

			return;
		}

		columns->setColumn(
			columnEnum, 
			name, 
			colType, 
			false, 
			false);	

		++columnEnum;
	}

	globals::async->resumeAsync();


	if (sourceZOrder)
	{
		auto zOrderStrings = table->getZOrderStrings();
		auto zOrderInts = table->getZOrderInts();

		auto sourceZStrings = sourceZOrder->getNodes();

		auto idx = 0;
		for (auto n : sourceZStrings)
		{
			zOrderStrings->emplace(n->getString(), idx);
			zOrderInts->emplace(MakeHash(n->getString()), idx);
			++idx;
		}
	}

// make the output directory
	//OpenSet::IO::Directory::mkdir(globals::running->path + "tables/" + tableName);

	// write our new file
	//cjson::toFile(globals::running->path + "tables/" + tableName + "/table.json", &tableJson, true);

	auto logLine = "table '" + tableName + "' created.";
	Logger::get().info(logLine);
	response->set("message", logLine);

}

void Admin::describeTable(
	Database* database,
	AsyncPool* partitions,
	cjson* request,
	cjson* response)
{

	auto tableName = request->xPathString("/params/table", "");

	if (!tableName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);

		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);

		return;
	}

	auto columnNodes = response->setArray("columns");
	auto columns = table->getColumns();

	for (auto &c : columns->columns)
		if (c.deleted == 0 && c.name.size() && c.type != columnTypes_e::freeColumn)
		{
			auto columnRecord = columnNodes->pushObject();

			std::string type;

			switch (c.type)
			{
			case columnTypes_e::intColumn:
				type = "int";
				break;
			case columnTypes_e::doubleColumn:
				type = "double";
				break;
			case columnTypes_e::boolColumn:
				type = "bool";
				break;
			case columnTypes_e::textColumn:
				type = "text";
				break;
			default:
				continue;
			}

			columnRecord->set("name", c.name);
			columnRecord->set("index", cast<int64_t>(c.idx));
			columnRecord->set("type", type);
			if (c.deleted)
				columnRecord->set("deleted", c.deleted);
		}

	auto logLine = "describe table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
}

void Admin::addColumn(
	Database* database,
	AsyncPool* partitions,
	cjson* request,
	cjson* response)
{
	auto tableName = request->xPathString("/params/table", "");
	const auto columnJson = request->xPath("/params/column");

	if (!tableName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);

		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);
		return;
	}

	if (!columnJson)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /param/column" },
			response);

		return;
	}

	auto columnName = columnJson->xPathString("/name", "");

	if (!columnName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing column name" },
			response);
		return;
	}

	auto columns = table->getColumns();

	int64_t lowest = 999;
	for (auto &c: columns->nameMap)
	{
		if (c.second->idx > lowest)
			lowest = c.second->idx;
	}

	++lowest;

	const auto name = columnJson->xPathString("/name", "");
	const auto type = columnJson->xPathString("/type", "");

	columnTypes_e colType;

	if (type == "text")
		colType = columnTypes_e::textColumn;
	else if (type == "int")
		colType = columnTypes_e::intColumn;
	else if (type == "double")
		colType = columnTypes_e::doubleColumn;
	else if (type == "bool")
		colType = columnTypes_e::boolColumn;
	else
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing or invalid column type" },
			response);

		return; // TODO hmmm...
	}

	columns->setColumn(lowest, name, colType, false, false);

	const auto logLine = "added column '" + columnName + "' from table '" + tableName + "' created.";
	Logger::get().info(logLine);
	response->set("message", logLine);
}

void Admin::dropColumn(
	Database* database,
	AsyncPool* partitions,
	cjson* request,
	cjson* response)
{
	auto tableName = request->xPathString("/params/table", "");
	auto columnName = request->xPathString("/params/column", "");

	if (!tableName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);

		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);
		return;
	}

	if (!columnName.size())
	{
		// TODO error
		return;
	}

	const auto column = table->getColumns()->getColumn(columnName);

	if (!column)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"column not found" },
			response);
		return;
	}

	// delete the actual column
	table->getColumns()->deleteColumn(column);

	// delete it in the config
	// change - save is performed on commit
	//table->saveConfig();

	const auto logLine = "dropped column '" + columnName + "' from table '" + tableName + "' created.";
	Logger::get().info(logLine);
	response->set("message", logLine);
}

void Admin::setTrigger(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	
	auto tableName = request->xPathString("/params/table", "");
	auto triggerName = request->xPathString("/params/trigger", "");
	auto script = request->xPathString("/params/script", "");

	if (!tableName.size() || !triggerName.size() || !script.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);
		return;
	}

	toLower(triggerName);
	// TODO check triggerName

	{ // scope for lock, saveConfig also locks, we don't nest locks

		csLock lock(globals::running->cs);

		// lets do some checking, are we making a new trigger
		// or updating an old one
		auto triggers = table->getTriggerConf();

		// does this trigger exist? If so this is an update!
		if (triggers->count(triggerName))
		{
			auto t = triggers->at(triggerName);
			t->script = script;

			// recompile script
			auto err = openset::trigger::Trigger::compileTrigger(
				table,
				t->name,
				t->script,
				t->macros);

			t->configVersion++; // this will force a reload
		}
		else // it's new trigger
		{
			auto t = new openset::trigger::triggerSettings_s;

			t->name = triggerName;
			t->id = MakeHash(t->name);
			t->script = script;
			t->entryFunction = "on_insert"; // these may be configurable at some point
			t->entryFunctionHash = MakeHash(t->entryFunction);
			t->configVersion = 0;

			auto err = openset::trigger::Trigger::compileTrigger(
				table,
				t->name,
				t->script,
				t->macros);

			if (err.inError())
			{
				error(err,response);
				return;
			}

			triggers->insert(std::make_pair(triggerName, t));
			table->forceReload(); // this updates the load version

			// note: async workers that are executing triggers will check the load version
			// and determine if they need to reload  triggers.
		}
	}

	// save out the config
	// change - save is performed on commit
	// table->saveConfig();

	auto logLine = "set trigger '" + triggerName + "' on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);

}

void Admin::describeTriggers(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	csLock lock(globals::running->cs);

	auto tableName = request->xPathString("/params/table", "");

	if (!tableName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);

		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);
		return;
	}

	auto triggers = table->getTriggerConf();

	auto triggerNode = response->setArray("triggers");

	for (auto &t: *triggers)
	{
		auto trigNode = triggerNode->pushObject();
		trigNode->set("name", t.second->name);
		trigNode->set("entry", t.second->entryFunction);
	}

	auto logLine = "describe triggers on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
}

void Admin::dropTrigger(Database* database, AsyncPool* partitions, cjson* request, cjson* response)
{
	csLock lock(globals::running->cs);

	auto tableName = request->xPathString("/params/table", "");
	auto triggerName = request->xPathString("/params/trigger", "");

	if (!tableName.size() || !triggerName.size())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			response);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			response);
		return;
	}

	auto triggers = table->getTriggerConf();
	auto trigger = triggers->find(triggerName);

	if (trigger == triggers->end())
	{
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"trigger not found" },
			response);
		return;
	}

	triggers->erase(triggerName);
	table->forceReload();

	auto logLine = "dropped trigger '" + triggerName + "' on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
}

void Insert::onInsert(
	Database* database,
	AsyncPool* partitions,
	//uvConnection* connection,
	openset::comms::Message* message)
{
	auto msgText = message->toString();
	cjson request(msgText, msgText.length());

	const auto onError = [message](const string &error)
	{
		message->reply("{\"error\":\"" + error + "\"}");
	};

	if (!partitions->getPartitionMax())
	{
		message->reply("{\"error\":{\"class\":\run_time\",\"message\":\"initialize_cluster\"}}");
		return;
	}

	Table* table = nullptr;

	const auto eventsNode = request.xPath("/events");

	const auto isFork = request.xPathBool("/is_fork", false);

	const auto tableName = request.xPathString("/profile",
		request.xPathString("/table", ""));

	if (!table)
		table = database->getTable(tableName);

	if (!table)
	{
		onError("missing table"s);
		return;
	}

	auto rows = eventsNode->getNodes();

	Logger::get().info("Inserting " + to_string(rows.size()) + " events.");

	// vectors go gather locally inserted, or remotely distributed events from this set
	std::unordered_map<int, std::vector<char*>> localGather;
	std::unordered_map<int64_t, std::vector<char*>> remoteGather;

	const auto mapper = openset::globals::mapper->getPartitionMap();

	for (auto row : rows)
	{
		const auto uuid = row->xPathString("/person", "");
		const auto uuHash = MakeHash(uuid) % 17783LL;
		const auto destination = cast<int32_t>(std::abs(uuHash) % partitions->getPartitionMax());

		const auto mapInfo = globals::mapper->partitionMap.getState(destination, globals::running->nodeId);
		
		if (mapInfo == openset::mapping::NodeState_e::active_owner ||
			mapInfo == openset::mapping::NodeState_e::active_clone)
		{
			if (!localGather.count(destination))
				localGather.emplace(destination, vector<char*>{});
				
			int64_t len;
			localGather[destination].push_back(cjson::StringifyCstr(row, len));
		}

		if (!isFork)
		{
			const auto nodes = mapper->getNodesByPartitionId(destination);

			for (auto targetNode : nodes)
			{
				if (targetNode == openset::globals::running->nodeId)
					continue;

				if (!remoteGather.count(targetNode))
					remoteGather.emplace(targetNode, vector<char*>{});

				int64_t len;
				remoteGather[targetNode].push_back(cjson::StringifyCstr(row, len));
			}
		}

	}

	for (auto &g: localGather)
	{			
		if (!g.second.size())
			continue;

		auto parts = table->getPartitionObjects(g.first);

		if (parts) 					
		{
			csLock lock(parts->insertCS); // lock once, then bulk queue

			parts->insertBacklog += g.second.size();
			parts->insertQueue.insert(
				parts->insertQueue.end(), 
				std::make_move_iterator(g.second.begin()), 
				std::make_move_iterator(g.second.end()));
		}
	}

	auto remoteCount = 0;
	auto thanksCount = 0;

	const auto thankyouCB = [](openset::comms::Message* message)
	{		
		// delete message;
	};

	if (!isFork)
		for (auto &data: remoteGather)
		{
			++remoteCount;

			const auto targetNode = data.first;
			const auto& events = data.second;

			// make an JSON array object
			cjson json;			

			json.set("table", tableName);
			json.set("is_fork", true);
			auto eventNode = json.setArray("events");

			for (auto e: events)
			{
				cjson::Parse(e, eventNode->pushObject(), true);
				PoolMem::getPool().freePtr(e);
			}

			auto jsonText = cjson::Stringify(&json);

			openset::globals::mapper->dispatchAsync(
				targetNode,
				openset::mapping::rpc_e::insert_async,
				jsonText.c_str(),
				jsonText.length(),
				thankyouCB);
		}

	// FLOW CONTROL - check for backlogging, delay the 
	// "thank you." response until backlog is acceptable
	for (auto &g: localGather)
	{
		const auto parts = table->getPartitionObjects(g.first);

		if (!parts)
			continue;

		auto sleepCount = 0;
		const auto sleepStart = Now();
		while (parts->insertBacklog > 5000)
		{
			ThreadSleep(5);
			++sleepCount;
		}

		if (sleepCount)
			Logger::get().info("insert drain timer for " + to_string(Now() - sleepStart) + "ms on partition " + 
				to_string(parts->partition) + ".");
	}

	message->reply("{\"response\":\"thank you.\"}");
}

void Feed::onSub(
	Database* database, 
	AsyncPool* partitions, 
	openset::comms::Message* message)
{

	if (!partitions->partitionMax || !partitions->running)
	{
		// TODO - error cluster not ready
		message->reply("[]");
		return;
	}

	if (message->clientConnection)
		message->clientConnection->holdDropped = true; // we don't recycle these, we want check for the error

	// this type of query will wait until there is a message
	auto messageLambda = [message, partitions, database]() noexcept
	{
		auto msgText = message->toString();
		cjson request(msgText, msgText.length());

		auto tableName = request.xPathString("/table", "");
		auto triggerName = request.xPathString("/trigger", "");
		auto subName = request.xPathString("/subscription", "");
		auto holdTime = request.xPathInt("/hold", 10'800'000); // 3 hours
		auto max = request.xPathInt("/max", 500);

		auto table = database->getTable(tableName);

		if (!table)
		{
			// TODO - table doesn't exist
			if (message->clientConnection)
				message->clientConnection->holdDropped = false; // we don't recycle these, we want check for the error

			message->reply("[]");
			return;
		}
		
		auto messages = table->getMessages();

		messages->registerSubscriber(triggerName, subName, holdTime);
	    		
		while (!messages->size(triggerName, subName))
		{
			ThreadSleep(100);

			if (message->clientConnection && 
				message->clientConnection->dropped)
			{
				Logger::get().error("subscriber '" + subName + "' on table '" + tableName + "' connection lost.");
				message->clientConnection->holdDropped = false;
				return;
			}
			// TODO - check for disconnect!
		}

		auto list = messages->pop(triggerName, subName, max);

		cjson response;

		auto messageArray = response.setArray("messages");

		for (auto m : list)
		{
			auto msg = messageArray->pushObject();
			msg->set("stamp", m.stamp);
			msg->set("uid", m.uuid);
			msg->set("message", m.message);
		}

		response.set("remaining", messages->size(triggerName, subName));
		
		message->reply(cjson::Stringify(&response, true));
	};

	// start the subscriber thread
	std::thread messageThread(messageLambda);
	messageThread.detach();

}

enum class queryFunction_e : int32_t
{
	none,
	status,
	query,
	count,
};

static const unordered_map<string, queryFunction_e> queryMap = {
	{ "status", queryFunction_e::status },
	{ "query", queryFunction_e::query },
	{ "count", queryFunction_e::count },
};


void Query::onMessage(
	Database* database,
	AsyncPool* partitions,
	openset::comms::Message* message)
{
	auto msgText = message->toString();
	cjson request(msgText, msgText.length());

	const auto command = request.xPathString("/action", "__error__");
	const auto isFork = request.xPathBool("/is_fork", false);
	const auto iter = queryMap.find(command);

	if (iter == queryMap.end())
	{
		cjson response;
		error(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_query_error,
			"missing or invalid action" },
			&response);

		message->reply(cjson::Stringify(&response, true));

		return;
	}

	switch (iter->second) // second is type adminFunction_e 
	{
	case queryFunction_e::none:
		break;

	case queryFunction_e::status:
		break;	

	case queryFunction_e::query:
		onQuery(database, partitions, isFork, &request, message);
		break;

	case queryFunction_e::count:
		onCount(database, partitions, isFork, &request, message);
		break;

	default:
		// error
		break;
	}

}

/*  
 * The magic FORK function. 
 *
 * This will add a `is_fork: true` member to the request
 * and forward the query to other nodes cluster.
 * 
 * fork nodes will return binary result sets.
 * the non-fork (or originating node) will call this function
 * and wait for results to return, at which point it will merge them.
 * 
 * Note: a single node could have any number of partitions, these partitions
 * are merged into a single result by `is_fork` nodes before return the
 * result set. This greatly reduces the number of data sets that need to be held
 * in memory and marged by the originator.
 */
void forkQuery(
	Database* database,
	Table* table,
	AsyncPool* partitions,
	cjson* request,
	openset::query::macro_s queryMacros,
	openset::comms::Message* message) // errorHandled will be true if an error was happend during the fork
{

	// add the `is_fork` value
	request->set("is_fork", true);

	auto requestText = cjson::Stringify(request);

	// call all nodes and gather results - JSON is what's coming back
	// NOTE - it would be fully possible to flatten results to binary
	auto result = openset::globals::mapper->dispatchCluster(
		openset::mapping::rpc_e::query_pyql, 
		requestText.c_str(), 
		requestText.length(),
		true);

	std::vector<openset::result::ResultSet*> resultSets;

	for (auto &r : result.responses)
	{
		if (ResultMuxDemux::isInternode(r.first, r.second))
			resultSets.push_back(ResultMuxDemux::internodeToResultSet(r.first, r.second));
		else
		{
			// there is an error message from one of the participing nodes
			// TODO - handle error
			Logger::get().error("some kinda strange");
		}
	}
	
	cjson resultJSON;

	// 1. merge the text
	auto mergedText = ResultMuxDemux::mergeText(
		queryMacros,
		table,
		resultSets);

	// 2. merge the rows
	auto rows = ResultMuxDemux::mergeResultSets(
		queryMacros, 
		table, 
		resultSets);

	// 3. make some JSON
	ResultMuxDemux::resultSetToJSON(
		queryMacros, 
		table, 
		&resultJSON, 
		rows,
		mergedText);


	// local function to fill Meta data in result JSON
	const auto fillMeta = [](const openset::query::VarList& mapping, cjson* jsonArray) {

		for (auto c : mapping)
		{
			auto tNode = jsonArray->pushObject();

			if (c.modifier == openset::query::modifiers_e::var)
			{
				tNode->set("mode", "var");
				tNode->set("name", c.alias);

				switch (c.value.typeof())
				{
				case cvar::valueType::INT32:
				case cvar::valueType::INT64:
					tNode->set("type", "int");
					break;
				case cvar::valueType::FLT:
				case cvar::valueType::DBL:
					tNode->set("type", "double");
					break;
				case cvar::valueType::STR:
					tNode->set("type", "text");
					break;
				case cvar::valueType::BOOL:
					tNode->set("type", "bool");
					break;
				default:
					tNode->set("type", "na");
					break;
				}
			}
			else if (openset::query::isTimeModifiers.count(c.modifier))
			{
				auto mode = openset::query::modifierDebugStrings.at(c.modifier);
				toLower(mode);
				tNode->set("mode", mode);
				tNode->set("name", c.alias);
				tNode->set("type", "int");
			}
			else
			{
				auto mode = openset::query::modifierDebugStrings.at(c.modifier);
				toLower(mode);
				tNode->set("mode", mode);
				tNode->set("name", c.alias);
				tNode->set("column", c.actual);

				switch (c.schemaType)
				{
				case columnTypes_e::freeColumn:
					tNode->set("type", "na");
					break;
				case columnTypes_e::intColumn:
					tNode->set("type", "int");
					break;
				case columnTypes_e::doubleColumn:
					tNode->set("type", "double");
					break;
				case columnTypes_e::boolColumn:
					tNode->set("type", "bool");
					break;
				case columnTypes_e::textColumn:
					tNode->set("type", "text");
					break;
				default:;
				}
			}
		}


	};

	// add status nodes to JSON document
	auto metaJson = resultJSON.setObject("info");

	auto dataJson = metaJson->setObject("data");
	fillMeta(queryMacros.vars.columnVars, dataJson->setArray("columns"));
	//fillMeta(queryMacros.vars.groupVars, dataJson->setArray("groups"));


	//metaJson->set("query_time", queryTime);
	//metaJson->set("pop_evaluated", population);
	//metaJson->set("pop_total", totalPopulation);
	//metaJson->set("compile_time", compileTime);
	//metaJson->set("serialize_time", serialTime);
	//metaJson->set("total_time", elapsed);

	// send back the resulting JSON
	message->reply(&resultJSON);

	Logger::get().info("Query on " + table->getName());

	// free up the responses
	openset::globals::mapper->releaseResponses(result);

	// clean up all those resultSet*
	for (auto r : resultSets)
		delete r;

	// TODO - handle some errors?
}

void Query::error(openset::errors::Error error, cjson* response)
{
	cjson::Parse(error.getErrorJSON(), response);
}

void Query::onQuery(
	Database* database,
	AsyncPool* partitions,
	bool isFork,
	cjson* request,
	openset::comms::Message* message)
{

	std::string log = "Inbound query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
	Logger::get().info(log);

	const auto tableName = request->xPathString("/params/table", "");
	const auto queryCode = request->xPathString("/params/code", "");
	const auto debug = request->xPathBool("/params/debug", false);
	const auto params = request->xPath("/params/params");

	const auto startTime = Now();

	if (!tableName.length())
	{
		message->reply("{\"error\":\"missing table name\"}");
		return;
	}

	if (!queryCode.length())
	{
		message->reply("{\"error\":\"missing query code\"}");
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		message->reply("{\"error\":\"table '" + tableName +	"' could not be found\"}");
		return;
	}

	/*
	 * Build a map of variable names and vars that will
	 * become the new default value for variables defined
	 * in a pyql script (under the params headings).
	 * 
	 * These will be reset upon each run of the script
	 * to return it's state back to original
	 */

	openset::query::ParamVars paramVars;

	if (params)
	{
		auto nodes = params->getNodes();
		for (auto n: nodes)
		{
			cvar paramVar;
			switch (n->type())
			{
				case cjsonType::VOIDED: 
				case cjsonType::NUL:
					paramVar = NULLCELL;
				break;
				case cjsonType::OBJECT: 
				case cjsonType::ARRAY: 
					continue;
				case cjsonType::INT: 
					paramVar = n->getInt();
				break;
				case cjsonType::DBL: 
					paramVar = n->getDouble();
				break;
				case cjsonType::STR: 
					paramVar = n->getCstr();
				break;
				case cjsonType::BOOL: 
					paramVar = n->getBool();
				break;
			}

			paramVars.emplace(n->nameCstr(), paramVar);
		}
	}

	openset::query::macro_s queryMacros; // this is our compiled code block
	openset::query::QueryParser p;

	try
	{
		p.compileQuery(queryCode.c_str(), table->getColumns(), queryMacros, &paramVars);
	}
	catch (const std::runtime_error &ex)
	{
		cjson response;

		error(
			openset::errors::Error{
				openset::errors::errorClass_e::parse, 
				openset::errors::errorCode_e::syntax_error, 
				std::string{ex.what()}
			}, 
			&response);

		message->reply(cjson::Stringify(&response, true));

		return;
	}

	if (p.error.inError())
	{
		cjson response; 
		cout << "---------------------------" << endl;
		cout << p.error.getErrorJSON() << endl;
		error(p.error, &response);
		message->reply(cjson::Stringify(&response, true));
		return;
	}

	const auto compileTime = Now() - startTime;
	const auto queryStart = Now();
	
	if (debug)
	{
		cjson response;
		response.set("debug", openset::query::MacroDbg(queryMacros));
		message->reply(cjson::Stringify(&response, true));
		return;
	}

	/*  
	 * We are originating the query.
	 * 
	 * At this point in the function we have validated that the
	 * script compiles, maps to the schema, is on a valid table,
	 * etc.
	 * 
	 * We will call our forkQuery function.
	 * 
	 * forQuery will call all the nodes (including this one) with the
	 * `is_fork` varaible set to true.
	 *
	 *
	 */
	if (!isFork)
	{
		forkQuery(
			database,
			table,
			partitions,
			request,
			queryMacros,
			message);
		return;
	}

	// We are a Fork!


	// create list of active_owner parititions for factory function
	auto activeList = openset::globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
		openset::globals::running->nodeId,
		{
			openset::mapping::NodeState_e::active_owner
		}
	);

	// Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
	//      we don't have to worry about locking anything shared between partitions in the same
	//      thread as they are executed serially, rather than in parallel. 
	//
	//      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
	//      as well as generally reduce the number of ResultSets needed (especially when partition
	//      counts are high).
	//
	//      Note: These are heap objects because we lose scope, as this function
	//            exits before the result objects are used.
	//
	std::vector<ResultSet*> resultSets;

	for (auto i = 0; i < partitions->getWorkerCount(); ++i)
		resultSets.push_back(new openset::result::ResultSet());

	// nothing active - return an empty set - not an error
	if (!activeList.size())
	{
		// 1. Merge the text hashes together
		auto mergedText = ResultMuxDemux::mergeText(
			queryMacros,
			table,
			resultSets);

		// 2. Merge the rows
		auto rows = ResultMuxDemux::mergeResultSets(
			queryMacros,
			table,
			resultSets);

		int64_t bufferLength = 0;

		const auto buffer = ResultMuxDemux::resultSetToInternode(
			queryMacros, table, rows, mergedText, bufferLength);

		// reply will be responsible for buffer
		message->reply(buffer, bufferLength);
		return;
	}

	
	/*
	 *  this Shuttle will gather our result sets roll them up and spit them back
	 *  
	 *  note that queryMacros are captured with a copy, this is because a reference
	 *  version will have had it's destructor called when the function exits.
	 *  
	 *  Note: ShuttleLamda comes in two versions, 
	 */ 

	//auto shuttle = new ShuttleLambdaAsync<CellQueryResult_s>(
	const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
		message,
		activeList.size(),
		[queryMacros, table, resultSets, startTime, queryStart, compileTime]
			(const vector<openset::async::response_s<CellQueryResult_s>> &responses,
			openset::comms::Message* message,
				voidfunc release_cb)
		{ // process the data and respond

			auto queryTime = Now() - queryStart;
			auto serialStart = Now();

			int64_t population = 0;
			int64_t totalPopulation = 0;

			// check for errors, add up totals
			for (const auto &r : responses)
			{
				if (r.data.error.inError())
				{
					// any error that is recorded should be considered a hard error, so report it
					message->reply(r.data.error.getErrorJSON());

					// clean up stray resultSets
					for (auto resultSet : resultSets)
						delete resultSet;

					return;
				}

				population += r.data.population;
				totalPopulation += r.data.totalPopulation;
			}

			// 1. Merge the text hashes together
			auto mergedText = ResultMuxDemux::mergeText(
				queryMacros,
				table,
				resultSets);

			// 2. Merge the rows
			auto rows = ResultMuxDemux::mergeResultSets(
				queryMacros, 
				table, 
				resultSets);
			
			int64_t bufferLength = 0;

			auto buffer = ResultMuxDemux::resultSetToInternode(
				queryMacros, table, rows, mergedText, bufferLength);

			message->reply(buffer, bufferLength);

			Logger::get().info("Fork query on " + table->getName());

			// clean up all those resultSet*
			for (auto r : resultSets)
				delete r;

			release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector
		});

	auto instance = 0;
	// pass factory function (as lambda) to create new cell objects
	partitions->cellFactory(activeList, [shuttle, table, queryMacros, resultSets, &instance](AsyncLoop* loop) -> OpenLoop*
		{
			instance++;
			return new OpenLoopQuery(shuttle, table, queryMacros, resultSets[loop->getWorkerId()], instance);
		});
}


void Query::onCount(
	Database* database,
	AsyncPool* partitions,
	bool isFork,
	cjson* request,
	openset::comms::Message* message)
{

	const auto tableName = request->xPathString("/params/table", "");
	const auto queryCode = request->xPathString("/params/code", "");
	const auto debug = request->xPathBool("/params/debug", false);
	const auto params = request->xPath("/params/params");

	const auto startTime = Now();


	if (!tableName.length())
	{
		message->reply("{\"error\":\"missing table name\"}");
		return;
	}

	if (!queryCode.length())
	{
		message->reply("{\"error\":\"missing query code\"}");
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		message->reply("{\"error\":\"table '" + tableName +	"' could not be found\"}");
		return;
	}


	/*
	* Build a map of variable names and vars that will
	* become the new default value for variables defined
	* in a pyql script (under the params headings).
	*
	* These will be reset upon each run of the script
	* to return it's state back to original
	*/

	openset::query::ParamVars paramVars;

	if (params)
	{
		auto nodes = params->getNodes();
		for (auto n : nodes)
		{
			cvar paramVar;
			switch (n->type())
			{
			case cjsonType::VOIDED:
			case cjsonType::NUL:
				paramVar = NULLCELL;
				break;
			case cjsonType::OBJECT:
			case cjsonType::ARRAY:
				continue;
			case cjsonType::INT:
				paramVar = n->getInt();
				break;
			case cjsonType::DBL:
				paramVar = n->getDouble();
				break;
			case cjsonType::STR:
				paramVar = n->getCstr();
				break;
			case cjsonType::BOOL:
				paramVar = n->getBool();
				break;
			}

			paramVars.emplace(n->nameCstr(), paramVar);
		}
	}

	// get the functions extracted and de-indented as named code blocks
	auto subQueries = openset::query::QueryParser::extractCountQueries(queryCode.c_str());

	openset::query::QueryPairs queries;

	// loop through the extracted functions (subQueries) and compile them
	for (auto r: subQueries)
	{

		openset::query::macro_s queryMacros; // this is our compiled code block
		openset::query::QueryParser p;
		p.compileQuery(r.second.c_str(), table->getColumns(), queryMacros, &paramVars);

		if (p.error.inError())
		{
			cjson response;
			error(p.error, &response);
			message->reply(cjson::Stringify(&response, true));
			return;
		}

		if (queryMacros.segmentTTL != -1)
			table->setSegmentTTL(r.first, queryMacros.segmentTTL);

		if (queryMacros.segmentRefresh != -1)
			table->setSegmentRefresh(r.first, queryMacros, queryMacros.segmentRefresh);

		queryMacros.isSegment = true;

		queries.emplace_back(std::pair<std::string, openset::query::macro_s>{r.first, queryMacros});
	}

	// Shared Results - Partitions spread across working threads (AsyncLoop's made by AsyncPool)
	//      we don't have to worry about locking anything shared between partitions in the same
	//      thread as they are executed serially, rather than in parallel. 
	//
	//      By creating one result set for each AsyncLoop thread we can have a lockless ResultSet
	//      as well as generally reduce the number of ResultSets needed (especially when partition
	//      counts are high).
	std::vector<ResultSet*> resultSets;

	const auto compileTime = Now() - startTime;
	const auto queryStart = Now();

	if (debug)
	{
		cjson response;
		// TODO fix this - response.set("debug", OpenSet::query::MacroDbg(queryMacros));
		message->reply(cjson::Stringify(&response, true));
		return;
	}

	/*
	* We are originating the query.
	*
	* At this point in the function we have validated that the
	* script compiles, maps to the schema, is on a valid table,
	* etc.
	*
	* We will call our forkQuery function.
	*
	* forQuery will call all the nodes (including this one) with the
	* `is_fork` varaible set to true.
	*
	*
	*/
	if (!isFork)
	{
		forkQuery(
			database,
			table,
			partitions,
			request,
			queries.front().second, // we node some macros, any macros
			message);
		return;
	}

	auto activeList = openset::globals::mapper->partitionMap.getPartitionsByNodeIdAndStates(
		openset::globals::running->nodeId,
		{ 
			openset::mapping::NodeState_e::active_owner 
		}
	);

	for (auto i = 0; i < partitions->getWorkerCount(); ++i)
		resultSets.push_back(new openset::result::ResultSet());

	// nothing active - return an empty set - not an error
	if (!activeList.size())
	{
		// 1. Merge the text hashes together
		auto mergedText = ResultMuxDemux::mergeText(
			queries.front().second,
			table,
			resultSets);

		// 2. Merge the rows
		auto rows = ResultMuxDemux::mergeResultSets(
			queries.front().second,
			table,
			resultSets);

		int64_t bufferLength = 0;

		const auto buffer = ResultMuxDemux::resultSetToInternode(
			queries.front().second, table, rows, mergedText, bufferLength);
		
		// reply is responible for buffer
		message->reply(buffer, bufferLength);
		return;
	}


	auto shuttle = new ShuttleLambda<CellQueryResult_s>(
		message,
		activeList.size(),
			[queries, table, resultSets, startTime, queryStart, compileTime]
			(const vector<openset::async::response_s<CellQueryResult_s>> &responses,
			 openset::comms::Message* message,
			 voidfunc release_cb)
		{ 
			// process the data and respond
			auto queryTime = Now() - queryStart;
			auto serialStart = Now();

			int64_t population = 0;
			int64_t totalPopulation = 0;

			// check for errors, add up totals
			for (const auto &r : responses)
			{
				if (r.data.error.inError())
				{
					// any error that is recorded should be considered a hard error, so report it
					message->reply(r.data.error.getErrorJSON());

					// clean up stray resultSets
					for (auto resultSet : resultSets)
						delete resultSet;

					return;
				}

				population += r.data.population;
				totalPopulation += r.data.totalPopulation;
			}

			// 1. Merge the text hashes together
			auto mergedText = ResultMuxDemux::mergeText(
				queries.front().second,
				table,
				resultSets);

			// 2. Merge the rows
			auto rows = ResultMuxDemux::mergeResultSets(
				queries.front().second,
				table,
				resultSets);

			int64_t bufferLength = 0;

			auto buffer = ResultMuxDemux::resultSetToInternode(
				queries.front().second, table, rows, mergedText, bufferLength);

			// reply is responsible for buffer
			message->reply(buffer, bufferLength);

			Logger::get().info("Fork count(s) on " + table->getName());

			// clean up all those resultSet*
			for (auto r : resultSets)
				delete r;

			release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector

		});

	auto instance = 0;
	auto workers = 0;
	// pass factory function (as lambda) to create new cell objects
	partitions->cellFactory(activeList, [shuttle, table, queries, resultSets, &workers, &instance](AsyncLoop* loop) -> OpenLoop*
	{
		instance++;
		auto partitionId = loop->partition;
		++workers;
		return new OpenLoopCount(shuttle, table, queries, resultSets[loop->getWorkerId()], instance);
	});

	Logger::get().info("Started " + to_string(workers) + " count worker async cells.");
}

void InternodeXfer::onXfer(
	Database* database,
	AsyncPool* partitions,	
	openset::comms::Message* message)
{
	// This is a binary message, it will contain an inbound table for a given partition.
	// in the header will be the partition, and table name. 

	Logger::get().info("transfer in (received " + to_string(message->length) + " bytes).");

	auto read = message->data;

	const auto partitionId = *recast<int32_t*>(read);
	read += 4;

	const auto tableNameLength = *recast<int32_t*>(read);
	read += 4;
	const std::string tableName(read);

	read += tableNameLength;

	openset::globals::async->suspendAsync();

	auto table = database->getTable(tableName);

	if (!table)
		table = database->newTable(tableName);

	// make table partition objects
	auto parts = table->getPartitionObjects(partitionId);
	// make async partition object (loop, etc).
	openset::globals::async->initPartition(partitionId);

	read += parts->attributes.deserialize(read);
	parts->people.deserialize(read);

	openset::globals::async->resumeAsync();
	
	// reply when done
	cjson response;
	response.set("transferred", true);
	message->reply(&response);	
}
