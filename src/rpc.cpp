#include <stdexcept>
#include <cinttypes>
#include <regex>

#include "rpc.h"
#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"
#include "oloop_query.h"
#include "oloop_count.h"

#include "asyncpool.h"
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
#include "names.h"
#include "http_serve.h"

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

void RpcError(openset::errors::Error error, const openset::web::MessagePtr message)
{
	message->reply(openset::http::StatusCode::client_error_bad_request, error.getErrorJSON());
}

static const unordered_map<string, internodeFunction_e> internodeMap = {
	{ "init_config_node", internodeFunction_e::init_config_node },
	{ "cluster_member", internodeFunction_e::cluster_member },
	{ "node_add", internodeFunction_e::node_add},
	{ "transfer", internodeFunction_e::transfer },
	{ "map_change", internodeFunction_e::map_change},
	{ "cluster_lock", internodeFunction_e::cluster_lock },
	{ "cluster_release", internodeFunction_e::cluster_release },
};

void RpcInternode::is_member(const openset::web::MessagePtr message, const RpcMapping&)
{
	cjson response;
	response.set("part_of_cluster", globals::running->state != openset::config::nodeState_e::ready_wait);
	message->reply(openset::http::StatusCode::success_ok, response);
}

void RpcInternode::join_to_cluster(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	globals::mapper->removeRoute(globals::running->nodeId);

	const auto request = message->getJSON();
	const auto nodeName = request.xPathString("/node_name", "");
	const auto nodeId = request.xPathInt("/node_id", 0);
	const auto partitionMax = request.xPathInt("/partition_max", 0);

	// TODO - error check here
	Logger::get().info("Joining cluster as: '" + nodeName + "'.");

	// assign a new node id
	{
		csLock lock(openset::globals::running->cs);
		globals::running->nodeId = nodeId;
		globals::running->state = openset::config::nodeState_e::active;
		globals::running->configVersion = 1;
		globals::running->partitionMax = partitionMax;
	}

	// create the routes
	openset::globals::mapper->deserializeRoutes(request.xPath("/routes"));

	// set number of partitions
	globals::async->setPartitionMax(partitionMax);
	// set them running - this return right away
	globals::async->startAsync();

	// set the partition map
	openset::globals::mapper->getPartitionMap()->deserializePartitionMap(request.xPath("/cluster"));
	globals::async->mapPartitionsToAsyncWorkers();

	globals::async->suspendAsync();
	// create the tables
	auto nodes = request.xPath("/tables")->getNodes();
	for (auto n : nodes)
	{
		auto tableName = n->xPathString("/name", "");

		if (!tableName.length())
			continue;

		auto table = openset::globals::database->newTable(tableName);

		table->deserializeTable(n->xPath("/table"));
		table->deserializeTriggers(n->xPath("/triggers"));
	}

	openset::globals::async->resumeAsync();

	Logger::get().info("configured for " + to_string(partitionMax) + " partitions.");

	cjson response;
	response.set("configured", true);
	message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::add_node(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	auto requestJson = message->getJSON();

	const auto nodeName = requestJson.xPathString("/node_name", "");
	const auto nodeId = requestJson.xPathInt("/node_id", 0);
	auto host = requestJson.xPathString("/host", "");
	const auto port = requestJson.xPathInt("/port", 0);

	if (host.length() && port && nodeId)
	{
		openset::globals::mapper->addRoute(nodeName, nodeId, host, port);
		Logger::get().info("added route " + globals::mapper->getRouteName(nodeId) + " @" + host + ":" + to_string(port) + ".");
	}
	else
	{
		Logger::get().error("change_cluster:node_add - missing params");
		RpcError(
			openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"change_cluster:node_add missing params" 
			},
			message);
		return;
	}

	cjson response;
	response.set("response", "thank you.");
	message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::transfer_init(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	const auto targetNode = message->getParamString("node");
	const auto partitionId = message->getParamInt("partition");

	std::vector<openset::db::Table*> tables;

	{ // get a list of tables
		csLock lock(globals::database->cs);
		for (const auto t : globals::database->tables)
			tables.push_back(t.second);
	}
	
	Logger::get().info("transfer started for partition " + to_string(partitionId) + ".");

	globals::async->suspendAsync();

	for (auto t : tables)
	{
		auto part = t->getPartitionObjects(partitionId);

		if (part)
		{
			char* blockPtr;
			int64_t blockSize;

			{
				HeapStack mem;

				// we need to stick a header on this
				// the header needs the partition and the table name
				// the data belongs to.

				// grab 4 bytes and assign the partitionId
				*(recast<int32_t*>(mem.newPtr(sizeof(int32_t)))) = partitionId;

				// grab 4 bytes and set it the length of the table name
				const auto tableNameLength = recast<int32_t*>(mem.newPtr(sizeof(int32_t)));
				*tableNameLength = t->getName().length() + 1;

				// grab some bytes for the table name, and copy in the table name
				const auto name = mem.newPtr(*tableNameLength);
				strcpy(name, t->getName().c_str());

				// serialize the attributes
				part->attributes.serialize(&mem);

				// serialize the people
				part->people.serialize(&mem);

				blockPtr = mem.flatten();
				blockSize = mem.getBytes();
			} // HeapStack mem gets release here

			const auto targetNodeId = globals::mapper->getRouteId(targetNode);

			// TODO - test for target

			const auto responseMessage = openset::globals::mapper->dispatchSync(
				targetNodeId,
				"POST",
				"/v1/internode/transfer",
				{},
				blockPtr,
				blockSize);

			if (!responseMessage)
				Logger::get().error("partition transfer error " + t->getName() + ".");
			else
				Logger::get().info(
					"transferred for table " + t->getName() + " to " + openset::globals::mapper->getRouteName(partitionId) + 
					" (transfered " + to_string(blockSize) + " bytes).");
		}
	}

	globals::async->resumeAsync();

	Logger::get().info("transfer complete on partition " + to_string(partitionId) + ".");

	cjson response;
	response.set("response", "thank you.");	
	message->reply(http::StatusCode::success_ok, response);
}

void RpcInternode::transfer_receive(const openset::web::MessagePtr message, const RpcMapping& matches)
{

	// This is a binary message, it will contain an inbound table for a given partition.
	// in the header will be the partition, and table name. 

	Logger::get().info("transfer in (received " + to_string(message->getPayloadLength()) + " bytes).");

	auto read = message->getPayload();

	const auto partitionId = *recast<int32_t*>(read);
	read += 4;

	const auto tableNameLength = *recast<int32_t*>(read);
	read += 4;
	const std::string tableName(read);

	read += tableNameLength;

	openset::globals::async->suspendAsync();

	auto table = globals::database->getTable(tableName);

	// TODO - skipping this might be correct, and return false
	if (!table)
		table = globals::database->newTable(tableName);

	// make table partition objects
	auto parts = table->getPartitionObjects(partitionId);
	// make async partition object (loop, etc).
	openset::globals::async->initPartition(partitionId);

	read += parts->attributes.deserialize(read);
	parts->people.deserialize(read);

	openset::globals::async->resumeAsync();

	Logger::get().info("transfer comlete");

	// reply when done
	cjson response;
	response.set("transferred", true);
	message->reply(http::StatusCode::success_ok, response);
}



void RpcInternode::map_change(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	// These callbacks allow us to clean objects up when the map is altered.
	// The map doesn't have knowledge of these objects (and shouldn't) and these
	// objects are not in that scope so this is a nice tidy way to do this

	const auto addPartition = [&](int partitionId)
	{
		// add this partition to the async pool, it will add it to a loop
		globals::async->initPartition(partitionId);
		globals::async->assertAsyncLock(); // dbg - assert we are in a lock

		for (auto t : globals::database->tables)
			t.second->getPartitionObjects(partitionId);
	};

	const auto removePartition = [&](int partitionId)
	{
		// drop this partition from the async engine
		globals::async->freePartition(partitionId);

		globals::async->assertAsyncLock();

		// drop this partition from any table objects
		for (auto t : globals::database->tables)
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

	const auto requestJson = message->getJSON();

	// map changes require the full undivided attention of the cluster!
	// nothing executing, means no goofy locks and no bad pointers
	openset::globals::mapper->changeMapping(
		requestJson,
		addPartition,
		removePartition,
		addRoute,
		removeRoute);

	globals::async->resumeAsync();

	cjson response;
	response.set("response", "thank you.");
	message->reply(http::StatusCode::success_ok, response);
}


enum class ForwardStatus_e : int
{
	dispatched,
	alreadyForwarded,
	error
};

ForwardStatus_e ForwardRequest(const openset::web::MessagePtr message)
{
	if (!openset::globals::mapper->routes.size())
		return ForwardStatus_e::error;
	
	if (message->getParamBool("forwarded"))
		return ForwardStatus_e::alreadyForwarded;

	auto newParams = message->getQuery();
	newParams.emplace("forwarded"s, "true"s);

	// broadcast to the cluster
	auto result = openset::globals::mapper->dispatchCluster(
		message->getMethod(),
		message->getPath(),
		newParams,
		message->getPayload(),
		message->getPayloadLength(),
		true);

	/*
	 * If it's not an error we reply with the first response received by the cluster
	 * as they are going to be all the same
	 */
	if (!result.routeError)
	{
		cjson response;
		cjson::Parse(
			string{
				result.responses[0].first,
				static_cast<size_t>(result.responses[0].second)
			},
			&response
		);

		message->reply(openset::http::StatusCode::success_ok, response);
	}
	else // if it's an error, reply with generic "something bad happened" type error
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::route_error,
			"potential node failure - please re-issue the request" },
			message);
	}

	openset::globals::mapper->releaseResponses(result);

	return (result.routeError) ? ForwardStatus_e::error : ForwardStatus_e::dispatched;
}

void RpcCluster::init(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	const auto partitions = openset::globals::async;
	const auto partitionMax = message->getParamInt("partitions"s, 0);

	if (partitionMax < 1 || partitionMax > 1000)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"partitions must be >= 1 and <= 1000" },
			message);
		return;
	}

	if (partitions->isRunning())
	{
		RpcError(
			openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"This instance is already part of a cluster" },
			message);
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

	cjson response;

	const auto logLine = globals::running->nodeName + " configured for " + to_string(partitionMax) + " partitions.";
	Logger::get().info(logLine);
	response.set("server_name", globals::running->nodeName);
	response.set("message", logLine);	
	
	// routes are broadcast to nodes, we use the external host and port
	// so that nodes can find each other in containered situations where
	// the container doesn't know it's own IP and ports are mapped
	openset::globals::mapper->addRoute(
		globals::running->nodeName, 
		globals::running->nodeId, 
		globals::running->hostExternal, 
		globals::running->portExternal);

	message->reply(http::StatusCode::success_ok, response);
}


void RpcCluster::join(const openset::web::MessagePtr message, const RpcMapping& matches)
{

	if (globals::running->state != openset::config::nodeState_e::active)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::route_error,
			"node_not_initialized" },
			message);
		return;
	}

	const auto host = message->getParamString("host");
	const auto port = message->getParamInt("port", 8080);

	const auto newNodeName = openset::config::createName();
	const auto newNodeId = MakeHash(newNodeName);

	if (!host.length() || !port)
	{
		Logger::get().error("invite node: missing params.");
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing host. Use param: host={host|ip}" },
			message);
		return;
	}

	// Step 1 - Verify that the remote node exists and is able to join
	{
		openset::web::Rest client(host + ":" + to_string(port));

		auto error = false;
		auto ready = false;

		cjson responseJson;

		client.request("GET", "/v1/internode/is_member", {}, nullptr, 0, [&error, &ready, &responseJson](http::StatusCode status, bool err, cjson json) mutable
		{
			error = err;

			if (!err)
				responseJson = std::move(json);

			ready = true;
		});

		// dumb loop, wait for callback
		while (!ready)
			ThreadSleep(50);

		if (error || responseJson.memberCount == 0)
		{
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"target node could not be reached." },
				message);
			return;
		}

		// node is already part of a cluster
		if (responseJson.xPathBool("/part_of_cluster", true))
		{
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"target node already part of a cluster." },
				message);
			return;
		}
	}


	// Step 2 - The remote node is open to being configured, lets send it the entire config
	{
		// TODO glue together the whole config
		cjson configBlock;

		configBlock.set("node_name", newNodeName);
		configBlock.set("node_id", newNodeId);
		configBlock.set("partition_max", cast<int64_t>(openset::globals::async->partitionMax));

		// make am array node called tables, push the tables, triggers, columns into the array
		auto tables = configBlock.setArray("tables");

		for (auto n : openset::globals::database->tables)
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
		openset::globals::mapper->serializeRoutes(configBlock.setObject("routes"));

		// make a node called cluster, serialize the partitionMap under it
		openset::globals::mapper->getPartitionMap()->serializePartitionMap(configBlock.setObject("cluster"));


		auto rpcJson = cjson::Stringify(&configBlock);

		cout << rpcJson << endl;

		Logger::get().info("configuring node " + newNodeName + "@" + host + ":" + to_string(port) + ".");

		openset::web::Rest client(host + ":" + to_string(port));

		auto error = false;
		auto ready = false;

		cjson responseJson;

		// send command that joins remote node to this cluster, this transfers all
		// config to the remote node.
		client.request(
            "POST", 
            "/v1/internode/join_to_cluster", 
            {}, 
            &rpcJson[0], 
            rpcJson.length(), 
            [&error, &ready, &responseJson](http::StatusCode status, bool err, cjson json)
		{
			error = err;

			if (!err)
				responseJson = std::move(json);

			ready = true;
		});

		// dumb loop, wait for callback
		while (!ready)
			ThreadSleep(50);


		if (error || responseJson.memberCount == 0)
		{
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"target node could not be reached." },
				message);
			return;
		}

		// copy to string
		if (!responseJson.xPathBool("/configured", false))
		{
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"target node could not be configured." },
				message);
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
		//params->set("config_version", globals::running->updateConfigVersion());
		newNode.set("node_name", newNodeName);
		newNode.set("node_id", newNodeId);
		newNode.set("host", host);
		newNode.set("port", port);

		auto newNodeJson = cjson::Stringify(&newNode);

		auto addResponses = openset::globals::mapper->dispatchCluster(
			"POST",
			"/v1/internode/add_node",
			{},
			newNode,
			false);

		openset::globals::mapper->releaseResponses(addResponses);

	}

	// respond to client
	cjson response;
	response.set("node_joined", true);
	message->reply(http::StatusCode::success_ok, response);
}



void RpcTable::table_create(const openset::web::MessagePtr message, const RpcMapping& matches)
{	

	// this request must be forwarded to all the other nodes
	if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
		return;
	
	auto database = openset::globals::database;
	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;

	if (!tableName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"bad table name" },
			message);
		return;
	}

	if (database->getTable(tableName))
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table already exists" },
			message);
		return;
	}

	// TODO - look for spaces, check length, look for symbols that aren't - or _ etc.

	const auto sourceColumns = request.xPath("/columns");
	if (!sourceColumns)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"column definition required, missing /columns" },
			message);

		return;
	}

	const auto sourceZOrder = request.xPath("/z_order");

	auto sourceColumnsList = sourceColumns->getNodes();

	globals::async->suspendAsync();
	auto table = database->newTable(tableName);
	auto columns = table->getColumns();

	// set the default required columns
	columns->setColumn(COL_STAMP, "__stamp", columnTypes_e::intColumn, false);
	columns->setColumn(COL_ACTION, "__action", columnTypes_e::textColumn, false);
	columns->setColumn(COL_UUID, "__uuid", columnTypes_e::intColumn, false);
	columns->setColumn(COL_TRIGGERS, "__triggers", columnTypes_e::textColumn, false);
	columns->setColumn(COL_EMIT, "__emit", columnTypes_e::textColumn, false);
	columns->setColumn(COL_SEGMENT, "__segment", columnTypes_e::textColumn, false);
	columns->setColumn(COL_SESSION, "__session", columnTypes_e::intColumn, false);

	int64_t columnEnum = 1000;

	for (auto n: sourceColumnsList)
	{
		auto name = n->xPathString("/name", "");
		auto type = n->xPathString("/type", "");

		if (!name.size() || !type.size())
		{
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"primary column name or type" },
				message);
			return;
		}

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
			RpcError(
				openset::errors::Error{
				openset::errors::errorClass_e::config,
				openset::errors::errorCode_e::general_config_error,
				"invalide column type" },
				message);

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

	globals::async->resumeAsync();

	const auto logLine = "table '" + tableName + "' created.";
	Logger::get().info(logLine);

	cjson response;
	response.set("message", logLine);
	message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::table_describe(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	auto database = openset::globals::database;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;

	if (!tableName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing table name" },
			message);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			message);

		return;
	}

	cjson response;

	response.set("table", tableName);

	auto columnNodes = response.setArray("columns");
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

	const auto logLine = "describe table '" + tableName + "'.";
	Logger::get().info(logLine);
	
	response.set("message", logLine);
	message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_add(const openset::web::MessagePtr message, const RpcMapping& matches)
{

	// this request must be forwarded to all the other nodes
	if (ForwardRequest(message) != ForwardStatus_e::alreadyForwarded)
		return;

	auto database = openset::globals::database;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto columnName = matches.find("name"s)->second;
	const auto columnType = matches.find("type"s)->second;

	if (!tableName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			message);

		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			message);
		return;
	}

	if (!columnName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing or invalid column name" },
			message);
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

	columnTypes_e colType;

	if (columnType == "text")
		colType = columnTypes_e::textColumn;
	else if (columnType == "int")
		colType = columnTypes_e::intColumn;
	else if (columnType == "double")
		colType = columnTypes_e::doubleColumn;
	else if (columnType == "bool")
		colType = columnTypes_e::boolColumn;
	else
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing or invalid column type" },
			message);

		return; // TODO hmmm...
	}

	columns->setColumn(lowest, columnName, colType, false, false);

	const auto logLine = "added column '" + columnName + "' from table '" + tableName + "' created.";
	Logger::get().info(logLine);

	cjson response;
	response.set("message", logLine);
	response.set("table", tableName);
	response.set("column", columnName);
	response.set("type", columnType);
	message->reply(http::StatusCode::success_ok, response);
}

void RpcTable::column_drop(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	auto database = openset::globals::database;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto columnName = matches.find("name"s)->second;

	if (!tableName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"missing /params/table" },
			message);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"table not found" },
			message);
		return;
	}

	if (!columnName.size())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"invalid column name" },
			message);
		return;
	}

	const auto column = table->getColumns()->getColumn(columnName);

	if (!column)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::config,
			openset::errors::errorCode_e::general_config_error,
			"column not found" },
			message);
		return;
	}

	// delete the actual column
	table->getColumns()->deleteColumn(column);

	const auto logLine = "dropped column '" + columnName + "' from table '" + tableName + "' created.";
	Logger::get().info(logLine);
	cjson response;
	response.set("message", logLine);	
	response.set("table", tableName);
	response.set("column", columnName);
	message->reply(http::StatusCode::success_ok, response);
}

void RpcRevent::revent_create(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	/*
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

	const auto logLine = "set trigger '" + triggerName + "' on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
	*/
}

void RpcRevent::revent_describe(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	/*
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

	const auto triggers = table->getTriggerConf();

	auto triggerNode = response->setArray("triggers");

	for (auto &t: *triggers)
	{
		auto trigNode = triggerNode->pushObject();
		trigNode->set("name", t.second->name);
		trigNode->set("entry", t.second->entryFunction);
	}

	const auto logLine = "describe triggers on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
	*/
}

void RpcRevent::revent_drop(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	/*
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

	const auto triggers = table->getTriggerConf();
	const auto trigger = triggers->find(triggerName);

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

	const auto logLine = "dropped trigger '" + triggerName + "' on table '" + tableName + "'.";
	Logger::get().info(logLine);
	response->set("message", logLine);
	*/
}

void RpcInsert::insert(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	auto database = openset::globals::database;
	const auto partitions = openset::globals::async;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto isFork = message->getParamBool("fork");

	if (!partitions->getPartitionMax())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::insert,
			openset::errors::errorCode_e::route_error,
			"node_not_initialized" },
			message);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::insert,
			openset::errors::errorCode_e::general_error,
			"missing or invalid table name" },
			message);
		return;
	}

	auto rows = request.getNodes();

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

	const auto thankyouCB = [](http::StatusCode, bool, char*, size_t)
	{		
        // TODO - we should probably handle this horrible possibility somehow.
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
				"POST",
				"/v1/insert/" + tableName,
				{},
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

	cjson response;
	response.set("message", "thank you.");
	message->reply(http::StatusCode::success_ok, response);
}

void Feed::onSub(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	/*
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
	*/
}

enum class queryFunction_e : int32_t
{
	none,
	status,
	query,
	count,
};


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
shared_ptr<cjson> forkQuery(
	Table* table,
	const openset::web::MessagePtr message,
	openset::query::Macro_s queryMacros) // errorHandled will be true if an error was happend during the fork
{
	auto newParams = message->getQuery();
	newParams.emplace("fork", "true");
	
	// call all nodes and gather results - JSON is what's coming back
	// NOTE - it would be fully possible to flatten results to binary
	auto result = openset::globals::mapper->dispatchCluster(
		message->getMethod(),
		message->getPath(),
		newParams,
		message->getPayload(),
		message->getPayloadLength(),
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
			if (!r.second)
				RpcError(
					openset::errors::Error{
						openset::errors::errorClass_e::internode,
						openset::errors::errorCode_e::internode_error,
						"Cluster error. Node had empty reply."},
					message);
			else
				message->reply(openset::http::StatusCode::success_ok, r.first, r.second);
			return nullptr;
		}
	}
	
	auto resultJson = make_shared<cjson>();

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
		resultJson.get(), // bare pointer fine here
		rows,
		mergedText);


	// local function to fill Meta data in result JSON
	const auto fillMeta = [](const openset::query::VarList& mapping, cjson* jsonArray) {

		for (auto c : mapping)
		{
			auto tNode = jsonArray->pushObject();

			if (c.modifier == openset::query::Modifiers_e::var)
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
				auto mode = openset::query::ModifierDebugStrings.at(c.modifier);
				toLower(mode);
				tNode->set("mode", mode);
				tNode->set("name", c.alias);
				tNode->set("type", "int");
			}
			else
			{
				auto mode = openset::query::ModifierDebugStrings.at(c.modifier);
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
	auto metaJson = resultJson->setObject("info");

	auto dataJson = metaJson->setObject("data");
	fillMeta(queryMacros.vars.columnVars, dataJson->setArray("columns"));
	//fillMeta(queryMacros.vars.groupVars, dataJson->setArray("groups"));


	//metaJson->set("query_time", queryTime);
	//metaJson->set("pop_evaluated", population);
	//metaJson->set("pop_total", totalPopulation);
	//metaJson->set("compile_time", compileTime);
	//metaJson->set("serialize_time", serialTime);
	//metaJson->set("total_time", elapsed);

	Logger::get().info("RpcQuery on " + table->getName());

	// free up the responses
	openset::globals::mapper->releaseResponses(result);

	// clean up all those resultSet*
	for (auto r : resultSets)
		delete r;

	return std::move(resultJson);
}


void RpcQuery::events(const openset::web::MessagePtr message, const RpcMapping& matches)
{

	auto database = openset::globals::database;
	const auto partitions = openset::globals::async;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto queryCode = std::string{ message->getPayload(), message->getPayloadLength() };

	const auto debug = message->getParamBool("debug");
	const auto isFork = message->getParamBool("fork");

	const auto log = "Inbound events query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
	Logger::get().info(log);
	
	const auto startTime = Now();

	if (!tableName.length())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"missing or invalid table name" },
			message);
		return;
	}

	if (!queryCode.length())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"missing query code (POST query as text)" },
			message);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"table could not be found" },
			message);
		return;
	}

	// override session time if provided, otherwise use table default
	const auto sessionTime = message->getParamInt("session_time", table->getSessionTime());

	/*
	 * Build a map of variable names and vars that will
	 * become the new default value for variables defined
	 * in a pyql script (under the params headings).
	 * 
	 * These will be reset upon each run of the script
	 * to return it's state back to original
	 */

	openset::query::ParamVars paramVars;

	/* TODO let us revist these
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
					paramVar = NONE;
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
	*/

	openset::query::Macro_s queryMacros; // this is our compiled code block
	openset::query::QueryParser p;

	try
	{
		p.compileQuery(queryCode.c_str(), table->getColumns(), queryMacros, &paramVars);
	}
	catch (const std::runtime_error &ex)
	{
		RpcError(
			openset::errors::Error{
				openset::errors::errorClass_e::parse, 
				openset::errors::errorCode_e::syntax_error, 
				std::string{ex.what()}
			}, 
			message);
		return;
	}

	if (p.error.inError())
	{
		Logger::get().error(p.error.getErrorJSON());
		message->reply(http::StatusCode::client_error_bad_request, p.error.getErrorJSON());
		return;
	}

	// set the sessionTime (timeout) value, this will get relayed 
	// through the to oloop_query, the person object and finally the grid
	queryMacros.sessionTime = sessionTime;

	const auto compileTime = Now() - startTime;
	const auto queryStart = Now();
	
	if (debug)
	{
		auto debugOutput = openset::query::MacroDbg(queryMacros);

		// TODO - add functions for reply content types and error codes

		// reply as text
		message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
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
		const auto json = std::move(forkQuery(table, message, queryMacros));
		if (json) // if null/empty we had an error
			message->reply(http::StatusCode::success_ok, *json);
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
		message->reply(http::StatusCode::success_ok, buffer, bufferLength);
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
			(vector<openset::async::response_s<CellQueryResult_s>> &responses,
			openset::web::MessagePtr message,
				voidfunc release_cb)
		{ // process the data and respond

			int64_t population = 0;
			int64_t totalPopulation = 0;

			// check for errors, add up totals
			for (const auto &r : responses)
			{
				if (r.data.error.inError())
				{
					// any error that is recorded should be considered a hard error, so report it
					auto errorMessage = r.data.error.getErrorJSON();
					message->reply(http::StatusCode::client_error_bad_request, errorMessage);
										
					// clean up stray resultSets
					for (auto resultSet : resultSets)
						delete resultSet;

					release_cb();

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

			message->reply(http::StatusCode::success_ok, buffer, bufferLength);

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


void RpcQuery::counts(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	auto database = openset::globals::database;
	const auto partitions = openset::globals::async;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto queryCode = std::string{ message->getPayload(), message->getPayloadLength() };

	const auto debug = message->getParamBool("debug");
	const auto isFork = message->getParamBool("fork");

	const auto startTime = Now();

	const auto log = "Inbound counts query (fork: "s + (isFork ? "true"s : "false"s) + ")"s;
	Logger::get().info(log);

	if (!tableName.length())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"missing or invalid table name" },
			message);
		return;
	}

	if (!queryCode.length())
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"missing query code (POST query as text)" },
			message);
		return;
	}

	auto table = database->getTable(tableName);

	if (!table)
	{
		RpcError(
			openset::errors::Error{
			openset::errors::errorClass_e::query,
			openset::errors::errorCode_e::general_error,
			"table could not be found" },
			message);
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

	/*
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
				paramVar = NONE;
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
	*/

	// get the functions extracted and de-indented as named code blocks
	auto subQueries = openset::query::QueryParser::extractCountQueries(queryCode.c_str());

	openset::query::QueryPairs queries;

	// loop through the extracted functions (subQueries) and compile them
	for (auto r: subQueries)
	{

		openset::query::Macro_s queryMacros; // this is our compiled code block
		openset::query::QueryParser p;
		p.compileQuery(r.second.c_str(), table->getColumns(), queryMacros, &paramVars);

		if (p.error.inError())
		{
			cjson response;
			// FIX error(p.error, &response);
			message->reply(http::StatusCode::client_error_bad_request, cjson::Stringify(&response, true));
			return;
		}

		if (queryMacros.segmentTTL != -1)
			table->setSegmentTTL(r.first, queryMacros.segmentTTL);

		if (queryMacros.segmentRefresh != -1)
			table->setSegmentRefresh(r.first, queryMacros, queryMacros.segmentRefresh);

		queryMacros.isSegment = true;

		queries.emplace_back(std::pair<std::string, openset::query::Macro_s>{r.first, queryMacros});
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
		std::string debugOutput;

		for (auto &m : queries)
			debugOutput += 
				"Script: " + m.first + 
				"\n=====================================================================================\n\n" +
				openset::query::MacroDbg(m.second);

		// TODO - add functions for reply content types and error codes

		// reply as text
		message->reply(http::StatusCode::success_ok, &debugOutput[0], debugOutput.length());
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
		const auto json = std::move(forkQuery(table, message, queries.front().second));
		if (json) // if null/empty we had an error
			message->reply(http::StatusCode::success_ok, *json);
		return;
	}

	// We are a Fork!

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
		message->reply(http::StatusCode::success_ok, buffer, bufferLength);
		return;
	}


	auto shuttle = new ShuttleLambda<CellQueryResult_s>(
		message,
		activeList.size(),
			[queries, table, resultSets, startTime, queryStart, compileTime]
			(const vector<openset::async::response_s<CellQueryResult_s>> &responses,
			 openset::web::MessagePtr message,
			 voidfunc release_cb)
		{ 
			// process the data and respond
			int64_t population = 0;
			int64_t totalPopulation = 0;

			// check for errors, add up totals
			for (const auto &r : responses)
			{
				if (r.data.error.inError())
				{
					// any error that is recorded should be considered a hard error, so report it
					message->reply(http::StatusCode::client_error_bad_request, r.data.error.getErrorJSON());

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
			message->reply(http::StatusCode::success_ok, buffer, bufferLength);

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
		++instance;
		++workers;
		return new OpenLoopCount(shuttle, table, queries, resultSets[loop->getWorkerId()], instance);
	});

	Logger::get().info("Started " + to_string(workers) + " count worker async cells.");
}

void openset::comms::Dispatch(const openset::web::MessagePtr message)
{
	const auto path = message->getPath();

	//for_each(MatchList.begin(),MatchList.end(), [&path, &message](auto &item)
	for (auto& item: MatchList)
	{		
		const auto& [method, rx, handler, packing] = item;

		if (std::smatch matches; message->getMethod() == method && regex_match(path, matches, rx))
		{
			RpcMapping matchMap;

			for (auto &p : packing)
				if (p.first < matches.size())
					matchMap[p.second] = matches[p.first];

			handler(message, matchMap);
			return;
		}
	};

	message->reply(http::StatusCode::client_error_bad_request, "rpc not found");
}
