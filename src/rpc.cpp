#include <stdexcept>
#include <cinttypes>
#include <regex>

#include "rpc.h"
#include "cjson/cjson.h"
#include "str/strtools.h"
#include "sba/sba.h"
#include "oloop_insert.h"
#include "oloop_query.h"
#include "oloop_segment.h"
#include "oloop_person.h"
#include "oloop_column.h"
#include "oloop_histogram.h"

#include "asyncpool.h"
#include "asyncloop.h"
#include "config.h"
#include "sentinel.h"
#include "querycommon.h"
#include "queryparser.h"
#include "database.h"
#include "result.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "internoderouter.h"
#include "names.h"
#include "http_serve.h"

//#include "trigger.h"

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

void RpcInternode::is_member(const openset::web::MessagePtr message, const RpcMapping&)
{
	cjson response;
	response.set("part_of_cluster", globals::running->state != openset::config::NodeState_e::ready_wait);
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
		globals::running->state = openset::config::NodeState_e::active;
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

	Logger::get().info(globals::running->nodeName + " configured for " + to_string(partitionMax) + " partitions.");

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
				result.responses[0].data,
				result.responses[0].length
			},
			&response
		);

		message->reply(openset::http::StatusCode::success_ok, response);
	}
	else // if it's an error, reply with generic "something bad happened" type error
	{

        auto nodeFail = false;

        // try to capture a json error that has perculated up from the forked call.
        if (result.responses[0].data &&
            result.responses[0].length &&
            result.responses[0].data[0] == '{')
        {
            cjson error(std::string(result.responses[0].data, result.responses[0].length), result.responses[0].length);

            if (error.xPath("/error"))
                message->reply(openset::http::StatusCode::client_error_bad_request, error);
            else
                nodeFail = true;
        }
        else
            nodeFail = true;

        if (nodeFail)
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::route_error,
                "potential node failure - please re-issue the request" },
                message);
        }
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
		globals::running->state = openset::config::NodeState_e::active;
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

	Logger::get().info(globals::running->nodeName + " configured for " + to_string(partitionMax) + " partitions.");

    cjson response;
    response.set("server_name", globals::running->nodeName);
	
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

	if (globals::running->state != openset::config::NodeState_e::active)
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

    // validate column names and types
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
                "missing column type or name" },
                message);
            return;
        }

        // bad type
        if (openset::db::ColumnTypes.find(type) == openset::db::ColumnTypes.end())
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column type: must be int|double|text|bool" },
                message);
            return;
        }

        if (!openset::db::Columns::validColumnName(name))
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
            return;
        }

    }


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
	    const auto name = n->xPathString("/name", "");
	    const auto type = n->xPathString("/type", "");

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

	Logger::get().info("table '" + tableName + "' created.");

	cjson response;
	response.set("message", "created");
    response.set("table", tableName);
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
		if (c.idx > 6 && c.deleted == 0 && c.name.size() && c.type != columnTypes_e::freeColumn)
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
			columnRecord->set("type", type);
            //columnRecord->set("index", cast<int64_t>(c.idx)); not required for describe, possibly confusing
		}

    Logger::get().info("describe table '" + tableName + "'.");	
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

    if (!openset::db::Columns::validColumnName(columnName))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column name: may contain lowercase a-z, 0-9 and _ but cannot start with a number." },
                message);
        return;
    }

    if (openset::db::ColumnTypes.find(columnType) == openset::db::ColumnTypes.end())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::config,
                openset::errors::errorCode_e::general_config_error,
                "bad column type: must be int|double|text|bool" },
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

	Logger::get().info("added column '" + columnName + "' from table '" + tableName + "' created.");

	cjson response;
	response.set("message", "added");
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

    if (!column || column->type == columnTypes_e::freeColumn)
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

	Logger::get().info("dropped column '" + columnName + "' from table '" + tableName + "' created.");

	cjson response;
	response.set("message", "dropped");	
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
        const auto personNode = row->xPath("/person");
        if (!personNode || (personNode->type() != cjsonType::INT && personNode->type() != cjsonType::STR))
            continue;

       
        // straight up numeric ID nodes don't need hashing, actually hashing would be very bad.
        // We can use numeric IDs (i.e. a customer id) directly.
        int64_t uuid = 0;

        if (personNode->type() == cjsonType::STR)
        {
            auto uuString = personNode->getString();
            toLower(uuString);

            if (uuString.length())
                uuid = MakeHash(uuString);
        }
        else
            uuid = personNode->getInt(); 

		const auto destination = cast<int32_t>(std::abs(uuid) % partitions->getPartitionMax());

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
	response.set("message", "yummy");
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
    const int resultColumnCount,
    const int resultSetCount,
    const ResultSortMode_e sortMode = ResultSortMode_e::column,
    const ResultSortOrder_e sortOrder = ResultSortOrder_e::Desc,
    const int sortColumn = 0,
    const int trim = -1,
    const int64_t bucket = 0,
    const int64_t forceMin = std::numeric_limits<int64_t>::min(),
    const int64_t forceMax = std::numeric_limits<int64_t>::min())
{
	auto newParams = message->getQuery();
	newParams.emplace("fork", "true");

    const auto setCount = resultSetCount ? resultSetCount : 1;
	
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
		if (ResultMuxDemux::isInternode(r.data, r.length))
			resultSets.push_back(ResultMuxDemux::internodeToResultSet(r.data, r.length));
		else
		{
			// there is an error message from one of the participing nodes
			// TODO - handle error
			if (!r.data || !r.length)
				RpcError(
					openset::errors::Error{
						openset::errors::errorClass_e::internode,
						openset::errors::errorCode_e::internode_error,
						"Cluster error. Node had empty reply."},
					message);
			else if (r.code != openset::http::StatusCode::success_ok)
            {
                // try to capture a json error that has perculated up from the forked call.
                if (r.data &&
                    r.length &&
                    r.data[0] == '{')
                {
                    cjson error(std::string(r.data, r.length), r.length);

                    if (error.xPath("/error"))
                    {
                        message->reply(openset::http::StatusCode::client_error_bad_request, error);

                        // free up the responses
                        openset::globals::mapper->releaseResponses(result);
                        // clean up all those resultSet*
                        for (auto r : resultSets)
                            delete r;

                        return nullptr;
                    }
                    else
                        result.routeError = true;
                }
                else
                    result.routeError = true; // this will trigger the next error
            }
        }

        if (result.routeError)
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::config,
                    openset::errors::errorCode_e::route_error,
                    "potential node failure - please re-issue the request" },
                    message);

            // free up the responses
            openset::globals::mapper->releaseResponses(result);
            // clean up all those resultSet*
            for (auto r : resultSets)
                delete r;

            return nullptr;
        }
	}
    
    auto resultJson = make_shared<cjson>();
    ResultMuxDemux::resultSetToJson(
        resultColumnCount,
        setCount,
        resultSets,
        resultJson.get());

    // free up the responses
    openset::globals::mapper->releaseResponses(result);
    // clean up all those resultSet*
    for (auto r : resultSets)
        delete r;

    if (bucket)
        ResultMuxDemux::jsonResultHistogramFill(resultJson.get(), bucket, forceMin, forceMax);

    switch (sortMode)
    {
        case ResultSortMode_e::key: 
            ResultMuxDemux::jsonResultSortByGroup(resultJson.get(), sortOrder);
        break;
        case ResultSortMode_e::column: 
            ResultMuxDemux::jsonResultSortByColumn(resultJson.get(), sortOrder, sortColumn);
        break;
        default: ;
    }

    ResultMuxDemux::jsonResultTrim(resultJson.get(), trim);

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
	//auto metaJson = resultJson->setObject("info");
	//auto dataJson = metaJson->setObject("data");
	//fillMeta(queryMacros.vars.columnVars, dataJson->setArray("columns"));
	//fillMeta(queryMacros.vars.groupVars, dataJson->setArray("groups"));


	//metaJson->set("query_time", queryTime);
	//metaJson->set("pop_evaluated", population);
	//metaJson->set("pop_total", totalPopulation);
	//metaJson->set("compile_time", compileTime);
	//metaJson->set("serialize_time", serialTime);
	//metaJson->set("total_time", elapsed);

	Logger::get().info("RpcQuery on " + table->getName());

	return std::move(resultJson);
}

openset::query::ParamVars getInlineVaraibles(const openset::web::MessagePtr message)
{
    /*
    * Build a map of variable names and vars that will
    * become the new default value for variables defined
    * in a pyql script (under the params headings).
    *
    * These will be reset upon each run of the script
    * to return it's state back to original
    */
    openset::query::ParamVars paramVars;

    for (auto p : message->getQuery())
    {
        cvar value = p.second;

        if (p.first.find("str_") != string::npos)
        {
            auto name = trim(p.first.substr(4));

            if (name.length())
                paramVars[name] = value;
        }
        else if (p.first.find("int_") != string::npos)
        {
            auto name = trim(p.first.substr(4));

            if (name.length())
                paramVars[name] = value.getInt64();
        }
        else if (p.first.find("dbl_") != string::npos)
        {
            auto name = trim(p.first.substr(4));

            if (name.length())
                paramVars[name] = value.getDouble();
        }
        else if (p.first.find("bool_") != string::npos)
        {
            auto name = trim(p.first.substr(4));

            if (name.length())
                paramVars[name] = value.getBool();
        }

    }

    return std::move(paramVars);
}

void RpcQuery::event(const openset::web::MessagePtr message, const RpcMapping& matches)
{

	auto database = openset::globals::database;
	const auto partitions = openset::globals::async;

	const auto request = message->getJSON();
	const auto tableName = matches.find("table"s)->second;
	const auto queryCode = std::string{ message->getPayload(), message->getPayloadLength() };

	const auto debug = message->getParamBool("debug");
	const auto isFork = message->getParamBool("fork");

    const auto trimSize = message->getParamInt("trim", -1);
    const auto sortOrder = message->getParamString("order", "desc") == "asc" ? ResultSortOrder_e::Asc : ResultSortOrder_e::Desc;

    auto sortColumnName = ""s;
    auto sortMode = ResultSortMode_e::column;
    if (message->isParam("sort"))
    {
        sortColumnName = message->getParamString("sort");
        if (sortColumnName == "key" || sortColumnName == "g")
            sortMode = ResultSortMode_e::key;
    }

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

    openset::query::ParamVars paramVars = getInlineVaraibles(message);
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

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts = split(segmentText, ',');

        queryMacros.segments.clear();

        for (auto p : parts)
        {
            p = trim(p);
            if (p.length())
                queryMacros.segments.push_back(p);
        }

        if (!queryMacros.segments.size())
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "no segment names specified" },
                message);
            return;
        }
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

    auto sortColumn = 0;

    if (sortMode != ResultSortMode_e::key && sortColumnName.size())
    {
        if (sortColumnName == "person" || sortColumnName == "people")
            sortColumnName = "__uuid";
        else if (sortColumnName == "stamp")
            sortColumnName = "__stamp";
        else if (sortColumnName == "session")
            sortColumnName = "__session";

        auto set = false;
        auto idx = -1;
        for (auto &c: queryMacros.vars.columnVars)
        {
            ++idx;
            if (c.alias == sortColumnName)
            {
                set = true;
                sortColumn = c.index;
                break;
            }
        }

        if (!set)
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::parse,
                    openset::errors::errorCode_e::syntax_error,
                    "sort column not found in query aggregates"
                },
                message);
            return;
        }
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

		const auto json = std::move(
            forkQuery(
                table, 
                message, 
                queryMacros.vars.columnVars.size(), 
                queryMacros.segments.size(),
                sortMode,
                sortOrder,
                sortColumn,
                trimSize)
        );

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
		resultSets.push_back(
            new openset::result::ResultSet(
                queryMacros.vars.columnVars.size() * (queryMacros.segments.size() ? queryMacros.segments.size() : 1)
            )
        );

	// nothing active - return an empty set - not an error
	if (!activeList.size())
	{
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(
            queryMacros.vars.columnVars.size(),
            queryMacros.indexes.size(),
            resultSets,
            bufferLength);

		// reply will be responsible for buffer
		message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;
        
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
				voidfunc release_cb) mutable
		{ // process the data and respond
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
			}

            // 1. Merge the Macro Literals in
            ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

            // 2. Merge the rows
            int64_t bufferLength = 0;
            const auto buffer = ResultMuxDemux::multiSetToInternode(
                queryMacros.vars.columnVars.size(),
                queryMacros.indexes.size(),
                resultSets,
                bufferLength);

			message->reply(http::StatusCode::success_ok, buffer, bufferLength);

			Logger::get().info("Fork query on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;

            PoolMem::getPool().freePtr(buffer);

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


void RpcQuery::segment(const openset::web::MessagePtr message, const RpcMapping& matches)
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


    openset::query::ParamVars paramVars = getInlineVaraibles(message);
	// get the functions extracted and de-indented as named code blocks
	auto subQueries = openset::query::QueryParser::extractSections(queryCode.c_str());

	openset::query::QueryPairs queries;

	// loop through the extracted functions (subQueries) and compile them
	for (auto r: subQueries)
	{

        if (r.sectionType != "segment")
            continue;

		openset::query::Macro_s queryMacros; // this is our compiled code block
		openset::query::QueryParser p;
		p.compileQuery(r.code.c_str(), table->getColumns(), queryMacros, &paramVars);

		if (p.error.inError())
		{
			cjson response;
			// FIX error(p.error, &response);
			message->reply(http::StatusCode::client_error_bad_request, cjson::Stringify(&response, true));
			return;
		}

        if (r.flags.contains("ttl"))
        {
            queryMacros.segmentTTL = r.flags["ttl"];
            table->setSegmentTTL(r.sectionName, r.flags["ttl"]);
        }

        if (r.flags.contains("refresh"))
        {
            queryMacros.segmentRefresh = r.flags["refresh"];
            table->setSegmentRefresh(r.sectionName, queryMacros, r.flags["refresh"]);
        }

		queryMacros.isSegment = true;

		queries.emplace_back(std::pair<std::string, openset::query::Macro_s>{r.sectionName, queryMacros});
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
    if (!queries.size())
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "no @segment sections could be found" },
                message);
        return;
    }
    
	if (!isFork)
	{
		const auto json = std::move(
            forkQuery(
                table, 
                message, 
                queries.front().second.vars.columnVars.size(), 
                queries.front().second.segments.size())
        );
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
		resultSets.push_back(
            new openset::result::ResultSet(1)
        );

	// nothing active - return an empty set - not an error
	if (!activeList.size())
	{
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queries.front().second, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(
            1,
            1,
            resultSets,
            bufferLength);
		
		// reply is responible for buffer
		message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;
        
		return;
	}

	auto shuttle = new ShuttleLambda<CellQueryResult_s>(
		message,
		activeList.size(),
			[queries, table, resultSets, startTime, queryStart, compileTime]
			(const vector<openset::async::response_s<CellQueryResult_s>> &responses,
			 openset::web::MessagePtr message,
			 voidfunc release_cb) mutable
		{ 
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
			}

            // 1. Merge Macro Literals
            ResultMuxDemux::mergeMacroLiterals(queries.front().second, resultSets);

			// 2. Merge the rows
            int64_t bufferLength = 0;
			const auto buffer = ResultMuxDemux::multiSetToInternode(
                queries.front().second.vars.columnVars.size(),
                queries.front().second.indexes.size(),
				resultSets,
                bufferLength);

			// reply is responsible for buffer
			message->reply(http::StatusCode::success_ok, buffer, bufferLength);

            PoolMem::getPool().freePtr(buffer);

			Logger::get().info("Fork count(s) on " + table->getName());

            // clean up stray resultSets
            for (auto resultSet : resultSets)
                delete resultSet;

			release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector

		});

	auto instance = 0;
	auto workers = 0;
	// pass factory function (as lambda) to create new cell objects
	partitions->cellFactory(activeList, [shuttle, table, queries, resultSets, &workers, &instance](AsyncLoop* loop) -> OpenLoop*
	{
		++instance;
		++workers;
		return new OpenLoopSegment(shuttle, table, queries, resultSets[loop->getWorkerId()], instance);
	});

	Logger::get().info("Started " + to_string(workers) + " count worker async cells.");
}

void RpcQuery::column(openset::web::MessagePtr message, const RpcMapping& matches)
{
    auto database = openset::globals::database;
    const auto partitions = openset::globals::async;

    const auto tableName = matches.find("table"s)->second;
    const auto columnName = matches.find("name"s)->second;
    const auto isFork = message->getParamBool("fork");

    const auto trimSize = message->getParamInt("trim", -1);
    const auto sortOrder = message->getParamString("order", "desc") == "asc" ? ResultSortOrder_e::Asc : ResultSortOrder_e::Desc;

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

    const auto table = database->getTable(tableName);

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

    if (!column || column->type == columnTypes_e::freeColumn)
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::config,
            openset::errors::errorCode_e::general_config_error,
            "column not found" },
            message);
        return;
    }

   
    // We are a Fork!
    async::OpenLoopColumn::ColumnQueryConfig_s queryInfo;

    queryInfo.columnName = columnName;
    queryInfo.columnType = column->type;
    queryInfo.columnIndex = column->idx;

    if (message->isParam("gt"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::gt;
        queryInfo.filterLow = message->getParamString("gt");
    }
    else if (message->isParam("gte"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::gte;
        queryInfo.filterLow = message->getParamString("gte");
    }
    else if (message->isParam("lt"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::lt;
        queryInfo.filterLow = message->getParamString("lt");
    }
    else if (message->isParam("lte"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::lte;
        queryInfo.filterLow = message->getParamString("lte");
    }
    else if (message->isParam("eq"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::eq;
        queryInfo.filterLow = message->getParamString("eq");
    }
    else if (message->isParam("between"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::between;
        queryInfo.filterLow = message->getParamString("between");
        queryInfo.filterHigh = message->getParamString("and");               
    }
    else if (message->isParam("rx"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::rx;

        // bad regex blows this thing up... so lets catch any errors
        auto isError = false;
        try
        {
            queryInfo.rx = std::regex(message->getParamString("rx"));
        } 
        catch (const std::runtime_error& ex)
        {
            isError = true;
        }
        catch (...)
        {
            isError = true;
        }

        if (isError)
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "could not compile regular express: " + message->getParamString("rx") },
                message);
            return;
        }

    }
    else if (message->isParam("sub"))
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::sub;
        queryInfo.filterLow = message->getParamString("sub");
    }
    else
    {
        queryInfo.mode = OpenLoopColumn::ColumnQueryMode_e::all;
    }

    if (queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::all)
    {
        if (queryInfo.filterLow.getString().length() == 0)
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "column filter requires a value" },
                message);
            return;
        }
    }

    if (queryInfo.mode == OpenLoopColumn::ColumnQueryMode_e::between)
    {
        if (queryInfo.filterHigh.getString().length() == 0)
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "column query using 'between' requires an 'and' param" },
                message);
            return;
        }
    }

    if (message->isParam("bucket"))
        queryInfo.bucket = message->getParamString("bucket");

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts = split(segmentText, ',');

        queryInfo.segments.clear();

        for (auto p : parts)
        {
            p = trim(p);
            if (p.length())
                queryInfo.segments.push_back(p);
        }

        if (!queryInfo.segments.size())
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::syntax_error,
                "no segment names specified" },
                message);
            return;
        }
    }

    // here we are going to force typing depending on the column type
    // note: prior to conversion these will all be strings
    switch (queryInfo.columnType)
    {
        case columnTypes_e::intColumn:
            queryInfo.bucket = queryInfo.bucket.getInt64();
            queryInfo.filterLow = queryInfo.filterLow.getInt64();
            queryInfo.filterHigh = queryInfo.filterHigh.getInt64();
        break;
        case columnTypes_e::doubleColumn: 
            // floating point data in the db is in scaled integers, scale our ranges and buckets
            queryInfo.bucket = static_cast<int64_t>(queryInfo.bucket.getDouble() * 10000.0);
            queryInfo.filterLow = static_cast<int64_t>(queryInfo.filterLow.getDouble() * 10000.0);
            queryInfo.filterHigh = static_cast<int64_t>(queryInfo.filterHigh.getDouble() * 10000.0);
        break;
        case columnTypes_e::boolColumn: 
            queryInfo.filterLow = queryInfo.filterLow.getBool();
        break;
        case columnTypes_e::textColumn: 
            queryInfo.filterLow = queryInfo.filterLow.getString();
        break;
        default: ;
    }

    // now lets make sure the `columnType` and the `mode` make sense together and
    // return an error if they do not.
    if (queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::all &&
        queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::eq)
    {
        switch (queryInfo.columnType)
        {
        case columnTypes_e::intColumn:
        case columnTypes_e::doubleColumn:
            if (queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::between &&
                queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::gt &&
                queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::gte &&
                queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::lt &&
                queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::lte)
            {
                RpcError(
                    openset::errors::Error{
                    openset::errors::errorClass_e::query,
                    openset::errors::errorCode_e::syntax_error,
                    "specified filter type not compatible with integer or double column" },
                    message);
                return;
            }
            break;
        case columnTypes_e::textColumn:
            if (queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::rx &&
                queryInfo.mode != OpenLoopColumn::ColumnQueryMode_e::sub)
            {
                RpcError(
                    openset::errors::Error{
                    openset::errors::errorClass_e::query,
                    openset::errors::errorCode_e::syntax_error,
                    "specified filter type not compatible with string column" },
                    message);
                return;
            }
            break;
        case columnTypes_e::boolColumn:
        default:;
        }
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
        const auto json = std::move(
            forkQuery(
                table, 
                message, 
                1, 
                queryInfo.segments.size(),
                ResultSortMode_e::column,
                sortOrder,
                0,
                trimSize
            )
        );
        if (json) // if null/empty we had an error
            message->reply(http::StatusCode::success_ok, *json);
        return;
    }


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
        resultSets.push_back(new openset::result::ResultSet(1 * (queryInfo.segments.size() ? queryInfo.segments.size() : 1)));

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(
            1,
            queryInfo.segments.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;

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

    const auto shuttle = new ShuttleLambda<CellQueryResult_s>(
        message,
        activeList.size(),
        [table, resultSets, queryInfo]
    (vector<openset::async::response_s<CellQueryResult_s>> &responses,
        openset::web::MessagePtr message,
        voidfunc release_cb) mutable
    { // process the data and respond
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
        }

        // 1. Merge the rows
        int64_t bufferLength = 0;
        auto buffer = ResultMuxDemux::multiSetToInternode(
            1,
            queryInfo.segments.size(),
            resultSets,
            bufferLength);

        message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        Logger::get().info("Fork query on " + table->getName());

        // clean up all those resultSet*
        for (auto r : resultSets)
            delete r;

        release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector
    });

    auto instance = 0;
    // pass factory function (as lambda) to create new cell objects
    partitions->cellFactory(activeList, [shuttle, table, queryInfo, resultSets, &instance](AsyncLoop* loop) -> OpenLoop*
    {
        instance++;
        return new OpenLoopColumn(shuttle, table, queryInfo, resultSets[loop->getWorkerId()], instance);
    });

}

void RpcQuery::person(openset::web::MessagePtr message, const RpcMapping& matches)
{
    
    auto uuString = message->getParamString("sid");
    auto uuid = message->getParamInt("id");

    if (uuid == 0 && uuString.length())
    {
        toLower(uuString);
        uuid = MakeHash(uuString);
    }

    // no UUID... so make an error!
    if (uuid == 0)
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::query,
            openset::errors::errorCode_e::general_error,
            "person query must have an id={number} or idstring={text} parameter" },
            message);
        return;
    }

    const auto tableName = matches.find("table"s)->second;

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

    const auto table = globals::database->getTable(tableName);

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

    const auto partitions = openset::globals::async;
    const auto targetPartition = cast<int32_t>(std::abs(uuid) % partitions->getPartitionMax());
    const auto mapper = globals::mapper->getPartitionMap();

    auto owners = mapper->getNodesByPartitionId(targetPartition);

    int64_t targetRoute = -1;

    // find the owner
    for (auto o : owners)
        if (mapper->isOwner(targetPartition, o))
        {
            targetRoute = o;
            break;
        }

    if (targetRoute == -1)
    {
        RpcError(
            openset::errors::Error{
            openset::errors::errorClass_e::query,
            openset::errors::errorCode_e::route_error,
            "potential node failure - please re-issue the request" },
            message);
        return;
    }

    // this query is local - we will fire up a single async get user task on this node
    if (targetRoute == globals::running->nodeId)
    {
        // lets use the super basic shuttle.
        const auto shuttle = new Shuttle<int>(message);
        auto loop = globals::async->getPartition(targetPartition);

        if (!loop)
        {
            RpcError(
                openset::errors::Error{
                openset::errors::errorClass_e::query,
                openset::errors::errorCode_e::route_error,
                "potential node failure - please re-issue the request" },
                message);
            return;
        }
        
        loop->queueCell(new OpenLoopPerson(shuttle, table, uuid));

    }
    else // remote - we will route to the correct destination node
    {
        const auto response = globals::mapper->dispatchSync(
            targetRoute,
            message->getMethod(),
            message->getPath(),
            message->getQuery(),
            message->getPayload(),
            message->getPayloadLength()
        );
        
        message->reply(response->code, response->data, response->length);
    }    
}

void RpcQuery::histogram(openset::web::MessagePtr message, const RpcMapping& matches)
{
    auto database = openset::globals::database;
    const auto partitions = openset::globals::async;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto groupName = matches.find("name"s)->second;
    const auto queryCode = std::string{ message->getPayload(), message->getPayloadLength() };

    const auto debug = message->getParamBool("debug");
    const auto isFork = message->getParamBool("fork");

    const auto trimSize = message->getParamInt("trim", -1);
    const auto sortOrder = message->getParamString("order", "desc") == "asc" ? ResultSortOrder_e::Asc : ResultSortOrder_e::Desc;

    const auto sortMode = ResultSortMode_e::key;

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
    openset::query::ParamVars paramVars = getInlineVaraibles(message);     
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
                std::string{ ex.what() }
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

    // Histogram querys must call tally
    if (queryMacros.marshalsReferenced.count(query::Marshals_e::marshal_tally))
    {
        RpcError(
            openset::errors::Error{
                openset::errors::errorClass_e::parse,
                openset::errors::errorCode_e::syntax_error,
                "histogram queries should not call 'tally'. They should 'return' the value to store."
            },
            message);
        return;
    }

    if (message->isParam("segments"))
    {
        const auto segmentText = message->getParamString("segments");
        auto parts = split(segmentText, ',');

        queryMacros.segments.clear();

        for (auto& part : parts)
            if (part = trim(part); part.length())
                queryMacros.segments.push_back(part);

        if (!queryMacros.segments.size())
        {
            RpcError(
                openset::errors::Error{
                    openset::errors::errorClass_e::query,
                    openset::errors::errorCode_e::syntax_error,
                    "no segment names specified" },
                    message);
            return;
        }
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

    int64_t bucket = 0;
    if (message->isParam("bucket"))
        bucket = static_cast<int64_t>(stod(message->getParamString("bucket", "0")) * 10000.0);

    auto forceMin = std::numeric_limits<int64_t>::min();
    if (message->isParam("min"))
        forceMin = static_cast<int64_t>(stod(message->getParamString("min", "0")) * 10000.0);

    auto forceMax = std::numeric_limits<int64_t>::min();
    if (message->isParam("max"))
        forceMax = static_cast<int64_t>(stod(message->getParamString("max", "0")) * 10000.0);

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
        const auto json = std::move(
            forkQuery(
                table,
                message,
                1,
                queryMacros.segments.size(),
                sortMode,
                sortOrder,
                0,
                trimSize,
                bucket,
                forceMin,
                forceMax
            )
        );

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
        resultSets.push_back(
            new openset::result::ResultSet(
                queryMacros.vars.columnVars.size() * (queryMacros.segments.size() ? queryMacros.segments.size() : 1)
            )
        );

    // nothing active - return an empty set - not an error
    if (!activeList.size())
    {
        // 1. Merge Macro Literals
        ResultMuxDemux::mergeMacroLiterals(queryMacros, resultSets);

        // 2. Merge the rows
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(
            queryMacros.vars.columnVars.size(),
            queryMacros.indexes.size(),
            resultSets,
            bufferLength);

        // reply will be responsible for buffer
        message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        PoolMem::getPool().freePtr(buffer);

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;

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
        voidfunc release_cb) mutable
    { // process the data and respond
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
        }
                
        int64_t bufferLength = 0;
        const auto buffer = ResultMuxDemux::multiSetToInternode(
            1,
            queryMacros.indexes.size(),
            resultSets,
            bufferLength);

        message->reply(http::StatusCode::success_ok, buffer, bufferLength);

        Logger::get().info("Fork query on " + table->getName());

        // clean up stray resultSets
        for (auto resultSet : resultSets)
            delete resultSet;

        PoolMem::getPool().freePtr(buffer);

        release_cb(); // this will delete the shuttle, and clear up the CellQueryResult_s vector
    });


    auto forEach = message->isParam("foreach") ? message->getParamString("foreach") : ""s;
    auto instance = 0;
    // pass factory function (as lambda) to create new cell objects

    partitions->cellFactory(activeList, [shuttle, table, queryMacros, resultSets, groupName, bucket, forEach, &instance](AsyncLoop* loop) -> OpenLoop*
    {
        instance++;
        return new OpenLoopHistogram(
            shuttle, 
            table, 
            queryMacros, 
            groupName, 
            forEach,
            bucket, 
            resultSets[loop->getWorkerId()], 
            instance);
    });
}

openset::mapping::Mapper::Responses queryDispatch(std::string tableName, openset::query::SegmentList segments, openset::query::QueryParser::SectionDefinitionList queries)
{
    const auto runMax = 4; // maximum concurrent queries allowed.

    CriticalSection cs;
    auto iter = queries.begin();
    auto doneSending = false;

    auto receivedCount = 0;
    auto sendCount = 0; // number of queries sent
    auto running = 0; // number currently running

    std::function<void()> sendOne;

    openset::mapping::Mapper::Responses result;

    auto completeCallback = [&](const openset::http::StatusCode status, const bool, char* data, const size_t size)
    {
        {
            csLock lock(cs);
            auto dataCopy = static_cast<char*>(PoolMem::getPool().getPtr(size));
            memcpy(dataCopy, data, size);
            result.responses.emplace_back(openset::mapping::Mapper::DataBlock{ dataCopy, size, status });
            --running;
            ++receivedCount;
        }

        sendOne();
    };
    
    sendOne = [&]()
    {
        std::string method = "GET";
        std::string path;
        openset::web::QueryParams params;
        std::string payload;

        {
            csLock lock(cs);

            if (running > runMax) // send up to RunMax, fill any that are complete
                return;

            if (iter == queries.end())
            {
                doneSending = true;
                return;
            }

            ++running;
            ++sendCount;

            // convert captures in Section Defintion to REST params
            for (auto p : *(iter->params.getDict()))
                if (p.first.getString() != "each") // missing a char* != ???
                    params.emplace(p.first.getString(), p.second.getString());

            // add a segments param
            if (segments.size())
                params.emplace("segments"s, join(segments));

            // make queries
            if (iter->sectionType == "segment")
            {
                method = "POST";
                path = "/v1/query/" + tableName + "/segment";
                std::string segline = "@segment " + iter->sectionName + " ";
                for (auto f : *(iter->flags.getDict()))
                    segline += f.first.getString() + "=" + f.second.getString() + " ";
                segline += "\n";

                payload += segline + std::move(iter->code); // eat it
            }
            else if (iter->sectionType == "column")
            {
                path = "/v1/query/" + tableName + "/column/" + iter->sectionName;
                payload = std::move(iter->code); // eat it
            }
            else if (iter->sectionType == "histogram")
            {
                method = "POST";
                path = "/v1/query/" + tableName + "/histogram/" + iter->sectionName;
                payload = std::move(iter->code); // eat it
            }

            ++iter;
        }

        // fire these queries off
        auto success = openset::globals::mapper->dispatchAsync(
            openset::globals::running->nodeId, // fork to self
            method,
            path,
            params,
            payload,
            completeCallback
        );

        if (!success)
            result.routeError = true;

        sendOne();
    };

    sendOne();

    while (!doneSending && sendCount != receivedCount)
    {
        ThreadSleep(50); // replace with semaphore
    }

    return std::move(result);
}

void RpcQuery::batch(openset::web::MessagePtr message, const RpcMapping& matches)
{
    auto database = openset::globals::database;
    const auto partitions = openset::globals::async;

    const auto request = message->getJSON();
    const auto tableName = matches.find("table"s)->second;
    const auto queryCode = std::string{ message->getPayload(), message->getPayloadLength() };

    const auto debug = message->getParamBool("debug");

    const auto startTime = Now();

    Logger::get().info("Inbound multi query"s);

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

    const auto table = database->getTable(tableName);

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
    
    thread runner([=]() {

        openset::query::ParamVars paramVars = getInlineVaraibles(message);
        // get the functions extracted and de-indented as named code blocks
        auto subQueries = openset::query::QueryParser::extractSections(queryCode.c_str());


        query::QueryParser::SectionDefinitionList segmentList;
        query::QueryParser::SectionDefinitionList queryList;
        query::QueryParser::SectionDefinition_s useSection;
        query::SegmentList segments;

        // extract the 
        for (auto &s : subQueries)
            if (s.sectionType == "segment")
                segmentList.push_back(s);
            else if (s.sectionType == "use")
                useSection = s;
            else
                queryList.push_back(s);

        if (useSection.sectionType == "use" && useSection.sectionName.length())
            segments = split(useSection.sectionName, ',');

        if (segmentList.size())
        {
            auto results = queryDispatch(tableName, segments, segmentList);

            for (auto &r : results.responses)
            {
                if (r.code != http::StatusCode::success_ok)
                {
                    // try to capture a json error that has perculated up from the forked call.
                    if (r.data &&
                        r.length &&
                        r.data[0] == '{')
                    {
                        cjson error(std::string(r.data, r.length), r.length);

                        if (error.xPath("/error"))
                        {
                            message->reply(openset::http::StatusCode::client_error_bad_request, error);
                            return;
                        }
                        else
                            results.routeError = true;
                    }
                    else
                        results.routeError = true; // this will trigger the next error
                }
            }

            if (results.routeError)
            {
                RpcError(
                    openset::errors::Error{
                        openset::errors::errorClass_e::config,
                        openset::errors::errorCode_e::route_error,
                        "potential node failure - please re-issue the request" },
                        message);
                return;
            }            
        }
        
        if (queryList.size())
        {
            auto results = queryDispatch(tableName, segments, queryList);
            
            for (auto &r : results.responses)
            {
                if (r.code != http::StatusCode::success_ok)
                {
                    // try to capture a json error that has perculated up from the forked call.
                    if (r.data &&
                        r.length &&
                        r.data[0] == '{')
                    {
                        cjson error(std::string(r.data, r.length), r.length);

                        if (error.xPath("/error"))
                        {
                            message->reply(openset::http::StatusCode::client_error_bad_request, error);
                            return;
                        }
                        else
                            results.routeError = true;
                    }
                    else
                        results.routeError = true; // this will trigger the next error
                }
            }

            if (results.routeError)
            {
                RpcError(
                    openset::errors::Error{
                        openset::errors::errorClass_e::config,
                        openset::errors::errorCode_e::route_error,
                        "potential node failure - please re-issue the request" },
                        message);
                return;
            }

            cjson responseJson;
            auto resultBranch = responseJson.setArray("_");

            for (auto &r : results.responses)
            {
                auto insertAt = resultBranch->pushObject();
                cjson resultItemJson{ std::string{ r.data, r.length }, r.length };
                               
                if (auto item = resultItemJson.xPath("/_/0"); item)
                    cjson::Parse(cjson::Stringify(item), insertAt, true);
            }

            message->reply(openset::http::StatusCode::success_ok, responseJson);
        }
    });

    runner.detach();
}

void openset::comms::Dispatch(const openset::web::MessagePtr message)
{
	const auto path = message->getPath();

	for (auto& item: MatchList)
	{		
        // the most beautifullest thing...
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
