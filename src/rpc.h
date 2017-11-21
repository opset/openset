#pragma once

#include "shuttle.h"
#include "asyncpool.h"
#include "database.h"
#include "http_serve.h"

/*
	Control - This class is responsible for mapping
		communications from the usvserver to jobs (Cells)
		within the DB.

	This is basically the big sandwich between three different
	parts of the database:

	1. Comms (OpenSet::server)
	2. Multitasking (OpenSet::async)
	3. DB Engine (OpenSet::db)
    4. Triggers (OpenSet::trigger)

	These classes ensure that when comms happen, cells are created
	in the multitasking engine for the correct partitions and that
	DB engine objects are mapped to these cells, and that partition
	data is created(*). 

	Most of this work is done with cell factories and shuttle objects. 
	Cells are assigned to loops which are assigned to partitions to provide
	thread separation. Shuttles are used to gather results, and relay
	responses back down the originating connection (comms).

	* The database mirrors the concept of partitions with the multi-tasking
	engine. Partition X in one is also Partition X in another when it comes
	to thread isolation. However, functionally they are completely separate.

*/

using namespace std;
using namespace openset::async;
using namespace openset::db;

namespace openset::comms
{
	using RpcMapping = std::unordered_map<std::string, std::string>;
	using RpcHandler = std::function<void(const openset::web::MessagePtr, const RpcMapping&)>;
	// method, regex, handler function, regex capture index to RpcMapping container
	using RpcMapTuple = std::tuple<const std::string, const std::regex, const RpcHandler, const std::vector<std::pair<int, string>>>;
	
	class RpcInternode
	{
	public:
		// POST /v1/internode/join_to_cluster
		static void join_to_cluster(const openset::web::MessagePtr message, const RpcMapping& matches);
		// GET /v1/cluster/is_member
		static void is_member(const openset::web::MessagePtr message, const RpcMapping& matches);
		// GET /v1/cluster/is_member
		static void add_node(const openset::web::MessagePtr message, const RpcMapping& matches);
		// POST /v1/internode/map_change
		static void map_change(const openset::web::MessagePtr message, const RpcMapping& matches);
		// PUT /v1/internode/transfer?partition={partition_id}&node={node_name}
		static void transfer_init(const openset::web::MessagePtr message, const RpcMapping& matches);
		// POST /v1/internode/transfer?partition={partition_id}&table={table_name}
		static void transfer_receive(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	class RpcCluster
	{
	public:
		// PUT /v1/cluster/init?partitions={#}
		static void init(const openset::web::MessagePtr message, const RpcMapping& matches);
		// PUT /v1/cluster/join?host={host|ip}&port={port}
		static void join(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	class RpcTable
	{
	public:
		// POST /v1/table/{table}
		static void table_create(const openset::web::MessagePtr message, const RpcMapping& matches);
		// GET /v1/table/{table}
		static void table_describe(const openset::web::MessagePtr message, const RpcMapping& matches);
		// PUT /v1/table/{table}/column/{name}:{type}
		static void column_add(const openset::web::MessagePtr message, const RpcMapping& matches);
		// DELETE /v1/table/{table}/column/{name}
		static void column_drop(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	class RpcRevent
	{
	public:
		// POST /v1/table/{table}/revent/{revent_name}
		static void revent_create(const openset::web::MessagePtr message, const RpcMapping& matches);
		// GET /v1/table/{table}/revent/{revent_name}
		static void revent_describe(const openset::web::MessagePtr message, const RpcMapping& matches);
		// DELETE /v1/table/{table}/revent/{revent_name}
		static void revent_drop(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	class RpcQuery
	{
	public:

		// POST /v1/query/{table}/events
		static void events(const openset::web::MessagePtr message, const RpcMapping& matches);
		// POST /v1/query/{table}/counts
		static void counts(const openset::web::MessagePtr message, const RpcMapping& matches);
		
		//static void person(const openset::web::MessagePtr message, const RpcMapping& matches);		
	};

	class RpcInsert
	{
	public:
		static void insert(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	class Feed
	{
	public:
		static void onSub(const openset::web::MessagePtr message, const RpcMapping& matches);
	};

	// order matters, longer matches in a section should appear first
	static const std::vector <RpcMapTuple> MatchList = {
		// RpcCluster
		{ "PUT", std::regex(R"(^/v1/cluster/init$)"), RpcCluster::init,{} },
		{ "PUT", std::regex(R"(^/v1/cluster/join$)"), RpcCluster::join,{} },

		// RpcTable
		{ "PUT", std::regex(R"(^/v1/table/([a-z0-9_]+)/column/([a-z0-9_\.]+):([a-z]+)(\/|\?|\#|)$)"), RpcTable::column_add,{ { 1, "table" }, { 2, "name" }, { 3, "type" } } },
		{ "DELETE", std::regex(R"(^/v1/table/([a-z0-9_]+)/column/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcTable::column_drop,{ { 1, "table" }, { 2, "name" } } },
		{ "GET", std::regex(R"(^/v1/table/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcTable::table_describe, { { 1, "table" } } },
		{ "POST", std::regex(R"(^/v1/table/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcTable::table_create, { { 1, "table" } } },

		// RpcQuery
		{ "POST", std::regex(R"(^/v1/query/([a-z0-9_]+)/events(\/|\?|\#|)$)"), RpcQuery::events,{ { 1, "table" } } },

		// RpcInsert
		{ "POST", std::regex(R"(^/v1/insert/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcInsert::insert, { { 1, "table" } } },

		// RpcRevent
		{ "GET", std::regex(R"(^/v1/table/([a-z0-9_]+)/revent/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_describe,{ { 1, "table" },{ 2, "name" } } },
		{ "POST", std::regex(R"(^/v1/table/([a-z0-9_]+)/revent/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_create,{ { 1, "table" },{ 2, "name" } } },
		{ "DELETE", std::regex(R"(^/v1/table/([a-z0-9_]+)/revent/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_drop,{ { 1, "table" },{ 2, "name" } } },

		// RpcInternode
		{ "GET", std::regex(R"(^/v1/internode/is_member$)"), RpcInternode::is_member, {} },
		{ "POST", std::regex(R"(^/v1/internode/join_to_cluster$)"), RpcInternode::join_to_cluster, {} },
		{ "POST", std::regex(R"(^/v1/internode/add_node$)"), RpcInternode::add_node, {} },
		{ "PUT", std::regex(R"(^/v1/internode/transfer)"), RpcInternode::transfer_init, {} },
		{ "POST", std::regex(R"(^/v1/internode/transfer)"), RpcInternode::transfer_receive, {} },
		{ "POST", std::regex(R"(^/v1/internode/map_change$)"), RpcInternode::map_change, {} },
	};

	void Dispatch(openset::web::MessagePtr message);
};
