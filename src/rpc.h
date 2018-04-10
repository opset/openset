#pragma once

#include <regex>
#include "shuttle.h"
#include "database.h"
#include "http_serve.h"
#include "rpc_global.h"
#include "rpc_internode.h"
#include "rpc_cluster.h"
#include "rpc_table.h"
#include "rpc_revent.h"
#include "rpc_query.h"
#include "rpc_insert.h"
#include "rpc_status.h"

using namespace openset::async;
using namespace openset::db;

/*
    All RPC maps to the giant tuple vector below.
*/

namespace openset::comms
{
	// order matters, longer matches in a section should appear first
	static const std::vector <RpcMapTuple> MatchList = {
		// RpcCluster
		{ "PUT", std::regex(R"(^/v1/cluster/init$)"), RpcCluster::init,{} },
		{ "PUT", std::regex(R"(^/v1/cluster/join$)"), RpcCluster::join,{} },

		// RpcTable
		{ "PUT", std::regex(R"(^/v1/table/([a-z0-9_]+)/column/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcTable::column_add,{ { 1, "table" }, { 2, "name" } } },
        { "DELETE", std::regex(R"(^/v1/table/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcTable::table_drop, { { 1, "table" } } },
		{ "DELETE", std::regex(R"(^/v1/table/([a-z0-9_]+)/column/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcTable::column_drop,{ { 1, "table" }, { 2, "name" } } },
		{ "GET", std::regex(R"(^/v1/table/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcTable::table_describe, { { 1, "table" } } },
		{ "POST", std::regex(R"(^/v1/table/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcTable::table_create, { { 1, "table" } } },
        { "PUT", std::regex(R"(^/v1/table/([a-z0-9_]+)/settings(\/|\?|\#|)$)"), RpcTable::table_settings, { { 1, "table" } } },

		// RpcQuery
		{ "POST", std::regex(R"(^/v1/query/([a-z0-9_]+)/event(\/|\?|\#|)$)"), RpcQuery::event,{ { 1, "table" } } },
        { "POST", std::regex(R"(^/v1/query/([a-z0-9_]+)/segment(\/|\?|\#|)$)"), RpcQuery::segment,{ { 1, "table" } } },
        { "GET", std::regex(R"(^/v1/query/([a-z0-9_]+)/person(\/|\?|\#|)$)"), RpcQuery::person,{ { 1, "table" } } },
        { "GET", std::regex(R"(^/v1/query/([a-z0-9_]+)/column/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcQuery::column,{ { 1, "table" }, { 2, "name" } } },
        { "POST", std::regex(R"(^/v1/query/([a-z0-9_]+)/histogram/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcQuery::histogram, { { 1, "table" },{ 2, "name" } } },
        { "POST", std::regex(R"(^/v1/query/([a-z0-9_]+)/batch(\/|\?|\#|)$)"), RpcQuery::batch,{ { 1, "table" } } },

		// RpcInsert
		{ "POST", std::regex(R"(^/v1/insert/([a-z0-9_]+)(\/|\?|\#|)$)"), RpcInsert::insert, { { 1, "table" } } },

		// RpcRevent
        { "POST", std::regex(R"(^/v1/revent/([a-z0-9_]+)/trigger/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_create,{ { 1, "table" },{ 2, "name" } } },
		{ "GET", std::regex(R"(^/v1/revent/([a-z0-9_]+)/trigger/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_describe,{ { 1, "table" },{ 2, "name" } } },
		{ "DELETE", std::regex(R"(^/v1/revent/([a-z0-9_]+)/trigger/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_drop,{ { 1, "table" },{ 2, "name" } } },
        { "PUT", std::regex(R"(^/v1/revent/([a-z0-9_]+)/trigger/([a-z0-9_\.]+)/sub/([a-z0-9_\.]+)(\/|\?|\#|)$)"), RpcRevent::revent_sub,{ { 1, "table" },{ 2, "name" },{ 3, "sub" } } },

		// RpcInternode
		{ "GET", std::regex(R"(^/v1/internode/is_member$)"), RpcInternode::is_member, {} },
		{ "POST", std::regex(R"(^/v1/internode/join_to_cluster$)"), RpcInternode::join_to_cluster, {} },
		{ "POST", std::regex(R"(^/v1/internode/add_node$)"), RpcInternode::add_node, {} },
		{ "PUT", std::regex(R"(^/v1/internode/transfer$)"), RpcInternode::transfer_init, {} },
		{ "POST", std::regex(R"(^/v1/internode/transfer$)"), RpcInternode::transfer_receive, {} },
		{ "POST", std::regex(R"(^/v1/internode/map_change$)"), RpcInternode::map_change, {} },
        { "POST", std::regex(R"(^/v1/internode/translog$)"), RpcInternode::transfer_translog, {} },

        // Status
        { "GET", std::regex(R"(^/v1/status(\/|\?|\#|)$)"), RpcStatus::status, {} }
	};

	void Dispatch(openset::web::MessagePtr message);
};
