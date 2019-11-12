#include "common.h"
#include "service.h"

#include "config.h"
#include "logger.h"
#include "asyncpool.h"
#include "internodecommon.h"
#include "internoderouter.h"
#include "rpc.h"

#include "sentinel.h"

#include "http_serve.h"


#include <thread>

namespace openset
{
	bool Service::start()
	{
		const auto ip = globals::running->host;
		const auto port = globals::running->port;
		const auto pool = std::thread::hardware_concurrency(); // set to number of cores

		const auto partitionTotal = globals::running->partitionMax;

#ifndef _MSC_VER
		signal(SIGPIPE, SIG_IGN);
#endif

		// generate our async workers, we are going to use one worker per core
		openset::async::AsyncPool async(partitionTotal, std::thread::hardware_concurrency());
		// DEBUG OpenSet::async::AsyncPool async(partitionTotal, 1);

		openset::mapping::Mapper mapper;
		mapper.startRouter();

		openset::db::Database db;

		// aysnc.run will create our thread pool o
		//
		// This thread will create a pool of thread (in 'run') that
		// where each thread is responsible for a list of partitions.
		//
		// Note: On an empty cluster this will exit out right away, which is fine.

		async.startAsync();

		// async loop will not be running if this node is not part of a cluster
		if (async.isRunning())
			async.mapPartitionsToAsyncWorkers();

		openset::mapping::Sentinel teamster(&mapper, &db);

		web::HttpServe httpd;
		httpd.serve(ip, port); // this function will never return

		/*
		// inter-node
		server.handler(openset::mapping::rpc_e::inter_node, [&db, &async]
		(comms::Message* message)
		{
			// evaluate message and start any threads, cells, processes, etc.
			openset::comms::RpcInternode::onMessage(&db, &async, message);
		});

		// inter_node xfer - this is a binary protocol
		server.handler(openset::mapping::rpc_e::inter_node_partition_xfer, [&db, &async]
		(comms::Message* message)
		{
			// evaluate message and start any threads, cells, processes, etc.
			openset::comms::InternodeXfer::onXfer(&db, &async, message);
		});

		// admin sub-service channel
		server.handler(openset::mapping::rpc_e::admin, [&db, &async]
			(comms::Message* message)
			{
				// evaluate message and start any threads, cells, processes, etc.
				openset::comms::RpcTable::onMessage(&db, &async, message);
			});

		// insert sub-service channel
		server.handler(openset::mapping::rpc_e::insert_async, [&db, &async]
			(comms::Message* message)
			{
				// create some insert cells
				//openset::comms::RpcInsert::onInsert(&db, &async, message);
			});

		// insert sub-service channel
		server.handler(openset::mapping::rpc_e::query_pyql, [&db, &async]
			(comms::Message* message)
			{
				// create query cells
				openset::comms::RpcQuery::onMessage(&db, &async, message);
			});

		// insert sub-service channel
		server.handler(openset::mapping::rpc_e::message_sub, [&db, &async]
			(comms::Message* message)
			{
				// create query cells
				openset::comms::Feed::onSub(&db, &async, message);
			});

		// Start the TCP server.
		// BTW - this function will never return...
		server.serve(IP, port, pool);

		Logger::get().error("could not start server");
		*/

		return true;
	}

	bool Service::stop()
	{
		/*_SHUTDOWNREQUEST = true;

		while (!_SHUTDOWNCOMPLETE)
		{
			Logger::get().info('@', "Waiting");
			this_thread::sleep_for(chrono::milliseconds(500));
		}*/

		return true;
	}

	bool Service::shutdown()
	{
		/*_SHUTDOWNREQUEST = true;

		Logger::get().info('@', "Stopping Salt");
		while (!_SHUTDOWNCOMPLETE)
		{
			Logger::get().info('@', "Waiting");
			this_thread::sleep_for(chrono::milliseconds(500));
		}*/

		return true;
	}
};
