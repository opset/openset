#pragma once

#include <atomic>
#include "common.h"
#include "internodemapping.h"
#include "http_serve.h"
#include "http_cli.h"
#include "threads/spinlock.h"

namespace openset
{

	namespace db
	{
		class Database;
	}

	namespace comms
	{
		class Message;
	}

	namespace mapping
	{
		class Mapper
		{
		public:

			// map route to node name
			using RouteNames = unordered_map<int64_t, string>;
			// map route to HTTP string
			using Routes = unordered_map<int64_t, std::pair<std::string, int>>;

			struct DataBlock
			{
                char* data{ nullptr };
                size_t length{ 0 };
                http::StatusCode code { http::StatusCode::success_ok };

                DataBlock(char* data, const size_t length, const http::StatusCode code):
                    data(data),
                    length(length),
                    code(code)
                {}

                ~DataBlock()
                {
                    if (data)
                    {
                        PoolMem::getPool().freePtr(data);
                        data = nullptr;
                    }
                }
			};

			using DataBlockPtr = shared_ptr<DataBlock>;

			struct Responses_s
			{
				bool routeError;
				vector<DataBlock> responses;
				Responses_s() :
					routeError(false)
				{}
			};

			using Responses = Responses_s;

			mutable CriticalSection cs;

			PartitionMap partitionMap;
			Routes routes;
			RouteNames names;

			// we increment every time we make a mailbox - use atomics as they are thread safe
			atomic<int64_t> slotCounter;

			Mapper();

			~Mapper();

			int64_t getSlotNumber();

			void addRoute(const std::string routeName, const int64_t routeId, const std::string ip, const int32_t port);
			void removeRoute(const int64_t routeId);

			std::string getRouteName(const int64_t routeId);
			int64_t getRouteId(const std::string routeName);

			// factories a Rest object
			openset::web::RestPtr getRoute(const int64_t routeId);

			bool isRoute(const int64_t routeId);
			bool isRouteNoLock(const int64_t routeId);

			// dispatchAsync - send a payload down a route.
			bool dispatchAsync(
				const int64_t route, 
				const std::string method,
				const std::string path, 
				const openset::web::QueryParams params,
				const char* payload, 
				const size_t length, 
				const openset::web::RestCbBin callback);

			bool dispatchAsync(
				const int64_t route,
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				const std::string& payload,
				const openset::web::RestCbBin callback);

			bool dispatchAsync(
				const int64_t route,
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				cjson& payload,
				const openset::web::RestCbBin callback);

			DataBlockPtr dispatchSync(
				const int64_t route,
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				const char* payload,
				const size_t length);

			DataBlockPtr dispatchSync(
				const int64_t route,
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				cjson& payload);


			openset::mapping::PartitionMap* getPartitionMap()
			{
				return &partitionMap;
			}

			// send a message to all known routes
			Responses dispatchCluster(
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				const char* data, 
				const size_t length,
				const bool internalDispatch = false);

			Responses dispatchCluster(
				const std::string method,
				const std::string path,
				const openset::web::QueryParams params,
				cjson& json,
				const bool internalDispatch = false);

			// helper to clean up responses from dispatchCluster
			static void releaseResponses(Responses &responseSet);

			// get the node ID that is the teamster
			int64_t getSentinelId() const;
			int countFailedRoutes();
			int countActiveRoutes();
			int countRoutes() const;
			std::vector<int64_t> getActiveRoutes();
			std::vector<int64_t> getFailedRoutes();

			std::vector<std::pair<int64_t, int>> getPartitionCountsByRoute(std::unordered_set<NodeState_e> states);
			
			// replaces existing mapping, cleaning up orphaned
			// partitions and creating new ones when needed
			void changeMapping(
				const cjson& config, 
				const std::function<void(int)> addPartition_cb, 
				const std::function<void(int)> deletePartition_cb, 
				const std::function<void(string, int64_t, string, int)> addRoute_cb,
				const std::function<void(int64_t)> deleteRoute_cb);

			void loadPartitions();
			void savePartitions();

			void serializeRoutes(cjson* doc);
			void deserializeRoutes(cjson* doc);

			void loadRoutes();
			void saveRoutes();

			void run() const;
			void startRouter();


		};

	};

	namespace globals
	{
		extern openset::mapping::Mapper* mapper;
	}
};


