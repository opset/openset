#pragma once

#include <atomic>
#include "common.h"
#include "internodecommon.h"
#include "internodeoutbound.h"
#include "internodemapping.h"
#include "threads/locks.h"

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

			using RouteNames = unordered_map<int64_t, string>;
			using Routes = unordered_map<int64_t, OutboundClient*>;
			using Messages = unordered_map<MessageID, openset::comms::Message*>;

			using DataBlock = std::pair<char*, int64_t>;

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
			Messages messages;

			// we increment every time we make a mailbox - use atomics as they are thread safe
			atomic<int64_t> slotCounter;

			Mapper();

			~Mapper();

			int64_t getSlotNumber();

			void removeRoute(int64_t routeId);
			void addRoute(std::string routeName, int64_t routeId, std::string ip, int32_t port);
			std::string getRouteName(int64_t routeId);

			OutboundClient* getRoute(int64_t routeId);

			// dispatchAsync - send a payload down a route.
			openset::comms::Message* dispatchAsync(
				int64_t route, 
				rpc_e rpc, 
				char* data, 
				int64_t length, 
				openset::comms::ReadyCB callback);		

			openset::comms::Message* dispatchAsync(
				int64_t route, 
				rpc_e rpc, 
				const char* data, 
				int64_t length, 
				openset::comms::ReadyCB callback);

			openset::comms::Message* dispatchAsync(
				int64_t route,
				rpc_e rpc,
				const cjson* doc,
				openset::comms::ReadyCB callback);

			openset::comms::Message* dispatchSync(
				int64_t route,
				rpc_e rpc,
				char* data,
				int64_t length);

			openset::comms::Message* dispatchSync(
				int64_t route,
				rpc_e rpc,
				const char* data,
				int64_t length);

			openset::comms::Message* dispatchSync(
				int64_t route,
				rpc_e rpc,
				const cjson* doc);

			openset::comms::Message* getMessage(MessageID messageId);

			void dereferenceMessage(MessageID messageId);
			void disposeMessage(MessageID messageId);

			openset::mapping::PartitionMap* getPartitionMap()
			{
				return &partitionMap;
			}

			// send a message to all known routes
			Responses dispatchCluster(
				rpc_e rpc, 
				const char* data, 
				int64_t length,
				bool internalDispatch = false);

			Responses dispatchCluster(
				rpc_e rpc,
				cjson* doc,
				bool internalDispatch = false);

			// helper to clean up responses from dispatchCluster
			static void releaseResponses(Responses &responseSet);

			// get the node ID that is the teamster
			int64_t getSentinelId() const;
			int countFailedRoutes() const;
			int countActiveRoutes() const;
			int countRoutes() const;
			std::vector<int64_t> getActiveRoutes() const;
			std::vector<int64_t> getFailedRoutes() const;

			std::vector<std::pair<int64_t, int>> getPartitionCountsByRoute(std::unordered_set<NodeState_e> states) const;
			
			// replaces existing mapping, cleaning up orphaned
			// partitions and creating new ones when needed
			void changeMapping(
				cjson* config, 
				std::function<void(int)> addPartition_cb, 
				std::function<void(int)> deletePartition_cb, 
				std::function<void(string, int64_t, string, int)> addRoute_cb,
				std::function<void(int64_t)> deleteRoute_cb);

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


