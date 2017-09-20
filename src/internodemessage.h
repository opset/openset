#pragma once

#include "internodecommon.h"
#include "threads/locks.h"
#include "sba/sba.h"
#include "heapstack/heapstack.h"
#include "cjson/cjson.h"
#include <atomic>

namespace openset
{
	namespace comms
	{

		enum class SlotType_e : int
		{
			none,
			local_origin,
			remote_origin,
		};

		class Message;

		using ReadyCB = std::function<void(openset::comms::Message*)>;

		extern atomic<int64_t> msgsCreated;
		extern atomic<int64_t> msgsDestroyed;
		
		class Message
		{
		public:
			SlotType_e mode;

			openset::mapping::MessageID routingId; // mailbox id
			int64_t replyRoute;
			openset::mapping::rpc_e rpc;
			char* data;
			int64_t length;

			int64_t stamp;

			ReadyCB ready_cb;
			openset::mapping::Mapper* mailbox;
			InboundConnection* clientConnection;

			explicit Message();

			// dispatchAsync constructor - creates, registers and queues message
			Message(
				int64_t route,
				openset::mapping::rpc_e rpc,
				char* data,
				int64_t length,
				ReadyCB ready_cb);

			// inbound constructor - construct message based on uvConnection
			Message(
				openset::mapping::Mapper* mailbox,
				InboundConnection* connection);

			~Message();

			char* newBuffer(int32_t length)
			{
				if (data)
					delete[]data;

				data = recast<char*>(PoolMem::getPool().getPtr(length));
				this->length = length;

				return data;
			}
			
			// frees buffers, sets zero length, does not alter routes
			void clear();
			void dispose() const;

			openset::mapping::rpc_e getRPC() const
			{
				return cast<openset::mapping::rpc_e>(rpc);
			}

			// called when a reply is received
			void onResponse(char* data, int64_t length);
			void onResponse(const char* data, int64_t length);

			void onMessage(int64_t route, int64_t replyRoute, int64_t slot, openset::mapping::rpc_e rpc, char* data, int64_t length);

			char* transferPayload(int64_t &length)
			{
				length = this->length;
				auto resultData = data;

				data = nullptr;
				this->length = 0;

				return resultData;
			}

			// reply to a node to a remote_origin
			void reply(char* data, int64_t length);

			void reply(std::string message)
			{
				auto tempLength = message.length();
				auto tempData = recast<char*>(PoolMem::getPool().getPtr(tempLength));
				memcpy(tempData, message.c_str(), tempLength);
				reply(tempData, tempLength);
			}

			void reply(cjson* doc);

			// send a message to a destination (generates an ID for this message)
			void dispatch(int64_t route, openset::mapping::rpc_e rpc, char* data, int64_t length, ReadyCB callback);
			void dispatch(int64_t route, openset::mapping::rpc_e rpc, const char* data, int64_t length, ReadyCB callback);

			std::string toString() const
			{
				return std::string(data, length);
			}
		};
	};
};