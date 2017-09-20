#pragma once
#include "common.h"

namespace openset
{
	namespace mapping
	{
		class Mapper;
		using MessageID = std::pair<int64_t, int64_t>;

		enum class rpc_e : int32_t {
			none = 0,
			inter_node = 1,
			inter_node_partition_xfer = 2,
			inter_node_healthcheck = 3,
			admin = 1000,
			insert_sync = 10'000,
			insert_async = 10'001,
			query_pyql = 20'000,
			message_sub = 30'000
		};

	}

	namespace comms
	{
		// forward
		
		class InboundConnection;
	};
};


namespace std { // hasher for RPC mappings
	template<>
	struct hash<openset::mapping::rpc_e> {
		size_t operator()(const openset::mapping::rpc_e &v) const {
			return cast<size_t>(v);
		}
	};
}

namespace openset
{
	namespace comms
	{
		

#pragma pack(push,1)
		struct RouteHeader_s
		{
			int64_t route{0}; // ID of destination or 0 (zero) for client
			int64_t replyTo{0}; // ID of sender or 0 (zero) for client origin
			int64_t slot{0}; // 0 (zero) for client connection
			int32_t rpc{0}; // -1 is error state, otherwise RPC handler
			int32_t length{0}; // length of payload

			RouteHeader_s() = default;

			explicit RouteHeader_s(bool error) 
			{
				rpc = -1; // wish you could put this in a C++11 initializer list... not yet.
			}

			bool isError() const
			{
				return (rpc == -1);
			}
		};
#pragma pack(pop)		
	};
};
