#pragma once

#include <unordered_set>

#include "common.h"
#include "mem/bigring.h"
#include "file/file.h"
#include "file/directory.h"
#include "config.h"

namespace openset
{
	namespace mapping
	{
		// status >= 'routable', means data will be routed to that node
		enum class NodeState_e : int
		{
			free = 0, // un-allocated routing record
			failed = 1, // failed node/instance

			routable = 2, // marker for comparing
			active_owner = 3, // active and this partitions owner
			active_clone = 4, // active and a clone for this partition
			active_placeholder = 5, // active and in a build state
		};
	}
}

namespace std
{
	template<>
	struct hash<openset::mapping::NodeState_e>
	{
		size_t operator()(const openset::mapping::NodeState_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};
}

namespace openset
{
	namespace mapping
	{

		class PartitionMap
		{
		public:

			static const int MAPDEPTH = 6;

			struct location_s
			{
				mutable CriticalSection cs;

				struct node_s
				{
					NodeState_e state;
					int64_t nodeId;
				};

				node_s nodes[MAPDEPTH];

				location_s()
				{
					clear();
				};

				void clear()
				{
					memset(nodes, 0, sizeof(nodes));
				}

				node_s* getMapPtr()
				{
					return nodes;
				}

				bool addMapping(int64_t nodeId, NodeState_e state)
				{
					csLock lock(cs);

					for (auto &n : nodes)
						if (n.state == NodeState_e::free)
						{
							n.nodeId = nodeId;
							n.state = state;
							return true;
						}

					return false;
				}

				bool removeMapping(int64_t nodeId, NodeState_e state)
				{
					csLock lock(cs);

					for (auto &n : nodes)
						if (n.nodeId == nodeId && n.state == state)
						{
							n.state = NodeState_e::free;
							n.nodeId = 0;
							return true;
						}

					return false;
				}

				node_s* isMapped(int64_t nodeId)
				{
					csLock lock(cs);

					for (auto &n : nodes)
					{
						if (n.state != NodeState_e::free &&
							n.nodeId == nodeId)
							return &n;
					}

					return nullptr;
				}

				bool isOwner(int64_t nodeId) const
				{
					csLock lock(cs);

					for (auto &n : nodes)
					{
						if (n.state == NodeState_e::active_owner &&
							n.nodeId == nodeId)
							return true;
					}
					return false;
				}

				void purgeNodeId(int64_t nodeId)
				{
					csLock lock(cs);

					for (auto &n : nodes)
						if (n.nodeId == nodeId)
						{
							n.nodeId = 0;
							n.state = NodeState_e::free;
						}
				}

				bool changeOwner(int64_t nodeId)
				{
					if (!isMapped(nodeId))
						return false;

					auto changed = false;

					{
						csLock lock(cs);
						// is nodeId in the nodes list, if so, make it active_owner,
						// while we are in here, set any other active to active_clone
						for (auto &n : nodes)
							if (n.nodeId == nodeId)
							{
								n.state = NodeState_e::active_owner;
								changed = true;
							}
							else if (n.state == NodeState_e::active_owner)
							{
								n.state = NodeState_e::active_clone;
							}

					}

					// if we find and change us in the list, then we add
					// ourselves.
					if (!changed)
						changed = addMapping(nodeId, NodeState_e::active_owner);

					return changed; // actually showing success addMapping
				}

				std::vector<int64_t> getByStatus(NodeState_e state)
				{
					csLock lock(cs);
					std::vector<int64_t> result;

					for (auto &n : nodes)
						if (n.state == state)
							result.push_back(n.nodeId);

					return result;
				}

				// lock externally before calling this funtion
				std::vector<int64_t> getReplicas()
				{
					std::vector<int64_t> result;

					for (auto &n : nodes)
						if (n.state >= NodeState_e::routable)
							result.push_back(n.nodeId);

					return result;
				}

				// returns a list of nodes the partition was removed from
				// primarily so data on nodes owning these partitions can be 
				// cleaned up.
				std::vector<int64_t> purgeIncomplete()
				{
					csLock lock(cs);
					std::vector<int64_t> result;

					for (auto &n : nodes)
						if (n.state != NodeState_e::active_owner && 
							n.state != NodeState_e::active_clone)
						{
							result.push_back(n.nodeId);
							n.state = NodeState_e::free;
							n.nodeId = 0;							
						}

					return result;
				}
			};

			mutable CriticalSection cs;

			// locStat is a map of location and status
			bigRing<int, location_s> part2node;

			PartitionMap();

			~PartitionMap() = default;

			// return a list of partitions mapped to a nodeId
			// this would generally be used for self discovery
			std::vector<int> getPartitionsByNodeId(int64_t nodeId);
			std::vector<int> getPartitionsByNodeIdAndStates(int64_t nodeId, std::unordered_set<NodeState_e> states);

			// return list of nodeIds by state
			std::vector<int> getNodeIdsByState(NodeState_e state);

			// return a list of nodeIds for a given partition
			// this would generally be used to route inserts
			std::vector<int64_t> getNodesByPartitionId(int partitionId) const;

			bool isClusterComplete(int totalPartitions, std::unordered_set<NodeState_e> states, int replication = 1);

			bool isOwner(int partitionId, int64_t nodeId) const;
			bool isMapped(int partitionId, int64_t nodeId) const;
			NodeState_e getState(int partitionId, int64_t nodeId) const;

			std::vector<int> getMissingPartitions(int totalPartitions, std::unordered_set<NodeState_e> states, int replication = 1);

			// removes any partition/node pairs that aren't in ACTIVE or CLONE state
			// returns a list of partitions that can be removed from THIS OpenSet node.
			std::vector<int> purgeIncomplete();

			void setOwner(int partitionId, int64_t nodeId);
			void setState(int partitionId, int64_t nodeId, NodeState_e state);
			bool swapState(int partitionId, int64_t oldOwner, int64_t newOwner) const;

			void removeMap(int partitionId, int64_t nodeId, NodeState_e state) const;

			void purgeNodeById(int64_t nodeId);
			void purgeByState(NodeState_e state);

			void clear();

			void serializePartitionMap(cjson* doc);

			void changeMapping(
				cjson* config, 
				std::function<void(int)> addPartition_cb,
				std::function<void(int)> deletePartition_cb);
			
			void deserializePartitionMap(cjson* doc);
			void loadPartitionMap();
			void savePartitionMap();
		};
	};
};