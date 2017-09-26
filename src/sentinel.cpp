#include "common.h"
#include "sentinel.h"
#include "tablepartitioned.h"
#include "asyncpool.h"

namespace openset
{
	namespace globals
	{
		openset::mapping::Sentinel* sentinel;
	}
};

openset::mapping::Sentinel::Sentinel(Mapper* mapper, openset::db::Database* database):
	mapper(mapper),
	partitionMap(mapper->getPartitionMap()),
	database(database),
	//xferQueueSize(0),
	inErrorState(false),
	inXferPurge(false)
{
	globals::sentinel = this;

	// fire up the worker
	std::thread monitorRunner(&openset::mapping::Sentinel::runMonitor, this);
	monitorRunner.detach();
}

openset::mapping::Sentinel::~Sentinel() { }

bool openset::mapping::Sentinel::isSentinel() const
{
	return mapper->getSentinelId() == globals::running->nodeId;
}

int64_t openset::mapping::Sentinel::getSentinel() const
{
	return mapper->getSentinelId();
}

bool openset::mapping::Sentinel::failCheck()
{
	// Did we lose many nodes?
	auto failedCount = mapper->countFailedRoutes();

	// are there any downed nodes? Because this could be bad!
	if (failedCount)
	{
		//enterErrorState(); // enter error state if we aren't already

		Logger::get().error("NODE DOWN - " + to_string(failedCount) + " node(s) down... verifying");
		ThreadSleep(500);

		failedCount = mapper->countFailedRoutes();

		if (!failedCount)
		{
			Logger::get().info("NODE DOWN - connections re-established");
			return true;
		}

		auto deadRoutes = mapper->getFailedRoutes();

		// purge partitions owned by the dead node from the map
		// and remove the route
		for (auto d : deadRoutes)
		{
			markDeadRoute(d);
			partitionMap->purgeNodeById(d);
			mapper->removeRoute(d);
		}

		Logger::get().info(to_string(deadRoutes.size()) + " node(s) removed.");

		//enterErrorState();
		if (isSentinel())
			broadcastMap();
		// go back up and test all logic
		return true;
	}

	return false;
}

bool openset::mapping::Sentinel::tranfer(int partitionId, int64_t sourceNode, int64_t targetNode) const
{
	// make a JSON request payload
	cjson request;

	request.set("action", "transfer");
	auto params = request.setObject("params");
	params->set("source_node", sourceNode);
	params->set("target_node", targetNode);
	params->set("partition", partitionId);

	Logger::get().info("dispatching transfer " + to_string(partitionId) + " to " + globals::mapper->getRouteName(targetNode));

	int64_t jsonLength;
	auto jsonPtr = cjson::StringifyCstr(&request, jsonLength);

	auto message = openset::globals::mapper->dispatchSync(
		sourceNode, // we send this to the source node, it will copy to target
		openset::mapping::rpc_e::inter_node,
		jsonPtr,
		jsonLength);

	if (!message)
	{
		openset::globals::mapper->getPartitionMap()->setState(partitionId, targetNode, openset::mapping::NodeState_e::free);
		Logger::get().error("xfer error on paritition " + to_string(partitionId) + ".");
		return false;
	}

	return true;
}

bool openset::mapping::Sentinel::broadcastMap() const
{
	cjson configBlock;
	configBlock.set("action", "map_change");

	auto params = configBlock.setObject("params");

	params->set("config_version", globals::running->updateConfigVersion());

	// make a node called routes, serialize the routes (nodes) under it
	mapper->serializeRoutes(params->setObject("routes"));
	// make a node called cluster, serialize the partitionMap under it
	partitionMap->serializePartitionMap(params->setObject("cluster"));

	// JSON to text
	auto newNodeJson = cjson::Stringify(&configBlock);
	
	// blast this out to our cluster
	auto responses = openset::globals::mapper->dispatchCluster(
		openset::mapping::rpc_e::inter_node,
		newNodeJson.c_str(),
		newNodeJson.length());

	openset::globals::mapper->releaseResponses(responses);

	// TODO - parse all those responses and figure out if this is actually TRUE
	return true;
}

void printMap()
{

	auto pad3 = [](int number)
	{
		auto str = to_string(number);
		while (str.length() < 3)
			str = " " + str;
		return str;
	};

	auto routes = openset::globals::mapper->getActiveRoutes();
	auto partCount = openset::globals::running->partitionMax;

	for (auto p = 0; p < partCount; ++p)
	{

		cout << pad3(p) << ": |";

		for (auto r: routes)
		{

			auto state = openset::globals::mapper->partitionMap.getState(p, r);

			switch (state)
			{
				case openset::mapping::NodeState_e::free: 
					cout << " |";
				break;
				case openset::mapping::NodeState_e::failed: 
					cout << "#|";
				break;
				case openset::mapping::NodeState_e::active_owner: 
					cout << "A|";
				break;
				case openset::mapping::NodeState_e::active_clone: 
					cout << "C|";
				break;
				case openset::mapping::NodeState_e::active_placeholder: 
					cout << "-|";
				break;
				default: 
					cout << " |";
			}
			
		}
		cout << endl;	
	}

	cout << endl;

}

void openset::mapping::Sentinel::dropLocalPartition(int partitionId)
{
	if (openset::globals::mapper->partitionMap.isMapped(
		partitionId, 
		openset::globals::running->nodeId))
	{
		openset::globals::async->suspendAsync();

		openset::globals::async->freePartition(partitionId);

		// drop this partition from any table objects
		for (auto t : openset::globals::database->tables)
			t.second->releasePartitionObjects(partitionId);

		openset::globals::async->resumeAsync();
	}
}

void openset::mapping::Sentinel::runMonitor() 
{
	auto actingSentinel = false;

	int64_t lastMovedClonePartition = -1;

	while (true)
	{		
		auto routes = mapper->countRoutes();
		auto up = mapper->countActiveRoutes();

		// if there are not enough (active) nodes.. or.. we are not part of a cluster...
		// or not initialized
		if (routes <= 1 || 
			globals::running->state != openset::config::nodeState_e::active)
		{
			ThreadSleep(100);
			continue;
		}

		Logger::get().info("waiting for cluster - " + to_string(up) + ":" + to_string(routes) + " reporting.");

		if (routes == up)
			break;

		ThreadSleep(100);
	}

	Logger::get().info("cluster complete.");

	auto mapTime = Now();

	// this loop runs every 100 milliseconds to ensure that
	// our cluster is complete. 
	while (true)
	{
		auto partitionMax = openset::globals::async->getPartitionMax();

		if (failCheck())
			continue;

		// Are we running this? If not, lets loop and wait until
		// someday we get to be the boss
		if (!isSentinel())
		{
			if (actingSentinel)
			{
				actingSentinel = false;
				Logger::get().info("no longer team leader.");
				//purgeTransferQeueue(); // clear the queue, it's not our job to watch it now
			}

			if (Now() > mapTime + 5000)
			{
				printMap();
				mapTime = Now();
			}

			ThreadSleep(100);
			continue;
		}

		if (!actingSentinel)
		{
			actingSentinel = true;
			Logger::get().info("promoted to team leader.");

			// purge placeholders - send a map
			partitionMap->purgeByState(NodeState_e::active_placeholder);
			if (broadcastMap())
				Logger::get().info("promotion - broadcast new map.");
			else
				Logger::get().error("promotion - broadcast failed.");
			continue;
		}

		if (Now() > mapTime + 5000)
		{
			printMap();
			mapTime = Now();
		}

		// ----------------------------------------------------------------
		// ACTIVE
		// ----------------------------------------------------------------

		// Are we ACTIVE complete on all partitions.
		if (!partitionMap->isClusterComplete(
			partitionMax, 
			{ 
				NodeState_e::active_owner 
			}, 
			1))
		{
			//enterErrorState(); // enter error state if we aren't already

			// look for missing active nodes, if any are missing we promote them
			// replication is expected to be 1 for active nodes
			auto missingActive = partitionMap->getMissingPartitions(
				partitionMax, 
				{ 
					NodeState_e::active_owner 
				}, 
				1);

			// purge partial partitions - anything that isn't ACTIVE or CLONE, all else is dirty
			// returns a list of partitions that THIS node can remove
			auto cleaningList = partitionMap->purgeIncomplete();

			// promote replicas for missingActive
			for (auto p: missingActive)
			{
				auto candidateNodes = partitionMap->getNodesByPartitionId(p);
				auto promoted = false;

				for (auto n : candidateNodes)
				{
					if (!promoted)
					{
						Logger::get().info("partition " + to_string(p) + " changed to ACTIVE on " + openset::globals::mapper->getRouteName(n) + ".");
						partitionMap->setOwner(p, n);
					}
					else
					{
						Logger::get().info("partition " + to_string(p) + " changed to CLONE " + openset::globals::mapper->getRouteName(n) + ".");
						partitionMap->setState(p, n, NodeState_e::active_clone);
					}

					promoted = true;
				}

				// nothing was promoted... this is bad
				if (!promoted)
				{
					Logger::get().error("cluster is broken, missing replica for partition " + to_string(p) + ".");
					// TODO - Handle FUBAR scenario
					return; // leave
				}
			}

			// drop all the partitions we don't want around from the async engine
			// and then drop them from the database/tables as well
			for (auto c : cleaningList)
			{
				// drop this partition from the async engine
				globals::async->freePartition(c);

				// drop this partition from any table objects
				for (auto t : database->tables)
					t.second->releasePartitionObjects(c);
			}

			// purge any transfer queues
			// whatever cluster balancing was happening is garbage now
			// Note - the cleaning list above clears out anything that 
			//        isn't an active_owner/active_clone status including
			//        nodes that are in build state. 
			//purgeTransferQeueue();

			/* GOOD TIMES - If we made it here then we should have a query complete cluster meaning
			* all partitions have an active copy.
			*
			* This means we can share the cluster map with the other nodes. The should receive a map
			* that has less mappings than they have at the time of receipt. So, those nodes will compare
			* and promote partitions, and remove any partitions that don't match exactly what is in the map
			* this will leave all nodes consistent.
			*/


			// save changes to our map
			//OpenSet::globals::mapper->saveRoutes();
			//OpenSet::globals::mapper->savePartitions();

			// share the partitionMap, wait for OK.

			if (broadcastMap())
			{
				Logger::get().info("primary check - broadcast new map.");
			}
			else
			{
				Logger::get().error("primary check - broadcast failed.");
			}

			// go back up and test all logic
			continue;
		}

		// get number of active routes		
		auto routes = mapper->countRoutes();

		// number of replicas, so 2 would mean there are three copies, the active and two clones
		auto replicas = 2; // amount of replication we want in the cluster

		// adjust the number of replicas depending on the number of remaining nodes in the cluster
		if (routes == 1)
			replicas = 0;
		else if (routes <= 3)
			replicas = 1;

		// ----------------------------------------------------------------
		// CLONES
		// ----------------------------------------------------------------

		// do we have enough clones to meet our replication requirements
		if (replicas && 
			!partitionMap->isClusterComplete(
				partitionMax, 
				{ 
					NodeState_e::active_clone, // this is why modern C++ is so nice.
					NodeState_e::active_placeholder,
				}, 
				replicas
			))
		{
			//enterErrorState(); // enter error state if we aren't already

			// look for missing active nodes, if any are missing we promote them
			// replication is expected to be 1 for active nodes
			auto missingClones = partitionMap->getMissingPartitions(
				partitionMax, 
				{ 
					NodeState_e::active_clone,
					NodeState_e::active_placeholder,
				}, 
				replicas);

			for (auto p : missingClones)
			{
				// these are nodes where this partition is also found
				auto foundOnNodes = partitionMap->getNodesByPartitionId(p);

				int64_t sourceNode = -1;

				for (auto n : foundOnNodes)
				{
					auto state = partitionMap->getState(p, n);
					if (state == NodeState_e::active_owner ||
						state == NodeState_e::active_clone)
					{
						sourceNode = n;
						break;
					}
				}

				if (sourceNode == -1)
				{
					Logger::get().error("a source node for partition " + to_string(p) + " could net be found (replication " + to_string(replicas) + ").");
					continue;
				}

				// this finds the least busy node that DOES NOT have this partition on it already
				// note, it takes the list of nodes that have the partition on it, and excludes those
				//auto targetNode = partitionMap->getLowestPartitionCount(NodeState_e::active_clone, foundOnNodes);

				auto nodesByPartitions = this->mapper->getPartitionCountsByRoute(
					{
						NodeState_e::active_clone,
						NodeState_e::active_placeholder,
					}
				);
	
				int64_t targetNode = -1;

				// go through list of nodes, lowest population to greatest population
				// of partitions matching the states above.
				for (auto n: nodesByPartitions)
				{
					auto nodeId = n.first; // easier reading

					// if node is this node, then back to top
					//if (nodeId == globals::config->nodeId)
						//continue;

					// if the partition is already mapped to `n` then back to top
					if (mapper->partitionMap.isMapped(p, nodeId))
						continue;

					targetNode = nodeId;
					break;
				}
				
				if (targetNode == -1)
				{
					printMap();
					ThreadSleep(5000);
					Logger::get().error("a target node for partition " + to_string(p) + " could net be found (replication " +  to_string(replicas) + ").");
					// TODO - Handle FUBAR scenario
					continue; // leave
				}

				Logger::get().info("partition " + to_string(p) + " being replicated to " + globals::mapper->getRouteName(targetNode) + ".");

				// update this map

				// NOTE - replicated nodes start in active_build state
				mapper->partitionMap.setState(p, targetNode, NodeState_e::active_placeholder);
				//mapper->savePartitions();

				// broadcast this revised map
				if (broadcastMap())
					Logger::get().info("replication check (1) - broadcast new map.");
				else
					Logger::get().error("replication check (1) - broadcast failed.");

				//  TRANSFER PARTITION HERE 				
				if (tranfer(p, sourceNode, targetNode)) // update the map again!
					mapper->partitionMap.setState(p, targetNode, NodeState_e::active_clone);

				if (broadcastMap())
					Logger::get().info("replication check (2) - broadcast new map.");
				else
					Logger::get().error("replication check (2) - broadcast failed.");

				// we want to go back to the top after each transfer and see if any other conditions have changed
				break; 
			}
			continue;
		}

		// ----------------------------------------------------------------
		// BALANCE ACTIVES
		// ----------------------------------------------------------------

		// check for "heavy" nodes
		if (replicas)
		{			
			auto heavyList = this->mapper->getPartitionCountsByRoute({NodeState_e::active_owner});

			// is there more than one node? Is the difference between the busiest and least busy
			// node more than one?
			if (heavyList.size() > 1 && 
				heavyList.front().second - heavyList.back().second > 1)
			{
				//enterErrorState(); // enter error state if we aren't already

				auto heavyNode = heavyList.front().first;
				auto lightIter = heavyList.rbegin();

				// get a partition from the heavyNode
				auto partitionList = partitionMap->getPartitionsByNodeId(heavyNode);

				// find an active_owner partition
				auto partition = -1;

				for (auto p:partitionList)
					if (partitionMap->getState(p, heavyNode) == NodeState_e::active_owner)
					{
						partition = p;
						break;
					}

				if (partition == -1)
					continue;

				// do both these nodes have the partition? If so we can probably swap!
				for (; heavyNode != (*lightIter).first; ++lightIter)
				{
					auto targetNode = (*lightIter).first;

					// is this a swappable node?
					// if the targetNode contains the clone, we can simply swap
					// responsibilites. 
					if (partitionMap->isMapped(partition, targetNode) &&
						partitionMap->getState(partition, targetNode) == NodeState_e::active_clone)
					{								
						partitionMap->swapState(partition, heavyNode, targetNode);
						//mapper->savePartitions();

						// broadcast this revised map
						if (broadcastMap())
							Logger::get().info("balance - swapping roles on partition " + to_string(partition) + ".");
						else
							Logger::get().error("error balance - swapping roles on partition " + to_string(partition) + ".");

						break;
					}

					// can we transfer a partition?
					// if the partition is not present on the target node (in any state)
					// we can adjust the map and send instructions to transfer the
					// partition from the heavy node to the target node
					if (!partitionMap->isMapped(partition, targetNode))
					{
												
						// set the target to a build state
						mapper->partitionMap.setState(partition, targetNode, NodeState_e::active_placeholder);

						// broadcast this revised map
						if (broadcastMap())
							Logger::get().info("balance - moving roles on partition " + to_string(partition) + ".");
						else
							Logger::get().error("error balance - moving roles on partition " + to_string(partition) + ".");

						//  TRANSFER PARTITION HERE 				
						if (tranfer(partition, heavyNode, targetNode)) // update the map again!
						{
							if (heavyNode == openset::globals::running->nodeId)
								dropLocalPartition(partition);

							// remove the old active_onwer from the heavy node
							mapper->partitionMap.removeMap(partition, heavyNode, NodeState_e::active_owner);
							// set the new node as the active_owner of this partition
							mapper->partitionMap.setState(partition, targetNode, NodeState_e::active_owner);
						}					

						if (broadcastMap())
							Logger::get().info("replication check (2) - broadcast new map.");
						else
							Logger::get().error("replication check (2) - broadcast failed.");

						break;

					}
				}

				continue;
			}
		}

		// ----------------------------------------------------------------
		// BALANCE REPLICAS
		// ----------------------------------------------------------------

		// check for "heavy" nodes
		if (replicas)
		{
			auto heavyList = this->mapper->getPartitionCountsByRoute({ NodeState_e::active_clone, NodeState_e::active_placeholder });

			// is there more than one node? Is the difference between the busiest and least busy
			// node more than one?
			if (heavyList.size() > 1 &&
				heavyList.front().second - heavyList.back().second > 1)
			{
				//enterErrorState(); // enter error state if we aren't already

				auto heavyNode = heavyList.front().first;
				auto targetNode = heavyList.back().first;

				// get a partition from the heavyNode
				auto partitionList = partitionMap->getPartitionsByNodeId(heavyNode);

				// find an active_clone partition
				auto partition = -1;

				for (auto p : partitionList)
				{
					if (p == lastMovedClonePartition)
						continue;

					if (partitionMap->getState(p, heavyNode) == NodeState_e::active_clone &&
						!partitionMap->isMapped(p, targetNode))
					{
						partition = p;
						break;
					}
				}

				if (partition == -1)
					continue;
				// can we transfer a partition?
				// if the partition is not present on the target node (in any state)
				// we can adjust the map and send instructions to transfer the
				// partition from the heavy node to the target node
				if (!partitionMap->isMapped(partition, targetNode))
				{

					// set the target to a build state
					mapper->partitionMap.setState(partition, targetNode, NodeState_e::active_placeholder);

					// broadcast this revised map
					if (broadcastMap())
						Logger::get().info("balance (clones) - moving roles on partition " + to_string(partition) + ".");
					else
						Logger::get().error("error balance (clones) - moving roles on partition " + to_string(partition) + ".");

					//  TRANSFER PARTITION HERE 				
					if (tranfer(partition, heavyNode, targetNode)) // update the map again!
					{
						lastMovedClonePartition = partition;

						// remove the local partition
						if (heavyNode == openset::globals::running->nodeId)
							dropLocalPartition(partition);

						// remove the old active_clone from the heavy node
						mapper->partitionMap.removeMap(partition, heavyNode, NodeState_e::active_clone);
						// set the new node as the active_clone of this partition
						mapper->partitionMap.setState(partition, targetNode, NodeState_e::active_clone);
					}

					if (broadcastMap())
						Logger::get().info("replication check (clones) - broadcast new map.");
					else
						Logger::get().error("replication check (clones) - broadcast failed.");


				}

				continue;
			}
		}

		// If we made it here, there are no errors to be concerned about.
		//clearErrorState(); // clear error state if set
		ThreadSleep(50); // sleep a little then we are back to the top of this loop

		lastMovedClonePartition = -1;

	}
}
