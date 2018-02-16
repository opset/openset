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
	database(database)
{
	globals::sentinel = this;

	// fire up the worker
	std::thread monitorRunner(&openset::mapping::Sentinel::runMonitor, this);
	monitorRunner.detach();
}

bool openset::mapping::Sentinel::isSentinel() const
{
	return mapper->getSentinelId() == globals::running->nodeId;
}

bool openset::mapping::Sentinel::isBalanced() const
{
    return Now() - lastMapChange > 15'000;   
}

int64_t openset::mapping::Sentinel::getSentinel() const
{
	return mapper->getSentinelId();
}

cjson openset::mapping::Sentinel::getPartitionStatus()
{

    cjson doc;

    auto routesBranch = doc.setObject("routes");

    {
        csLock lock(openset::globals::mapper->cs);

    	auto routes = openset::globals::mapper->routes;

        for (const auto &r : routes)
        {
            const auto routeName = globals::mapper->getRouteName(r.first);
            auto routeInfoBranch = routesBranch->setObject(routeName);

            routeInfoBranch->set("name", routeName );
            routeInfoBranch->set("public_host", r.second.first );
            routeInfoBranch->set("public_port", r.second.second );
        }
    }

    
    auto partitionsBranch = doc.setArray("partitions");
	const auto partCount = openset::globals::running->partitionMax;
    const auto routes = openset::globals::mapper->getActiveRoutes();

	for (auto p = 0; p < partCount; ++p)
	{
		auto entry = partitionsBranch->pushObject();

        entry->set("partition", p);
        auto routeObject = entry->setObject("routes");
        
		for (auto r: routes)
		{
            const auto routeName = openset::globals::mapper->getRouteName(r);

			const auto state = openset::globals::mapper->partitionMap.getState(p, r);

			switch (state)
			{
				case openset::mapping::NodeState_e::failed: 
					routeObject->set(routeName, "failed");
				break;
				case openset::mapping::NodeState_e::active_owner: 
					routeObject->set(routeName, "active");
				break;
				case openset::mapping::NodeState_e::active_clone: 
					routeObject->set(routeName, "clone");
				break;
				case openset::mapping::NodeState_e::active_placeholder: 
					routeObject->set(routeName, "move");
				break;
				default: 
					routeObject->set(routeName, "free");
				break;
			}
			
		}
	}

    return doc;
}

bool openset::mapping::Sentinel::isClusterComplete() const
{
    const auto partitionMax = openset::globals::async->getPartitionMax();

    return partitionMap->isClusterComplete(
		partitionMax, 
		{ NodeState_e::active_owner }
    );    
}

int openset::mapping::Sentinel::getFailureTolerance() const
{
	const auto routes = mapper->countRoutes();

	// number of replicas, so 2 would mean there are three copies, the active and two clones
    auto replicas = 2;

	// adjust the number of replicas depending on the number of remaining nodes in the cluster
	if (routes == 1)
		return 0;
	
    if (routes <= 4)
    {
        return partitionMap->isClusterComplete(
    	    openset::globals::async->getPartitionMax(), 
	        { 
		        NodeState_e::active_clone
	        }, 
	        1) ? 1 : 0;
            
    }
    
    const auto twoFails = partitionMap->isClusterComplete(
    	openset::globals::async->getPartitionMax(), 
	    { 
		    NodeState_e::active_clone
	    }, 
	    2);

    if (twoFails)
        return 2;
    
    const auto oneFail = partitionMap->isClusterComplete(
    	openset::globals::async->getPartitionMax(), 
	    { 
		    NodeState_e::active_clone
	    }, 
	    1);    

    if (oneFail)
        return 1;

    return 0;
}

int openset::mapping::Sentinel::getRedundancyLevel() const
{
	const auto routes = mapper->countRoutes();

	// number of replicas, so 2 would mean there are three copies, the active and two clones
	auto replicas = 2; // amount of replication we want in the cluster

	// adjust the number of replicas depending on the number of remaining nodes in the cluster
	if (routes == 1)
		replicas = 0;
	else if (routes <= 4)
		replicas = 1;

    return replicas;    
}

bool openset::mapping::Sentinel::failCheck()
{
	// Did we lose many nodes?
	auto activeRoutes = mapper->getActiveRoutes();

	auto errorCount = 0;

	for (auto r : activeRoutes)
	{
        // don't ping ourselves.
        if (r == openset::globals::running->nodeId)
            continue;
	
		const auto result = mapper->dispatchSync(
			r,
			"GET",
			"/ping",
			{},
			nullptr, 
			0);

		auto isValid = false;

		if (result)
		{
			const std::string resultJson( result->data,result->length );
			cjson json(resultJson, cjson::Mode_e::string);

			if (json.xPathBool("/pong", false))
				isValid = true;
		}

		if (!isValid)
		{
			markDeadRoute(r);
			partitionMap->purgeNodeById(r);
			mapper->removeRoute(r);
			Logger::get().error("node down, removing.");
			++errorCount;
		}
	}

	if (isSentinel() && errorCount)
	{
		broadcastMap();
		return true;
	}

	return false;
}

bool openset::mapping::Sentinel::tranfer(const int partitionId, const int64_t sourceNode, const int64_t targetNode) const
{
	// make a JSON request payload
	auto targetNodeName = globals::mapper->getRouteName(targetNode);

	Logger::get().info("dispatching transfer " + to_string(partitionId) + " to " + globals::mapper->getRouteName(targetNode));

	const auto message = openset::globals::mapper->dispatchSync(
		sourceNode, // we send this to the source node, it will copy to target
		"PUT",
		"/v1/internode/transfer",
		{{"partition", std::to_string(partitionId)}, { "node", targetNodeName }},
		nullptr,
		0);

	if (!message)
	{
		// this will unset the partition from the map
		openset::globals::mapper->getPartitionMap()->setState(partitionId, targetNode, openset::mapping::NodeState_e::free);
		Logger::get().error("transfer error on paritition " + to_string(partitionId) + ".");
		return false;
	}
	else
	{
		// TODO Parse message for errors
	}

	return true;
}

bool openset::mapping::Sentinel::broadcastMap()
{
    cjson configBlock;

    setMapChanged();

	// make a node called routes, serialize the routes (nodes) under it
	mapper->serializeRoutes(configBlock.setObject("routes"));

	// make a node called cluster, serialize the partitionMap under it
	partitionMap->serializePartitionMap(configBlock.setObject("cluster"));
    	
	// blast this out to our cluster
	auto responses = openset::globals::mapper->dispatchCluster(
		"POST",
		"/v1/internode/map_change",
		{},
		configBlock);

	const auto inError = !responses.routeError;

	openset::globals::mapper->releaseResponses(responses);


    openset::globals::async->suspendAsync();
    openset::globals::async->balancePartitions();
    openset::globals::async->resumeAsync();

	return inError;
}


void openset::mapping::Sentinel::dropLocalPartition(const int partitionId)
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

    auto replicas = 0;
    auto lastPartitionMove = Now();

	while (true)
	{		
		const auto routes = mapper->countRoutes();
		const auto up = mapper->countActiveRoutes();

		// if there are not enough (active) nodes.. or.. we are not part of a cluster...
		// or not initialized
		if (routes <= 1 || 
			globals::running->state != openset::config::NodeState_e::active)
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


    int64_t lastFailCheck = 0;

	// this loop runs every 100 milliseconds to ensure that
	// our cluster is complete. 
	while (true)
	{
		const auto partitionMax = openset::globals::async->getPartitionMax();

        if (Now() - lastFailCheck > 250)
        {
		    failCheck();
            lastFailCheck = Now();
        }

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
			// purge place_holder partitions - anything that isn't ACTIVE or CLONE
            // anything else is likely part of plan from a previously elected node
			auto cleaningList = partitionMap->purgeIncomplete();

			// look for missing active nodes, if any are missing we promote them
			// replication is expected to be 1 for active nodes
			auto missingActive = partitionMap->getMissingPartitions(
				partitionMax, 
				{ 
					NodeState_e::active_owner 
				}, 
				1);

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

			// properly drop all the LOCAL partitions we no longer need
            if (cleaningList.size())
            {
                openset::globals::async->suspendAsync();
			    for (auto c : cleaningList)
			    {
				    // drop this partition from the async engine
				    globals::async->freePartition(c);

				    // drop this partition from any table objects
				    for (auto t : database->tables)
					    t.second->releasePartitionObjects(c);
			    }
                openset::globals::async->resumeAsync();
            }

			if (broadcastMap())
			{
				Logger::get().info("primary check - broadcast new map.");
			}
			else
			{
				Logger::get().error("primary check - broadcast failed.");
			}

			// go back up and test all logic
            inBalance = false;
			continue;
		}

		// get number of active routes		
		const auto routes = mapper->countRoutes();

		// number of replicas, so 2 would mean there are three copies, the active and two clones

		// adjust the number of replicas depending on the number of remaining nodes in the cluster
		if (routes == 1)
			replicas = 0;
		else if (routes < 5)
    	    replicas = 1; 
        else 
            replicas = 2;

	    // ----------------------------------------------------------------
		// PURGE OVER REPLICATED
		// ----------------------------------------------------------------
        {
            auto purged = false;

            for (auto p = 0; p < partitionMax; ++p)
            {
                auto nodes = partitionMap->getNodesByPartitionId(p);

                if (static_cast<int>(nodes.size()) > replicas + 1)
                {
                    for (auto n : nodes)
                    {
                        if (partitionMap->getState(p, n) == NodeState_e::active_clone)
                        {
						    if (n == openset::globals::running->nodeId)
							    dropLocalPartition(p);

						    // remove the old active_owner from the heavy node
						    mapper->partitionMap.removeMap(p, n, NodeState_e::active_clone);

				            if (broadcastMap())
					            Logger::get().info("replication check (2) - broadcast new map.");
				            else
					            Logger::get().error("replication check (2) - broadcast failed.");

                            purged = true;

                            break;
                        }
                    }
                }

                if (purged)
                    break;
            }

            if (purged)
            {
                inBalance = false;
                continue;
            }
        }

        // Lazy balance if - 
        // we are in high replication (3 total copies) and have at least 2 copies of 
        // everything. 

        if (replicas == 2 && 
            lastPartitionMove + 2000 > Now() &&
            partitionMap->isClusterComplete(partitionMax, { NodeState_e::active_clone }, 1))
            continue;

        lastPartitionMove = Now();


	    // ----------------------------------------------------------------
		// CLONES
		// ----------------------------------------------------------------

		// do we have enough clones to meet our replication requirements
		if (replicas && 
			!partitionMap->isClusterComplete(
				partitionMax, 
				{ 
					NodeState_e::active_clone, 
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
					if (const auto state = partitionMap->getState(p, n); 
                        state == NodeState_e::active_owner) // || state == NodeState_e::active_clone)
					{
						sourceNode = n;
						break;
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
                        //NodeState_e::active_owner,
						NodeState_e::active_clone,
						NodeState_e::active_placeholder,
					}
				);
	
				int64_t targetNode = -1;

				// go through list of nodes, lowest population to greatest population
				// of partitions matching the states above.
				for (auto iter = nodesByPartitions.rbegin(); iter != nodesByPartitions.rend(); ++iter)
				{
					const auto nodeId = iter->first; // easier reading

					// if the partition `p` is already mapped to `nodeId` then back to top
					if (mapper->partitionMap.isMapped(p, nodeId))
						continue;

					targetNode = nodeId;
					break;
				};
				
				if (targetNode == -1)
				{
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

                lastPartitionMove = Now();

				// we want to go back to the top after each transfer and see if any other conditions have changed
				break; 
			}

            inBalance = false;
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
				const auto heavyNode = heavyList.front().first;
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
					const auto targetNode = (*lightIter).first;

					// is this a swappable node?
					// if the targetNode contains the clone, we can simply swap
					// responsibilites. 
					if (partitionMap->isMapped(partition, targetNode) &&
						partitionMap->getState(partition, targetNode) == NodeState_e::active_clone)
					{								
						partitionMap->swapState(partition, heavyNode, targetNode);

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
						// set the target to a build (place_holder) state
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

							// remove the old active_owner from the heavy node
							mapper->partitionMap.removeMap(partition, heavyNode, NodeState_e::active_owner);
							// set the new node as the active_owner of this partition
							mapper->partitionMap.setState(partition, targetNode, NodeState_e::active_owner);
						}					

						if (broadcastMap())
							Logger::get().info("replication check (2) - broadcast new map.");
						else
							Logger::get().error("replication check (2) - broadcast failed.");

                        lastPartitionMove = Now();

						break;

					}
				}

                inBalance = false;

				continue;
			}
		}

		// ----------------------------------------------------------------
		// BALANCE REPLICAS
		// ----------------------------------------------------------------

		// check for "heavy" nodes
		if (replicas)
		{
			auto heavyList = this->mapper->getPartitionCountsByRoute({ 
                NodeState_e::active_clone, 
			    NodeState_e::active_placeholder 
			});

			// is there more than one node? Is the difference between the busiest and least busy
			// node more than one?
			if (heavyList.size() > 1 &&
				heavyList.front().second - heavyList.back().second > 1)
			{				
				const auto heavyNode = heavyList.front().first;
				const auto targetNode = heavyList.back().first;

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

                    lastPartitionMove = Now();

				}

                inBalance = false;

				continue;
			}
		}

        inBalance = true;
		// If we made it here, there are no errors to be concerned about.
		//clearErrorState(); // clear error state if set
		ThreadSleep(100); // sleep a little then we are back to the top of this loop

		lastMovedClonePartition = -1;

	}
}
