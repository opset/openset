#pragma once

#include "cjson/cjson.h"

#include "internodemapping.h"
#include "internoderouter.h"
#include "database.h"

/*
* Teamster runs a team of Ponies (Ok, so, it manages a cluster or OpenSet Nodes)
*
* At any one time, only one node can be the Teamster, all nodes will have a loaded and running
* teamster thread running the Teamster object. The non-elected nodes will cycle waiting to become
* elected and take on the Teamster role.
*
* The elected member is simply the node with the lowest node_id. Node IDs are just the time stamp when the
* node was initialized, so generally the oldest node will always be the elected teamster (It's not really a
* democracy, and more like a monarchy).
* 
* Here is the logic:
* 
* TOP
* 
* 1. Am I the Teamster? No: loop to TOP. Yes: proceed.
*
* 2. Have any nodes Failed? No: Go to 3. Yes: proceed.
*    a. see if the node recovers quickly
*    b. delete nodes that didn't recover and remove their routes.
*    // at this point the map should have empty spots, we loop to TOP
*    
* 3. Is the Cluster "ACTIVE" Complete
*    // if anything happened in step 2, there should be a problem here
*    a. get a list Orphaned partitions that do not have an "ACTIVE" state.
*    b. remove any partial partitions - we only want ACTIVE/CLONE all others are now dirty.
*       i. remove these partitions from the async workers and the database (iterate tables)
*    c. find corresponding clones for these partitions and promote them to "ACTIVE"
*       i. if there isn't an clone, then we are in a bad state, global error.
*    d. broadcast the new map - the other nodes will clean up any orphans from this process.
*    // loop back to TOP. Go through all the logic again, we should end up at step 4.
*    
* 4. Are there enough "CLONES" for each partition (active_clone, active_build, active_ready).
*    a. if we are down to 4 or less nodes reduce redundancy requirements to 1 (if greater)
*       or, if we down to 1 node then set redundancy to 0.
*    b. get list of partitions that don't have enough active_clone, active_build or 
*       active_ready nodes.
*    c. for each partition missing a clone, get a list of nodes where any existing clones are
*       located.
*    d. for each partition find the least busy node (that doesn't contain the partition), this
*       will be the new replication target.
*       i.   transfer a new node map with active_build status for the missing partition on the 
*            target node.
*       ii.  transfer the partition to the new node.
*       iii. transfer a fresh node map upgrading the partition to active_clone.
*       // proceed to TOP, run checks
* 
*  5. If we made here we are not in an error state, so clear the error state.
*    
*/


namespace openset
{
	namespace mapping
	{
		class Sentinel
		{
			mutable CriticalSection cs_dead;
			Mapper* mapper;
			PartitionMap* partitionMap;
			db::Database* database;

			// they come back from the dead, we have to keep track of that
			std::unordered_set<int64_t> deadNodes;

            int64_t lastMapChange{0};
            bool inBalance{true};

		public:
			explicit Sentinel(Mapper* mapper, db::Database* database);

			~Sentinel() = default;

			// is this node the teamster
			bool isSentinel() const;
            bool isBalanced() const;
			int64_t getSentinel() const;
			bool failCheck();

            void setMapChanged()
            {
                lastMapChange = Now();
            }

            bool wasDuringMapChange(const int64_t startTime, const int64_t endTime) const
            {
                // started before and ended after change (change during)
                if (startTime - 500 < lastMapChange && endTime + 500 > lastMapChange)
                    return true;

                // started around 100ms of map change                
                if (startTime > lastMapChange - 500 && startTime < lastMapChange + 500)
                    return true;

                // ended around 100ms of map change
                if (endTime > lastMapChange - 500 && endTime < lastMapChange + 500)
                    return true;

                return false;
            }

			static void dropLocalPartition(const int partitionId);
            static cjson getPartitionStatus();

            bool isClusterComplete() const;
            int getFailureTolerance() const;
            int getRedundancyLevel() const;

		private:

			void markDeadRoute(int64_t nodeId)
			{
				csLock lock(cs_dead);
				deadNodes.insert(nodeId);
			}

			bool tranfer(const int partitionId, const int64_t sourceNode, const int64_t targetNode);

			bool broadcastMap();
			void runMonitor();
		};
	};

	namespace globals
	{
		extern openset::mapping::Sentinel* sentinel;
	}
};