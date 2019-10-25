#include "internodemapping.h"
#include <tuple>
#include <unordered_set>

openset::mapping::PartitionMap::PartitionMap() = default;

std::vector<int> openset::mapping::PartitionMap::getPartitionsByNodeId(int64_t nodeId)
{
    csLock lock(cs);

    std::vector<int> result;
    location_s::node_s* status;

    for (auto& p : part2node)
        if ((status = p.second.isMapped(nodeId)) != nullptr &&
            status->state >= NodeState_e::routable)
            result.push_back(p.first);

    return result;
}

std::vector<int> openset::mapping::PartitionMap::getPartitionsByNodeIdAndStates(int64_t nodeId, std::unordered_set<NodeState_e> states)
{
    csLock lock(cs);

    std::vector<int> result;
    location_s::node_s* status;

    for (auto& p : part2node)
        if ((status = p.second.isMapped(nodeId)) != nullptr &&
            states.count(status->state))
            result.push_back(p.first);

    return result;
}


std::vector<int> openset::mapping::PartitionMap::getNodeIdsByState(NodeState_e state)
{
    csLock lock(cs);

    std::vector<int> result;

    // here we have a set where we will insert nodeIds that
    // match the provided `state`
    std::unordered_set<int64_t> matchedNodes;

    for (auto& p : part2node)
    {
        auto matching = p.second.getByStatus(state);
        matchedNodes.insert(matching.begin(), matching.end());
    }

    result.assign(matchedNodes.begin(), matchedNodes.end());
    return result;
}

std::vector<int64_t> openset::mapping::PartitionMap::getNodesByPartitionId(int partitionId)
{
    csLock lock(cs);

    if (const auto part = part2node.find(partitionId); part != part2node.end())
        return part->second.getReplicas();

    return {};
}

bool openset::mapping::PartitionMap::isClusterComplete(int totalPartitions, std::unordered_set<NodeState_e> states, int replication)
{
    csLock lock(cs);

    // go through the partitions, confirm that we
    // have an active node for each partition.

    // first step make a map of partitions with
    // an active_owner node, active_owners are queried
    std::unordered_map<int, int> partitions;

    for (auto& p : part2node)
    {

        for (auto s : states)
        {
            auto activeNodes = p.second.getByStatus(s);

            if (activeNodes.size())
            {
                if (!partitions.count(p.first))
                    partitions[p.first] = activeNodes.size();
                else
                    partitions[p.first] += activeNodes.size();
            }
        }
    }

    auto partitionsFound = 0;

    for (auto i = 0; i < totalPartitions; ++i)
        if (partitions.count(i) && partitions[i] >= replication)
            ++partitionsFound;

    return totalPartitions == partitionsFound;
}

bool openset::mapping::PartitionMap::isOwner(int partitionId, int64_t nodeId)
{
    csLock lock(cs);

    if (const auto pair = part2node.find(partitionId); pair != part2node.end())
        return pair->second.isOwner(nodeId);

    return false;
}

bool openset::mapping::PartitionMap::isMapped(int partitionId, int64_t nodeId)
{
    csLock lock(cs);

    if (const auto pair = part2node.find(partitionId); pair != part2node.end())
        return pair->second.isMapped(nodeId);

    return false;
}

openset::mapping::NodeState_e openset::mapping::PartitionMap::getState(int partitionId, int64_t nodeId)
{
    csLock lock(cs);

    auto pair = part2node.find(partitionId);

    if (pair == part2node.end())
        return NodeState_e::free;

    auto nodePtr = pair->second.getMapPtr();

    for (auto i = 0; i < MAPDEPTH; ++i, ++nodePtr)
        if (nodePtr->nodeId == nodeId)
            return nodePtr->state;

    return NodeState_e::free;
}

std::vector<int> openset::mapping::PartitionMap::getMissingPartitions(int totalPartitions, std::unordered_set<NodeState_e> states, int replication)
{
    // go through the partitions, confirm that we
    // have an active node for each partition.

    // first step make a map of partitions with
    // an active_owner node, active_owners are queried

    csLock lock(cs);

    std::unordered_map<int, int> partitions;

    for (auto& p : part2node)
    {
        for (auto s : states)
        {
            auto activeNodes = p.second.getByStatus(s);

            if (activeNodes.size())
            {
                if (!partitions.count(p.first))
                    partitions[p.first] = activeNodes.size();
                else
                    partitions[p.first] += activeNodes.size();
            }
        }
    }

    std::vector<int> unownedPartitions;

    for (auto i = 0; i < totalPartitions; ++i)
        if (!partitions.count(i) || partitions[i] != replication) // missing!
            unownedPartitions.push_back(i);

    return unownedPartitions;
}

std::vector<int> openset::mapping::PartitionMap::purgeIncomplete()
{
    csLock lock(cs);

    std::vector<int> result;

    for (auto& p : part2node)
    {
        // get a list of nodes that had `p` dropped
        auto droppedNodes = p.second.purgeIncomplete();

        // look through the nodes that had `p` dropped and if
        // that node is our node, then we return a list of `p`
        // for us to clean up
        for (auto n: droppedNodes)
            if (n == globals::running->nodeId)
                result.push_back(p.first);
    }

    return result;
}

void openset::mapping::PartitionMap::setOwner(int partitionId, int64_t nodeId)
{
    csLock lock(cs);

    if (auto pair = part2node.find(partitionId); pair == part2node.end())
        part2node.emplace(partitionId, location_s{});

    auto pair = part2node.find(partitionId);

    if (!pair->second.changeOwner(nodeId))
        pair->second.addMapping(nodeId, NodeState_e::active_owner);
}

void openset::mapping::PartitionMap::setState(int partitionId, int64_t nodeId, NodeState_e state)
{
    csLock lock(cs);

    if (auto pair = part2node.find(partitionId); pair == part2node.end())
        part2node.emplace(partitionId, location_s{});

    auto pair = part2node.find(partitionId);

    // get the mapping record (if it exists)
    auto mapping = pair->second.isMapped(nodeId);

    if (!mapping) // make a mapping record
        pair->second.addMapping(nodeId, state);
    else // update the existing mapping record
        mapping->state = state;
}

bool openset::mapping::PartitionMap::swapState(int partitionId, int64_t oldOwner, int64_t newOwner)
{
    csLock lock(cs);

    if (!part2node.count(partitionId))
        return false;

    const auto pair = part2node.find(partitionId);
    auto nodePtr = pair->second.getMapPtr();

    // iterate our nodes, looking for our tuples
    for (auto i = 0; i < MAPDEPTH; ++i , ++nodePtr)
    {
        if (nodePtr->nodeId == oldOwner)
            nodePtr->state = NodeState_e::active_clone;
        else if (nodePtr->nodeId == newOwner)
            nodePtr->state = NodeState_e::active_owner;
    }

    return true;
}

void openset::mapping::PartitionMap::removeMap(int partitionId, int64_t nodeId, NodeState_e state)
{
    csLock lock(cs);

    //if (!pair->second.changeOwner(nodeId))
    if (auto pair = part2node.find(partitionId); pair != part2node.end())
        pair->second.removeMapping(nodeId, state);
}

void openset::mapping::PartitionMap::purgeNodeById(int64_t nodeId)
{
    csLock lock(cs);

    for (auto& p : part2node)
        p.second.purgeNodeId(nodeId);
}

void openset::mapping::PartitionMap::purgeByState(NodeState_e state)
{
    std::vector < std::tuple<int, int64_t, NodeState_e>> purgeList;

    {
        csLock lock(cs);

        for (auto& p : part2node)
            for (auto& n : p.second.nodes)
                if (n.state == state)
                    purgeList.emplace_back(p.first, n.nodeId, state);
    }

    for (auto &p : purgeList)
        this->removeMap(std::get<0>(p), std::get<1>(p), std::get<2>(p));
}

void openset::mapping::PartitionMap::clear()
{
    csLock lock(cs);

    for (auto& n : part2node)
        n.second.clear();
}

void openset::mapping::PartitionMap::serializePartitionMap(cjson* doc)
{
    csLock lock(cs);

    doc->setType(cjson::Types_e::OBJECT);

    // generate the document
    for (auto& p : part2node)
    {
        // object root nodes are named by string representation
        // of partition number i.e. partition 6 is "6".
        auto partDoc = doc->setObject(std::to_string(p.first));

        // partition object contains nodes object, to show
        // which nodes own the partition in which capacity
        auto nodeDoc = partDoc->setArray("nodes");

        // write out active nodes only as mini objects within
        // the nodes array (array of JSON objects)
        for (auto& n : p.second.nodes)
        {
            std::string state;
            // push out the node state
            switch (n.state)
            {
            case NodeState_e::active_owner:
                state = "active_owner";
                break;
            case NodeState_e::active_clone:
                state = "active_clone";
                break;
            case NodeState_e::active_placeholder:
                state = "active_build";
                break;
            default:
                continue;
            }

            auto infoDoc = nodeDoc->pushObject();
            infoDoc->set("node_id", n.nodeId);
            infoDoc->set("state", state);
        }
    }

}

void openset::mapping::PartitionMap::changeMapping(
    cjson* config,
    std::function<void(int)> addPartition_cb,
    std::function<void(int)> deletePartition_cb)
{
    if (!config)
    {
        Logger::get().error("expecting /cluster in changeMapping.");
        return;
    }

    auto partitions = config->getNodes();

    // List of partition/node pairs we've seen in the new mapping.
    // We will clean up any that remain int he partitionMap that are not
    // in the mapsVisited set
    using Visited = std::tuple<int, int64_t, NodeState_e>;
    std::unordered_set<Visited> mapsVisited;

    std::unordered_set<int> newPartitions;

    for (auto p: partitions)
    {
        int partitionId = std::atoi(p->nameCstr());
        auto nodeDoc = p->xPath("nodes");

        if (!nodeDoc)
            continue;

        auto nodes = nodeDoc->getNodes();

        for (auto n : nodes)
        {
            auto nodeName = n->xPathString("node_name", "");
            auto nodeId = n->xPathInt("node_id", -1);

            if (nodeId == -1)
                continue;

            auto typeString = n->xPathString("state", "");

            NodeState_e state;

            if (typeString == "active_owner")
                state = NodeState_e::active_owner;
            else if (typeString == "active_clone")
                state = NodeState_e::active_clone;
            else if (typeString == "active_build")
                state = NodeState_e::active_placeholder;
            else
                continue;

            // do we have a new partition, one that is not currently
            // in the map.
            if (nodeId == globals::running->nodeId &&
                !isMapped(partitionId, nodeId))
                newPartitions.insert(partitionId);

            setState(partitionId, nodeId, state);

            // note that we've seen this partition/node/state combo
            mapsVisited.insert(Visited{ partitionId, static_cast<int64_t>(nodeId), state });
        }
    }

    {
        csLock lock(cs);
        // create a list of partition IDs that need cleaning up if we own them
        for (auto& p : part2node)
        {
            // going direct to the node list here
            auto nodePtr = p.second.getMapPtr();

            // iterate our nodes, looking for our tuples
            for (auto i = 0; i < MAPDEPTH; ++i, ++nodePtr)
            {
                if (nodePtr->state == NodeState_e::free)
                    continue;

                // check for orphaned partition/node/state combos, count will be 0
                if (!mapsVisited.count(Visited{ p.first, nodePtr->nodeId, nodePtr->state }))
                {
                    // we own this partition, so we must add it to our clean up list
                    if (nodePtr->nodeId == globals::running->nodeId && deletePartition_cb)
                    {
                        deletePartition_cb(p.first);
                        Logger::get().info("removing local partition " + to_string(p.first) + ".");
                    }

                    nodePtr->nodeId = 0;
                    nodePtr->state = NodeState_e::free;
                }
            }
        }
    }

    // perform call backs for any new partitions
    if (addPartition_cb)
        for (auto p : newPartitions)
        {
            Logger::get().info("adding local partition " + to_string(p) + ".");
            addPartition_cb(p);
        }

}

void openset::mapping::PartitionMap::deserializePartitionMap(cjson* doc)
{
    auto partitions = doc->getNodes();

    for (auto p : partitions)
    {
        auto partitionId = std::atoll(p->nameCstr());
        auto nodeDoc = p->xPath("nodes");

        if (!nodeDoc)
            continue;

        auto nodes = nodeDoc->getNodes();

        for (auto n : nodes)
        {
            auto nodeId = n->xPathInt("node_id", -1);

            if (nodeId == -1)
                continue;

            auto typeString = n->xPathString("state", "");
            NodeState_e state;

            if (typeString == "active_owner")
                state = NodeState_e::active_owner;
            else if (typeString == "active_clone")
                state = NodeState_e::active_clone;
            else if (typeString == "active_build")
                state = NodeState_e::active_placeholder;
            else
                continue;

            setState(partitionId, nodeId, state);
        }
    }

}

void openset::mapping::PartitionMap::loadPartitionMap()
{
    clear();

    if (!openset::IO::File::FileExists(globals::running->path + "partitions.json"))
    {
        auto doc = cjson::makeDocument();
        doc->setType(cjson::Types_e::ARRAY);
        doc->toFile(globals::running->path + "partitions.json", doc, true);
        delete doc;
    }

    cjson doc(globals::running->path + "partitions.json", cjson::Mode_e::file);
    deserializePartitionMap(&doc);
}

void openset::mapping::PartitionMap::savePartitionMap()
{
    cjson doc;

    serializePartitionMap(&doc);

    // write the file
    cjson::toFile(globals::running->path + "partitions.json", &doc);
}
