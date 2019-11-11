#include <thread>
#include <string>
#include <memory>
#include "file/file.h"
#include "config.h"

#include "sba/sba.h"
#include "internoderouter.h"

namespace openset
{
    namespace globals
    {
        openset::mapping::Mapper* mapper;
    };
};

/*
 *  MAILBOX
 */

openset::mapping::Mapper::Mapper():
    slotCounter(1)
{
    globals::mapper = this; // set the global

    // change - load is only peformed on resume
    // loadRoutes();
    //addRoute("startup", globals::running->nodeId, globals::running->host, globals::running->port);
}

openset::mapping::Mapper::RestConnection openset::mapping::Mapper::getCachedConnection(const int64_t routeId)
{
    csLock lock(poolCs);

    if (const auto iter = restPool.find(routeId); iter != restPool.end())
    {
        PoolVector newPool;

        const auto now = Now();
        for (auto &c : iter->second)
            if (c.stamp > now - 120'000)
                newPool.emplace_back(c);

        iter->second = std::move(newPool);

        if (iter->second.empty())
        {
            return nullptr;
        }

        auto info = iter->second.back();
        iter->second.pop_back();
        return info.connection;
    }

    return nullptr;
}

void openset::mapping::Mapper::returnCachedConnection(RestConnection connection)
{
    // don't cache on route 0 - which are adhoc connections
    if (connection->getRouteId() == 0)
        return;

    csLock lock(poolCs);

    if (auto iter = restPool.find(connection->getRouteId()); iter != restPool.end())
    {
        iter->second.push_back(ConnectionPoolItem_s{Now(), connection});
    }
    else
    {
        restPool[connection->getRouteId()] = PoolVector{ConnectionPoolItem_s{Now(), connection}};
    }
}

int64_t openset::mapping::Mapper::getSlotNumber()
{
    ++slotCounter;
    return slotCounter;
}

void openset::mapping::Mapper::addRoute(const std::string routeName, const int64_t routeId, const std::string ip, const int32_t port)
{
    csLock lock(cs); // lock

    // name if first
    if (auto name = names.find(routeId); name == names.end())
        names.emplace(routeId, routeName); // new name
    else
        name->second = routeName; // replace name

    // create it if it doesn't exist
    if (const auto rt = routes.find(routeId); rt == routes.end())
    {
        if (routeId == globals::running->nodeId)
        {
            const auto host = (globals::running->host == "0.0.0.0"s)
                    ? "127.0.0.1"s
                    : globals::running->host;

            routes.insert({ routeId, { host, globals::running->port } }); // local route answers on local ip:port
        }
        else
        {
            routes.insert({ routeId, { ip, port }}); // make it
        }
    }
}

void openset::mapping::Mapper::removeRoute(const int64_t routeId)
{
    csLock lock(cs); // lock

    // is it missing?
    if (const auto rt = routes.find(routeId); rt != routes.end())
    {
        routes.erase(rt);
        // clear the name out - will be in dictionary
        names.erase(names.find(routeId));
    }

    if (const auto rp = restPool.find(routeId); rp != restPool.end())
        restPool.erase(rp);
}

std::string openset::mapping::Mapper::getRouteName(const int64_t routeId)
{
    if (names.count(routeId))
        return names.find(routeId)->second;
    return "startup";
}

int64_t openset::mapping::Mapper::getRouteId(const std::string& routeName)
{
    for (auto &info : names)
        if (info.second == routeName)
            return info.first;
    return -1;
}

openset::web::RestPtr openset::mapping::Mapper::getRoute(const int64_t routeId)
{
    csLock lock(cs); // lock

    if (const auto rt = routes.find(routeId); rt == routes.end())
        return nullptr;
    else
    {
        if (auto cachedRoute = getCachedConnection(routeId); cachedRoute != nullptr)
            return cachedRoute;
        return std::make_shared<openset::web::Rest>(routeId, rt->second.first + ":" + to_string(rt->second.second));
    }
}

bool openset::mapping::Mapper::isRoute(const int64_t routeId)
{
    csLock lock(cs); // lock
    return routes.find(routeId) != routes.end();
}

bool openset::mapping::Mapper::isRouteNoLock(const int64_t routeId)
{
    return routes.find(routeId) != routes.end();
}

// send a message to a destination
bool openset::mapping::Mapper::dispatchAsync(
    const int64_t route,
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    char* payload,
    const size_t length,
    const openset::web::RestCbBin callback)
{
    auto rest = getRoute(route);

    const auto callbackWrapper = [&](const http::StatusCode code, const bool error, char* data, const size_t size)
    {
        returnCachedConnection(rest);
        callback(code, error, data, size);
    };

    // check if there is a route here
    if (rest)
    {
        rest->request(method, path, params, payload, length, callbackWrapper);
        return true;
    }
    return false;
}

bool openset::mapping::Mapper::dispatchAsync(
    const int64_t route,
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    std::string& payload,
    const openset::web::RestCbBin callback)
{
    auto rest = getRoute(route);

    const auto callbackWrapper = [&](const http::StatusCode code, const bool error, char* data, const size_t size)
    {
        returnCachedConnection(rest);
        callback(code, error, data, size);
    };

    // check if there is a route here
    if (rest)
    {
        rest->request(method, path, params, &payload[0], payload.length(), callbackWrapper);
        return true;
    }
    return false;
}

bool openset::mapping::Mapper::dispatchAsync(
    const int64_t route,
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    cjson& payload,
    const openset::web::RestCbBin callback)
{
    auto rest = getRoute(route);

    const auto callbackWrapper = [&](const http::StatusCode code, const bool error, char* data, const size_t size)
    {
        returnCachedConnection(rest);
        callback(code, error, data, size);
    };

    // check if there is a route here
    auto json = cjson::stringify(&payload);

    if (rest)
    {
        rest->request(method, path, params, &json[0], json.length(), callbackWrapper);
        return true;
    }
    return false;
}

openset::mapping::Mapper::DataBlockPtr openset::mapping::Mapper::dispatchSync(
    const int64_t route,
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    char* payload,
    const size_t length)
{
    mutex nextLock;
    condition_variable nextReady;
    auto ready = false;

    char* resultData;
    size_t resultSize;
    http::StatusCode resultStatus;

    const auto doneCb = [&ready, &nextReady, &resultData, &resultSize, &resultStatus](const http::StatusCode status, const bool error, char* data, const size_t size)
    {
        resultData = data;
        resultSize = size;
        resultStatus = status;
        ready = true;
        // assign then notify
        nextReady.notify_one();
    };

    if (!dispatchAsync(route, method, path, params, payload, length, doneCb))
        return nullptr;

    while (!ready)
    {
        unique_lock<std::mutex> lock(nextLock);
        auto const timeout = std::chrono::steady_clock::now() + 250ms;
        nextReady.wait_until(lock, timeout, [&ready]()
        {
            return ready;
        });
    }

    return std::make_shared<DataBlock>(resultData, resultSize, resultStatus);
}

openset::mapping::Mapper::DataBlockPtr openset::mapping::Mapper::dispatchSync(
    const int64_t route,
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    cjson& payload)
{
    auto json = cjson::stringify(&payload);
    return dispatchSync(route, method, path, params, &json[0], json.length());
}

class dispatchState
{
    int requestCount{ 0 };
    atomic<int> responseCount{ 0 };
    bool active{ true };
public:
    bool isActive() const
    {
        return active;
    }
    void deactivate()
    {
        active = false;
    }
    bool isComplete() const
    {
        return responseCount >= requestCount;
    }
    void incrRequest()
    {
        ++requestCount;
    }
    void incrResponse()
    {
        ++responseCount;
    }
};

openset::mapping::Mapper::Responses openset::mapping::Mapper::dispatchCluster(
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    char* data,
    const size_t length,
    const bool internalDispatch)
{
    mutex continueLock;
    condition_variable continueReady;

    CriticalSection responseCs;

    Responses result;
    auto callbackState = new dispatchState();

    const auto doneCb = [callbackState, &responseCs, &result, &continueReady](const http::StatusCode status, const bool error, char* data, const size_t size)
    {
        if (!callbackState->isActive())
        {
            // Trapped dead callback - if it aborted by error.
            // callbackstate will still be on the heap
            callbackState->incrResponse();
            if (callbackState->isComplete())
                delete callbackState;
            return;
        }

        {
            csLock lock(responseCs);
            // store this response in the response object, but first
            // copy data, as data will be destoryed when this callback goes out of scope
            auto dataCopy = static_cast<char*>(PoolMem::getPool().getPtr(size));
            memcpy(dataCopy, data, size);
            result.responses.emplace_back(openset::mapping::Mapper::DataBlock{ dataCopy, size, status });

            if (error)
                result.routeError = true;
        }

        callbackState->incrResponse();

        if (callbackState->isComplete())
            continueReady.notify_one();
    };

    std::vector<int64_t> cachedRoutes;

    // we copy the routes so that another thread won't corrupt them
    decltype(routes) tRoutes;

    {
        csLock lock(cs);
        tRoutes = routes;
    }

    // dispatchAsync to all our nodes
    {
        for (const auto& r : tRoutes)
        {
            // don't call this node unless we want to
            if (!internalDispatch && r.first == globals::running->nodeId)
                continue;

            if (!dispatchAsync(r.first, method, path, params, data, length, doneCb))
            {
                result.routeError = true;
                break;
            }
            callbackState->incrRequest();

            cachedRoutes.push_back(r.first);
        }
    }

    // we are going to loop and wait until we get all our responses back
    while (!callbackState->isComplete())
    {
        unique_lock<std::mutex> lock(continueLock);
        auto const timeout = std::chrono::steady_clock::now() + 50ms;
        continueReady.wait_until(lock, timeout, [callbackState]()
        {
            return callbackState->isComplete();
        });

        for (auto r : cachedRoutes)
            if (!isRoute(r))
            {
                // route is missing, meaning it got dumped
                // during this request
                result.routeError = true;
                break;
            }

        if (result.routeError)
            break;
    }

    // this callback is done
    callbackState->deactivate();

    if (callbackState->isComplete())
        delete callbackState;

    return result;
}

openset::mapping::Mapper::Responses openset::mapping::Mapper::dispatchCluster(
    const std::string& method,
    const std::string& path,
    const openset::web::QueryParams& params,
    cjson& json,
    const bool internalDispatch)
{
    int64_t jsonLength;
    const auto jsonText = cjson::stringifyCstr(&json, jsonLength);

    auto result = dispatchCluster(method, path, params, jsonText, jsonLength, internalDispatch);

    cjson::releaseStringifyPtr(jsonText);
    return result;
}

void openset::mapping::Mapper::releaseResponses(Responses& responseSet)
{
    for (auto &r: responseSet.responses)
    {
        PoolMem::getPool().freePtr(r.data);
        r.data = nullptr;
    }

    responseSet.responses.clear();
}

int64_t openset::mapping::Mapper::getSentinelId() const
{
    auto lowestId = LLONG_MAX;

    // scope lock the mapper
    csLock lock(cs);

    for (auto& r : routes)
        if (r.first < lowestId)
            lowestId = r.first;

    return lowestId;
}

int openset::mapping::Mapper::countFailedRoutes()
{
    auto failedCount = 0;

    // scope lock the mapper
    csLock lock(cs);

    for (auto& r : routes)
        if (!isRouteNoLock(r.first))
            ++failedCount;

    return failedCount;
}

int openset::mapping::Mapper::countActiveRoutes()
{
    auto activeCount = 0;

    // scope lock the mapper
    csLock lock(cs);

    for (auto& r : routes)
        if (isRouteNoLock(r.first))
            ++activeCount;

    return activeCount;
}

int openset::mapping::Mapper::countRoutes() const
{
    csLock lock(cs);
    return routes.size(); // return total routes including local route
}

std::vector<int64_t> openset::mapping::Mapper::getActiveRoutes()
{
    std::vector<int64_t> activeRoutes;

    csLock lock(cs);
    for (auto& r : routes)
        if (isRouteNoLock(r.first))
            activeRoutes.push_back(r.first);

    sort(activeRoutes.begin(), activeRoutes.end(),
        [](const int64_t a, const int64_t b)
    {
        return a > b;
    });

    return activeRoutes;
}

openset::mapping::Mapper::PartitionCounts openset::mapping::Mapper::getPartitionCountsByRoute(const std::unordered_set<NodeState_e>& states)
{
    auto activeRoutes = getActiveRoutes();

    std::vector<std::pair<int64_t, int>> result;

    for (auto r: activeRoutes)
    {
        // start at zero for the count on this route
        auto stats = std::make_pair(r, 0);

        // get a list of partitions assigned to this route/node
        auto partitions = globals::mapper->partitionMap.getPartitionsByNodeId(r);

        for (auto p : partitions)
        {
            // call getState on the partition/route, if the value is in `states` then increment
            if (states.count(globals::mapper->partitionMap.getState(p, r)))
                stats.second++;
        }

        result.emplace_back(stats);
    }

    sort(result.begin(), result.end(),
        [](const std::pair<int64_t, int>& a, const std::pair<int64_t, int>& b)
    {
        return a.second > b.second;
    });

    return result;
}

void openset::mapping::Mapper::changeMapping(
    const cjson& config,
    const std::function<void(int)>& addPartition_cb,
    const std::function<void(int)>& deletePartition_cb,
    const std::function<void(string, int64_t, string, int)>& addRoute_cb,
    const std::function<void(int64_t)>& deleteRoute_cb)
{
    const auto routesNode = config.xPath("/routes");

    if (routesNode)
    {
        auto routesList = routesNode->getNodes();

        // set used to check for routes that have vanished
        std::unordered_set<int64_t> providedRouteIds;

        for (auto r: routesList)
        {
            const auto name = r->xPathString("name", "");
            const auto id = r->xPathInt("id", 0);
            const auto host = r->xPathString("host", "");
            const auto port = r->xPathInt("port", 0);

            if (name.length() && id && host.length() && port)
            {
                providedRouteIds.insert(id);
                if (routes.count(id) == 0)
                    addRoute_cb(name, id, host, port);
            }
        }

        std::vector<int64_t> cleanup;
        for (const auto& r: routes)
            if (!providedRouteIds.count(r.first))
                cleanup.push_back(r.first);

        for (auto r: cleanup)
            deleteRoute_cb(r);
    }

    // change some routes
    partitionMap.changeMapping(
        config.xPath("/cluster"),
        addPartition_cb,
        deletePartition_cb);

    // TODO add /routes
}

void openset::mapping::Mapper::loadPartitions()
{
    partitionMap.loadPartitionMap();
}

void openset::mapping::Mapper::savePartitions()
{
    partitionMap.savePartitionMap();
}

void openset::mapping::Mapper::serializeRoutes(cjson* doc)
{
    doc->setType(cjson::Types_e::ARRAY);

    for (const auto& r : routes)
    {
        auto item = doc->pushObject();
        // get the name from the names map
        item->set("name", names.find(r.first)->second);
        item->set("id", r.first);
        item->set("host", r.second.first);
        item->set("port", r.second.second);
    }
}

void openset::mapping::Mapper::deserializeRoutes(cjson* doc)
{
    if (!doc || doc->empty())
    {
        Logger::get().error("no cluster route provided.");
        return;
    }


    auto nodes = doc->getNodes();
    auto count = 0;

    for (auto n : nodes) // array of simple table names, nothing fancy
    {
        const auto nodeName = n->xPathString("/name", "");
        const auto nodeId = n->xPathInt("/id", 0);
        const auto ip = n->xPathString("/host", "");
        const auto port = n->xPathInt("/port", 0);

        if (!port || !nodeId || !ip.length())
            continue; // TODO - this is likely an error

        ++count;

        // create the route (or update it if it's not the same)
        addRoute(nodeName, nodeId, ip, port);
    }
}

void openset::mapping::Mapper::loadRoutes()
{

    if (globals::running->testMode)
        return;

    // see if it exists
    if (!openset::IO::File::FileExists(globals::running->path + "routes.json"))
    {
        saveRoutes();
        return;
    }

    cjson tableDoc(globals::running->path + "routes.json", cjson::Mode_e::file);

    deserializeRoutes(&tableDoc);

    Logger::get().info("cluster routes loaded.");
}


void openset::mapping::Mapper::saveRoutes()
{
    if (globals::running->testMode)
        return;

    cjson doc;

    serializeRoutes(&doc);

    cjson::toFile(globals::running->path + "routes.json", &doc);
}

void openset::mapping::Mapper::run() const
{
    Logger::get().info("cluster monitor created.");

    while (true)
    {
        ThreadSleep(1000);
        // TODO - lets run our pings in here
    }
}

void openset::mapping::Mapper::startRouter()
{
    // make a thread, call the `run` function, monitor nodes
    std::thread runner(&openset::mapping::Mapper::run, this);
    runner.detach();
}
