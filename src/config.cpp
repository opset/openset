#include "config.h"
#include "file/directory.h"
#include "file/file.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sys/stat.h>

#include "common.h"
#include "logger.h"

using namespace openset::config;

namespace openset
{
	namespace globals
	{
		Config* running;
	}
}
/*
-------------------------------------------------
	ConfigSettings
	- loads the list of known tables
	- get - gets named table JSON object
	- commit - writes back a change
-------------------------------------------------
*/
Config::Config(openset::config::CommandlineArgs args) :
	host(args.hostLocal),
	port(args.portLocal),
	hostExternal(args.hostExternal),
	portExternal(args.portExternal)
{
	globals::running = this;
	setRootPath(args.path);
}

void Config::setState(NodeState_e state)
{
	this->state = state;

	switch (state)
	{
		case NodeState_e::ready_wait:
			Logger::get().info("node ready and waiting.");
			break;
		case NodeState_e::resume_wait:
			Logger::get().info("node wait waiting to resume.");
			break;
		case NodeState_e::active:
			Logger::get().info("node is active.");
			break;
	}
}

void Config::setRootPath(string path)
{
	this->path = (path.back() != '/') ? path + "/" : path;
    globals::running = this;
}

void Config::updateConfigVersion(int64_t remoteConfigId)
{
	csLock lock(cs);
	configVersion = remoteConfigId;
}

int64_t Config::updateConfigVersion()
{
	csLock lock(cs);
	configVersion = Now();
	return configVersion;
}

void Config::setNodeName(std::string name)
{
	nodeName = name;
	nodeId = MakeHash(name);
}

