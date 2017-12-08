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

	try
	{
		globals::running = this;

		if (!openset::IO::File::FileExists(path + "openset.json"))
		{
			Logger::get().info("existing configuration available");
			existingConfig = true;
			// load the config version saved to disk for try restart
			configVersion = getExistingConfigVersion();
		}
	}
	catch (std::string e)
	{
		Logger::get().fatal("could not configure");
	}

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

void Config::save() const
{
	csLock lock(cs); // scoped lock

	openset::IO::Directory::mkdir(path);
	cjson doc;
	doc.set("node_id", nodeId);
	doc.set("partitions", partitionMax);
	doc.set("config_version", configVersion);
	doc.set("data_path", path + "data/");
	cjson::toFile(path + "openset.json", &doc, true);
}

int64_t Config::getExistingConfigVersion() const
{
	csLock lock(cs); // scoped lock
	cjson doc(path + "openset.json");
	return doc.xPathInt("/config_version", 0);
}

void Config::load()
{
	Logger::get().info("loading settings");

	csLock lock(cs); // scoped lock
	cjson doc(path + "openset.json");
	nodeId = doc.xPathInt("/node_id", 0);
	partitionMax = doc.xPathInt("partitions", 0);
	configVersion = doc.xPathInt("/config_version", Now());
	path = doc.xPathString("/data_path", "./tables");
}
