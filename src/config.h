#pragma once 

#include "common.h"
#include "cjson/cjson.h"
#include "threads/locks.h"

#include <vector>
#include <unordered_map>

// See initConfig() factory at the end of this file

namespace openset
{
	namespace config
	{
		enum class nodeState_e : int
		{
			ready_wait = 0,
			resume_wait = 1,
			active = 2
		};

		class CommandlineArgs
		{
		public:
			std::string hostLocal = "0.0.0.0";
			int portLocal = 2020;
			std::string hostExternal = "";
			int portExternal = 0;
			std::string path = "./";

			void fix()
			{
				if (!hostExternal.length())
				{
					char hostName[256] = { 0 };
					size_t len = 255;
					if (gethostname(hostName, len) == -1)
					{
						cout << "! could not get hostname" << endl << endl;
						exit(1);
					}
					hostExternal = hostName;
					Logger::get().info('*', "external host defaulting to hostname: '" + hostExternal + "'");
				}

				if (!portExternal)
					portExternal = portLocal;
			}
		};

		class Config
		{
		public:
			mutable CriticalSection cs;
			string path{ "./" };
			string host{ "0.0.0.0" };
			int port{ 1022 };

			string hostExternal{ "127.0.0.1" };
			int portExternal{ 1022 };

			int64_t partitionMax{ 0 }; // server will be in "waiting" mode if no partitions
			int64_t configVersion{ 0 };

			string nodeName{ "empty" };
			int64_t nodeId{ 0 };

			nodeState_e state{ nodeState_e::ready_wait };
			bool testMode{ false };
			bool existingConfig{ false };
			
			explicit Config(openset::config::CommandlineArgs args);

			void setState(nodeState_e state);

			void setRootPath(string path);;

			// update (and returns) configVersion
			void updateConfigVersion(int64_t remoteConfigId);

			// update (and returns) configVersion
			int64_t updateConfigVersion();

			void setNodeName(std::string name);

			int64_t getExistingConfigVersion() const;
			void save() const;
			void load();
		};
	}

	namespace globals
	{
		extern openset::config::Config* running;
	}
}

