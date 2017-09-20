#pragma once

#include <atomic>
#include <deque>
#include <unordered_map>
#include <condition_variable>

#include "internodecommon.h"
#include "threads/locks.h"

// add the newer preferred posix "inet_pton" function to windows 
// otherwise we are stuck with the exact same function 
// but with a silly name "inetPton"
#ifdef _MSC_VER
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netdb.h>
#endif

#include "internodemessage.h"

namespace openset
{

	namespace mapping
	{

		static const char* pingBuffer = "{\"ping\":true}";

		class DNS
		{
		private:
			CriticalSection cs;

			struct CacheEntry
			{
				std::string ip;
				int64_t lastRefresh;
			};
			unordered_map<std::string, CacheEntry> map;
		public:
			
			void purgeDNS()
			{
				csLock lock(cs);
				map.clear();
			}

			void remove(std::string host)
			{
				// when an error happens we purge the entry to
				// force a fresh DNS lookup
				csLock lock(cs);
				auto iter = map.find(host);
				if (iter != map.end())
					map.erase(iter);
			}

			bool lookup(std::string host, int port, std::string &ip)
			{

				// pull it from the cache if we can
				{ 
					csLock lock(cs);
					auto iter = map.find(host);
					if (iter != map.end())
					{
						ip = iter->second.ip;
						return true;
					}
				}

				ip = "";

				// check to see if host is actually an ip address
				struct sockaddr_in addr;
				if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) == 1) // not an error
				{
					ip = host;
					return true;
				}
				
				struct addrinfo hints, *servinfo;
				int rv;

				memset(&hints, 0, sizeof hints);
				// specify what type of entry we are looking for
				hints.ai_family = AF_INET; 
				hints.ai_socktype = SOCK_STREAM;

				// this will get us a list of IPs on that host that match our hints
				if ((rv = getaddrinfo(host.c_str(), nullptr, &hints, &servinfo)) != 0)
				{
					Logger::get().info('!', "could not resolve host '" + host + "'");
					return false;
				}

				addrinfo* infoPtr;
				SOCKET sock;
				// loop through our IP list looking for one that answers the phone
				for (infoPtr = servinfo; infoPtr != nullptr; infoPtr = infoPtr->ai_next)
				{
					if ((sock = socket(infoPtr->ai_family, infoPtr->ai_socktype, 0)) == -1)
						continue;

					reinterpret_cast<struct sockaddr_in*>(infoPtr->ai_addr)->sin_port = htons(port);
					
					if (connect(sock, infoPtr->ai_addr, static_cast<int>(infoPtr->ai_addrlen)) == 0)
					{
#ifdef _MSC_VER
						closesocket(sock);
#else
						::close(sock);
#endif
						break;
					}
				}

				if (infoPtr)
				{
					char ipBuffer[256] = {};
					size_t length = 255;
					getnameinfo(infoPtr->ai_addr, static_cast<socklen_t>(infoPtr->ai_addrlen), ipBuffer, static_cast<socklen_t>(length), nullptr, 0, NI_NUMERICHOST);
					ip = ipBuffer;

					csLock lock(cs);
					map.emplace(host, CacheEntry{ ip, Now() });
				}

				freeaddrinfo(servinfo); // leak not
				return ip.length();
			}

		};
				
		class OutboundClient
		{
		private:
			CriticalSection cs;
			mutex queueLock;
			condition_variable queueReady;

			bool isDirect;

			int64_t routingTo;
			std::string host;
			int32_t port;

			SOCKET sock;
			bool connected;

			int64_t lastRx;

		public:

			bool isLocalLoop;
			bool inDestroy;
			bool isDestroyed;

			atomic<int> backlogSize;
			std::deque<openset::comms::Message*> backlog;

			OutboundClient(int64_t destRoute, std::string host, int32_t port, bool direct = false);
			void request(openset::comms::Message* message);
			void teardown();

			bool isOpen() const
			{
				return connected;
			}

			int64_t getRoute() const
			{
				return routingTo;
			}

			std::string getHost() const
			{
				return host;
			}

			int32_t getPort() const
			{
				return port;
			}

			bool isDead() const;

			bool openConnection();
			void closeConnection();
			void idleConnection();
			int32_t directRequest(comms::RouteHeader_s routing, const char* buffer);
			// returns a pointer to a char buffer, remember to delete[] the buffer
			comms::RouteHeader_s waitDirectResponse(char* &data, int64_t toSeconds = 0);

		private:

			void runLocalLoop();
			void runRemote();
			void startRoute();
		};

	};
};

extern openset::mapping::DNS dnsCache;
