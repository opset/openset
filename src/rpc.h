#pragma once

#include "shuttle.h"
#include "asyncpool.h"
#include "database.h"

/*
	Control - This class is responsible for mapping
		communications from the usvserver to jobs (Cells)
		within the DB.

	This is basically the big sandwich between three different
	parts of the database:

	1. Comms (OpenSet::server)
	2. Multitasking (OpenSet::async)
	3. DB Engine (OpenSet::db)
    4. Triggers (OpenSet::trigger)

	These classes ensure that when comms happen, cells are created
	in the multitasking engine for the correct partitions and that
	DB engine objects are mapped to these cells, and that partition
	data is created(*). 

	Most of this work is done with cell factories and shuttle objects. 
	Cells are assigned to loops which are assigned to partitions to provide
	thread separation. Shuttles are used to gather results, and relay
	responses back down the originating connection (comms).

	* The database mirrors the concept of partitions with the multi-tasking
	engine. Partition X in one is also Partition X in another when it comes
	to thread isolation. However, functionally they are completely separate.

*/

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::db;

namespace openset
{
	namespace comms
	{
		class Internode
		{
		public:
			static void error(
				openset::errors::Error error,
				cjson* response);

			static void onMessage(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);

			static void initConfigureNode(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void isClusterMember(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void nodeAdd(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void transfer(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void mapChange(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

		};

		class Admin
		{
		public:
			static void error(
				openset::errors::Error error,
				cjson* response);

			static void onMessage(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);

			static void initCluster(
				Database* database,
				AsyncPool* partitions, 
				cjson* request, 
				cjson* response);

			static void inviteNode(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void createTable(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void describeTable(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void addColumn(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void dropColumn(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void setTrigger(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void describeTriggers(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

			static void dropTrigger(
				Database* database,
				AsyncPool* partitions,
				cjson* request,
				cjson* response);

		};

		class Query
		{
		public:
			static void error(
				openset::errors::Error error,
				cjson* response);

			static void onMessage(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);

			static void onQuery(
				Database* database,
				AsyncPool* partitions, 
				const bool isFork,
				cjson* request,
				comms::Message* message);

			static void onCount(
				Database* database,
				AsyncPool* partitions,
				const bool isFork,
				cjson* request,
				comms::Message* message);

			/*			
			static void onPerson(
				Database* database,
				AsyncPool* partitions,
				const bool isFork,
				cjson* request,
				comms::Message* message);
			*/
		};

		class Insert
		{
		public:
			static void onInsert(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);
		};

		class Feed
		{
		public:
			static void onSub(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);
		};

		class InternodeXfer
		{
		public:
			static void onXfer(
				Database* database,
				AsyncPool* partitions,
				comms::Message* message);
		};

	};
};
