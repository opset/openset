#include <regex>
#include "common.h"
#include "rpc.h"
#include "oloop_column.h"
#include "database.h"
#include "result.h"
#include "internoderouter.h"
#include "http_serve.h"

//#include "trigger.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

/*
void Feed::onSub(const openset::web::MessagePtr message, const RpcMapping& matches)
{
	if (!partitions->partitionMax || !partitions->running)
	{
		// TODO - error cluster not ready
		message->reply("[]");
		return;
	}

	if (message->clientConnection)
		message->clientConnection->holdDropped = true; // we don't recycle these, we want check for the error

	// this type of query will wait until there is a message
	auto messageLambda = [message, partitions, database]() noexcept
	{
		auto msgText = message->toString();
		cjson request(msgText, msgText.length());

		auto tableName = request.xPathString("/table", "");
		auto triggerName = request.xPathString("/trigger", "");
		auto subName = request.xPathString("/subscription", "");
		auto holdTime = request.xPathInt("/hold", 10'800'000); // 3 hours
		auto max = request.xPathInt("/max", 500);

		auto table = database->getTable(tableName);

		if (!table)
		{
			// TODO - table doesn't exist
			if (message->clientConnection)
				message->clientConnection->holdDropped = false; // we don't recycle these, we want check for the error

			message->reply("[]");
			return;
		}
		
		auto messages = table->getMessages();

		messages->registerSubscriber(triggerName, subName, holdTime);
	    		
		while (!messages->size(triggerName, subName))
		{
			ThreadSleep(100);

			if (message->clientConnection && 
				message->clientConnection->dropped)
			{
				Logger::get().error("subscriber '" + subName + "' on table '" + tableName + "' connection lost.");
				message->clientConnection->holdDropped = false;
				return;
			}
			// TODO - check for disconnect!
		}

		auto list = messages->pop(triggerName, subName, max);

		cjson response;

		auto messageArray = response.setArray("messages");

		for (auto m : list)
		{
			auto msg = messageArray->pushObject();
			msg->set("stamp", m.stamp);
			msg->set("uid", m.uuid);
			msg->set("message", m.message);
		}

		response.set("remaining", messages->size(triggerName, subName));
		
		message->reply(cjson::Stringify(&response, true));
	};

	// start the subscriber thread
	std::thread messageThread(messageLambda);
	messageThread.detach();

}
*/


void openset::comms::Dispatch(const openset::web::MessagePtr message)
{
	const auto path = message->getPath();

	for (auto& item: MatchList)
	{		
        // the most beautifullest thing...
		const auto& [method, rx, handler, packing] = item;

		if (std::smatch matches; message->getMethod() == method && regex_match(path, matches, rx))
		{
			RpcMapping matchMap;

			for (auto &p : packing)
				if (p.first < matches.size())
					matchMap[p.second] = matches[p.first];

			handler(message, matchMap);
			return;
		}
	};

	message->reply(http::StatusCode::client_error_bad_request, "rpc not found");
}
