#include <regex>
#include "common.h"
#include "rpc.h"
#include "oloop_column.h"
#include "database.h"
#include "result.h"
#include "internoderouter.h"
#include "http_serve.h"

using namespace std;
using namespace openset::comms;
using namespace openset::async;
using namespace openset::comms;
using namespace openset::db;
using namespace openset::result;

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
