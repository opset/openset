#include "http_cli.h"

std::string openset::web::Rest::makeParams(const QueryParams params) const
{
	std::string result;

	// encode params
	auto first = true;
	for (auto& p : params)
	{
		if (first)
		{
			result += "?";
			first = false;
		}
		else
		{
			result += "&";
		}

		result += p.first + "=" + http::Percent::encode(p.second);
	}

	return std::move(result);
}

void openset::web::Rest::request(const std::string method, const std::string path, const QueryParams params,
                                 const char* payload, const size_t length, RestCbJson cb)
{
	csLock lock(cs);

	stringstream buffer(stringstream::in | stringstream::out | stringstream::binary);
	if (payload && length)
		buffer.write(payload, length);

	const auto url = path + makeParams(params);

	client.request(method, url, buffer, {},
	               [this, cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
	               {
		               auto length = response->content.size();
		               auto data = static_cast<char*>(PoolMem::getPool().getPtr(length));
		               response->content.read(data, length);

		               cb(ec ? true : false, cjson(std::string{data, length}, length));

		               PoolMem::getPool().freePtr(data);
	               });

	client.io_service->reset();
	client.io_service->run();
}

void openset::web::Rest::request(const std::string method, const std::string path, const QueryParams params,
                                 const char* payload, const size_t length, RestCbBin cb)
{
	csLock lock(cs);

	stringstream buffer(stringstream::in | stringstream::out | stringstream::binary);
	if (payload && length)
		buffer.write(payload, length);

	const auto url = path + makeParams(params);

	client.request(method, url, buffer, {},
	               [this, cb](shared_ptr<HttpClient::Response> response, const SimpleWeb::error_code& ec)
	               {
		               auto length = response->content.size();
		               char* data = length ? static_cast<char*>(PoolMem::getPool().getPtr(length)) : nullptr;
		               response->content.read(data, length);
		               cb(ec ? true : false, data, length);
	               });

	client.io_service->reset();
	client.io_service->run();
}
