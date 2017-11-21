#pragma once

#include "common.h"
#include <functional>
#include <thread>
#include <vector>
#include "threads/locks.h"
#include "http_serve.h"

/*
	Shuttles - classes that fit between working cells
		and uvConnections. 
		
		When a cell is created from a communications event (uvserver)
		that cell should have a shuttle attached to it.

		The shuttle can be derived to create more shuttle types
		(like the ShuttleMulti). 

		When a shuttle has completed it's task, it will delete itself
		by calling release(). If a cell is not going to respond, then
		manually releasing the shuttle might be necessary.		
*/

namespace openset
{
	namespace async
	{
		// structure to store comms data coming in from
		// partitions
		template <typename response_t>
		struct response_s
		{
			int32_t code;
			response_t data;

			response_s(): 
				code(0) 
			{}

			response_s(const int32_t resultCode, response_t resultData) :
				code(resultCode),
				data(std::move(resultData))
			{}

			response_s(response_s&& other) noexcept:
				code(other.code),
				data(std::move(other.data))
			{}
		};

		template <typename response_t>
		class Shuttle
		{
		public:

			openset::web::MessagePtr message;

			explicit Shuttle(openset::web::MessagePtr message) :
				message(message)
			{ }

			virtual ~Shuttle()
			{ }

			void release() const
			{
				delete this;
			}

			/**
			 * \brief 
			 * \param messageString 
			 */
			virtual void reply(const http::StatusCode status, const string messageString)
			{
				message->reply(status, &messageString[0], messageString.length());
				release();
			}

			virtual void reply(const http::StatusCode status, const char* data, const int64_t length)
			{
				message->reply(status, data, length);
				// Shuttles are always pointers, they must delete themselves
				release();
			}
		};

		/*
			ShuttleLambda - allows multiple cells to respond to a 
				uvConnection.

				overrides (final) the pointer version of respond. The
				string version will call this version.

				respond will store the responses until as many are 
				gathered as it was told it would receive (constructor).

				A callback function will be passed the results the comms
				object and a destructor function.

				Lambda is called synchronously, so if it is long running it will
				will block the calling thread (the loop that completed the
				the distributed job last). 

				Note: The lambda will not have lockless access to the data
					  within the partitions, all jobs that need access to data
		              should acquire it within the loop they are running 
		              prior to calling the lambda
		*/
		template <typename response_t>
		class ShuttleLambda : public Shuttle<response_t>
		{
		public:
			// vector of responses from partitions			

			vector<openset::async::response_s<response_t>> responses;
			// guard for our responses
			CriticalSection responseLock;
			// how many responses we will get (should always = partitions)
			int32_t partitionCount;
			// our callback
			function<void(vector<openset::async::response_s<response_t>>&, openset::web::MessagePtr, voidfunc)> done_cb;

			/*
			This is a little ugly, but essentially you can call back
			to a function that takes three params, it can be a regular
			static function, or more probably a lambda.

			The lambda would look like this:

				[]( vector<OpenSet::async::response_s<YOUR_TYPE>> responses,
				    OpenSet::server::uvConnection* conn,
				    function<void ()> release ) 
				{
					// do something with the data
					conn->respond(0,"some JSON");
					release();
				}

			*/
			ShuttleLambda(
				openset::web::MessagePtr message,
				const int32_t partitions,
				function<void(
					vector<openset::async::response_s<response_t>>&,
					openset::web::MessagePtr,
					voidfunc)> OnProcessResponses):
				Shuttle<response_t>(message),
				partitionCount(partitions),
				done_cb(OnProcessResponses)
			{}

			~ShuttleLambda() override
			{}

			// gather responses, make this so it cannot be overridden
			virtual void reply(int32_t code, response_t&& data)
			{
				bool complete;
				{
					csLock lock(responseLock);
					responses.emplace_back(code, std::move(data)); // so cool	
					complete = (responses.size() == partitionCount);
				}

				if (complete) // outside of lock
					processResponses();
			}

			virtual void processResponses()
			{
				done_cb(
					this->responses,
					this->message,
					[&](){
						this->release();
					});
				// Shuttles are always pointers, they must delete themselves
			}
		};
	};

	/*
	 * ShuttleLambdaAysc has the same limits and requirements as ShuttleLambda
	 *    but calls the completion callback in an detached thread.
	 * 
	 *    Starting a thread is marginally expensive, so, this should only be
	 *    used when processing a large response (i.e. lots of sorting, 
	 *    serialization, etc), where the benefit of firing up a worker thread
	 *    outweighs the cost.
	 *    
	 *    Note: This version will not block the main async loops.
	 */

	template <typename response_t>
	class ShuttleLambdaAsync: public async::ShuttleLambda<response_t>
	{
	public:

		ShuttleLambdaAsync(
			openset::web::MessagePtr message,
			const int32_t partitions,
			const function<void(vector<openset::async::response_s<response_t>>&, openset::web::MessagePtr, function<void()>)>& onProcessResponses):
			async::ShuttleLambda<response_t>(message, partitions, onProcessResponses)
		{}

		virtual ~ShuttleLambdaAsync()
		{}

		void processResponses() override
		{
			auto threadLambda = [this]() noexcept
				{
					this->done_cb(
						this->responses,
						this->message,
						[&]() { this->release(); } // call back passed destructor
					);
					// Shuttles are always pointers, they must delete themselves
				};

			std::thread lambdaThread(threadLambda);
			lambdaThread.detach();
		}
	};
};
