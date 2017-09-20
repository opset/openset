#pragma once

#include <fstream>
#include <iostream>
#include <atomic>
#include <thread>

#include "heapstack/heapstack.h"

#include "common.h"
#include "table.h"
#include "tablepartitioned.h"
#include "attributes.h"


namespace openset
{

	namespace Groom
	{

		enum class serializeMode_e : int
		{
			replication,
			checkpoint
		};

		class SerializeOut
		{
		public:

			enum class step_e: int
			{
				begin = 0,
				attributes = 1,
				blob = 2,
				people = 3,
				complete = 4
			};

#pragma pack(push,1)
			struct Header_s
			{
				char db[256];
				char table[256];
				int partition;
				int64_t attributeBytes;
				int64_t attributeBlobBytes;
				int64_t peopleBytes;

				explicit Header_s(int partitionNumber)
				{
					memset(this, 0, sizeof(Header_s));
					partition = partitionNumber;
				}

				void setDbName(std::string name)
				{
					strcpy(db, name.c_str());
				}

				void setTables(std::string name)
				{
					strcpy(table, name.c_str());
				}

				void setAttributeBytes(int64_t bytes)
				{
					attributeBytes = bytes;
				}

				void setBlobBytes(int64_t bytes)
				{
					attributeBlobBytes = bytes;
				}

				void setPeopleBytes(int64_t bytes)
				{
					peopleBytes = bytes;
				}

			};
#pragma pack(pop)

			openset::db::Table* table;
			openset::db::TablePartitioned* parts;
			int partition;
			
			std::fstream file;
			serializeMode_e mode;

			HeapStack mem;
			Header_s header;

			step_e step;

			std::string fileName;
			std::atomic<bool> inAsync;

			SerializeOut(openset::db::Table* table, int partition, serializeMode_e mode ): 
				table(table), 
				partition(partition),
				mode(mode),
				header(partition),
				step(step_e::begin),
				inAsync(false)
			{
				parts = table->getPartitionObjects(partition);
			}

			~SerializeOut()
			{}

			void begin()
			{

				// open for write, open in binary and over write any existing
				file.open(fileName, std::fstream::out | std::fstream::binary | std::fstream::trunc);

				if (file.fail())
				{
					// TODO - handle error
					return;
				}

				inAsync = true;

				// make a worker thread so we can write
				// this back without blocking
				std::thread asyncWriteback([this]()
				{
					// write the header, we will be back for this
					file.write(recast<char*>(&header), sizeof(Header_s));

					step = step_e::attributes;
					inAsync = false;
				});

				asyncWriteback.detach();
			}

			void attributes()
			{				

				// reset mem
				mem.reset();

				// write placeholder header here, we will overwrite this 
				// when the entire file has been created.
				mem.newPtr(sizeof(Header_s));

				// first up are attributes
				parts->attributes.serialize(&mem);

				inAsync = true;

				// make a worker thread so we can write
				// this back without blocking
				std::thread asyncWriteback([this]()
				{
					auto block = this->mem.firstBlock();

					while (block)
					{
						file.write(block->data, block->endOffset);
						block = block->nextBlock;
					}

					header.attributeBytes = mem.getBytes();				
					step = step_e::blob;

					inAsync = false;
				});

				asyncWriteback.detach();
			}

			void blob()
			{				
				this->step = step_e::people;
			}

			void people()
			{
				this->step = step_e::complete;
			}

			/* run() 
			 *
			 * This run function is to be called from an oloop object.
			 * 
			 * It calls functions which bundle data (very quickly) and
			 * then through a worker thread, write the data to disk. 
			 * 
			 * When a write back thread is complete, it will move the
			 * next step.
			 * 
			 * the step will be at `complete` when it is done, and 
			 * run will return `true`
			 */
			bool run()
			{

				// if we are in one of our worker threads, there is nothing
				// new to do.
				if (inAsync)
					return false;

				switch (step)
				{
					case step_e::begin: 
						begin();
						break;
					case step_e::attributes: 
						attributes();
						break;
					case step_e::blob: 
						break;
					case step_e::people: 
						break;
					case step_e::complete: 
						return true;
						break;
				}

				return false;
			}
			

		};

	};

};