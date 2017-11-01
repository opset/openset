#pragma once

#include "common.h"
#include "querycommon.h"
#include "sba/sba.h"
#include "heapstack/heapstack.h"
#include "errors.h"

class cjson;

namespace openset
{

	namespace query
	{
		class Interpreter; 	// name spaced forward
	};

	namespace trigger
	{
		enum class triggerOn_e : int
		{
			changed_true
		};
	};

	namespace db // name spaced forwards
	{
		class Table;
		class Person;
		class TablePartitioned;
		class Attributes;
		class IndexBits;
		struct Attr_s;
	};

};

namespace std // hasher for triggerOn_e enum
{
	template<>
	struct hash<openset::trigger::triggerOn_e>
	{
		size_t operator()(const openset::trigger::triggerOn_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};
};


namespace openset
{
	namespace trigger
	{


		struct triggerMessage_s
		{
			int64_t stamp;
			int64_t id;
			char* uuid;
			char* message;

			triggerMessage_s():
				stamp(0), 
				id(0),
				uuid(nullptr),
				message(nullptr)
			{ }

			triggerMessage_s(int64_t triggerId, std::string triggerMessage, std::string uuidStr)
			{
				stamp = Now();
				id = triggerId;

				uuid = cast<char*>(PoolMem::getPool().getPtr(uuidStr.length() + 1));
				message = cast<char*>(PoolMem::getPool().getPtr(triggerMessage.length() + 1));

				strcpy(uuid, uuidStr.c_str());
				strcpy(message, triggerMessage.c_str());
			}

			triggerMessage_s(const triggerMessage_s &other):
				stamp(other.stamp),
				id(other.id)
			{
				uuid = cast<char*>(PoolMem::getPool().getPtr(strlen(other.uuid) + 1));
				message = cast<char*>(PoolMem::getPool().getPtr(strlen(other.message) + 1));

				strcpy(uuid, other.uuid);
				strcpy(message, other.message);
			}

			triggerMessage_s(triggerMessage_s&& other) noexcept
			{
				this->stamp = other.stamp;
				this->id = other.id;

				this->uuid = other.uuid;
				this->message = other.message;

				other.uuid = nullptr;
				other.message = nullptr;
			}

			~triggerMessage_s()
			{
				if (uuid)
				{
					PoolMem::getPool().freePtr(uuid);
					PoolMem::getPool().freePtr(message);
					uuid = nullptr;
					message = nullptr;
				}
			}
		};


		// String to Trigger Mode
		static const unordered_map<string, triggerOn_e> triggerModes = {
			{ "changed_true", triggerOn_e::changed_true }
		};

		struct triggerSettings_s
		{
			int64_t id;
			std::string entryFunction;
			int64_t entryFunctionHash;
			std::string name;
			std::string script;
			int configVersion;
			openset::query::Macro_s macros;
		};

		class Trigger
		{


		private:
			triggerSettings_s* settings;
			int lastConfigVersion; // class copy, used to check against settings version for reload

			openset::db::Table* table;
			openset::db::TablePartitioned* parts;

			openset::query::Macro_s macros; // this will be a copy of what is in the triggerSettings_S
			openset::query::Interpreter* interpreter;

			openset::db::Person* person;

			openset::db::Attr_s* attr;
			openset::db::IndexBits* bits;

			int64_t currentFunctionHash;

			int runCount;
			bool beforeState;
			bool inError;

		public:

			std::vector<triggerMessage_s> triggerQueue;
						
			explicit Trigger(triggerSettings_s* settings, openset::db::TablePartitioned* parts);
			~Trigger();

			static openset::errors::Error compileTrigger(
				openset::db::Table* table, 
				std::string name, 
				std::string script,
				openset::query::Macro_s &targetMacros);

			void init();

			void flushDirty();

			void mount(openset::db::Person* personPtr);

			void checkReload() 
			{
				if (lastConfigVersion != settings->configVersion)
					init();
			}
			
			void preInsertTest();
			void postInsertTest();

			bool runFunction(const int64_t functionHash);

			std::string getName() const
			{
				return settings->name;
			}

		};
	};
};