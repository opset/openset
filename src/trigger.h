#pragma once

#include "common.h"
#include "querycommon.h"
#include "sba/sba.h"
#include "errors.h"

class cjson;

namespace openset
{
	namespace query
	{
		class Interpreter; 	// name spaced forward
	};

	namespace revent
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
	struct hash<openset::revent::triggerOn_e>
	{
		size_t operator()(const openset::revent::triggerOn_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};
};

namespace openset
{
	namespace revent
	{
    	struct triggerMessage_s
		{
			int64_t stamp;
			int64_t id;
			string uuid;
            string method;
			string message;

			triggerMessage_s():
				stamp(0), 
				id(0)
			{}

			triggerMessage_s(const int64_t reventId, std::string triggerMessage, std::string methodName, std::string uuidStr)
			{
				stamp = Now();
				id = reventId;

                uuid = std::move(uuidStr);
                method = std::move(methodName);
                message = std::move(triggerMessage);
			}

			triggerMessage_s(const triggerMessage_s &other):
				stamp(other.stamp),
				id(other.id)
			{
                uuid = other.uuid;
                method = other.method;
                message = other.message;
			}

			triggerMessage_s(triggerMessage_s&& other) noexcept
			{
				this->stamp = other.stamp;
				this->id = other.id;

                uuid = std::move(other.uuid);
                method = std::move(other.method);
                message = std::move(other.message);
			}

            ~triggerMessage_s()
            {}
		};

		// String to Trigger Mode
		static const unordered_map<string, triggerOn_e> triggerModes = {
			{ "changed_true", triggerOn_e::changed_true }
		};

		struct reventSettings_s
		{
			int64_t id;
			std::string entryFunction;
			int64_t entryFunctionHash;
			std::string name;
			std::string script;
			int configVersion;
			openset::query::Macro_s macros;
		};

		class Revent
		{
			reventSettings_s* settings;
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
						
			explicit Revent(reventSettings_s* settings, openset::db::TablePartitioned* parts);
			~Revent();

			static openset::errors::Error compileTriggers(
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

            bool emit(const std::string& methodName);

            bool runFunction(const int64_t functionHash);

			std::string getName() const
			{
				return settings->name;
			}

		};
	};
};