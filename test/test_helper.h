#pragma once

#include "../src/queryinterpreter.h"
#include "../src/queryparser2.h"

struct TestEngineContainer_s
{
    openset::query::Interpreter* interpreter;
    openset::result::ResultSet resultSet;

    TestEngineContainer_s(openset::query::Macro_s& macros)
        : interpreter( new openset::query::Interpreter(macros)),
          resultSet(macros.vars.columnVars.size())
    {
        interpreter->setResultObject(&resultSet);
    }

    ~TestEngineContainer_s()
    {
        if (interpreter)
            delete interpreter;
    }

    openset::query::DebugLog& debugLog() const
    {
        return interpreter->debugLog;
    }
};

TestEngineContainer_s* TestScriptRunner(const std::string& tableName, const std::string& script, openset::query::Macro_s& queryMacros, const bool debug = false);
cjson resultToJson(TestEngineContainer_s* engine);
