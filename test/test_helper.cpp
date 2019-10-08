#include "test_helper.h"

#include "testing.h"
#include "../lib/var/var.h"
#include "../src/database.h"
#include "../src/table.h"
#include "../src/asyncpool.h"
#include "../src/tablepartitioned.h"
#include "../src/queryparserosl.h"
#include "../src/queryinterpreter.h"

TestEngineContainer_s* TestScriptRunner(const std::string& tableName, const std::string& script, openset::query::Macro_s& queryMacros, const bool debug)
{
    const auto database = openset::globals::database;
    const auto table    = database->getTable(tableName);
    const auto parts    = table->getPartitionObjects(0, true); // partition zero for test

    openset::query::QueryParser p;

    // compile test script
    p.compileQuery(script, table->getColumns(), queryMacros, nullptr);

    if (debug)
        cout << openset::query::MacroDbg(queryMacros) << endl;

    ASSERT(p.error.inError() == false);

    auto engine = new TestEngineContainer_s(queryMacros);

    const auto personRaw = parts->people.getMakePerson("user1@test.com"); // get a user
    ASSERT(personRaw != nullptr);
    auto mappedColumns = engine->interpreter->getReferencedColumns();

    // MappedColumns? Why? Because the basic mapTable function (without a
    // columnList) maps all the columns in the table - which is what we want when
    // inserting or updating rows but means more processing and less data affinity
    // when performing queries
    Person person; // Person overlay for personRaw;
    person.mapTable(table.get(), 0, mappedColumns);
    person.mount(personRaw); // this tells the person object where the raw compressed data is
    person.prepare();        // this actually decompresses

    // this mounts the now decompressed data (in the person overlay)
    // into the interpreter
    engine->interpreter->mount(&person); // run it
    engine->interpreter->exec();

    //ASSERT(interpreter->error.inError() == false);
    return engine;
}

cjson ResultToJson(TestEngineContainer_s* engine)
{
    auto result = engine->interpreter->result;

    ASSERT(result->results.size() != 0);

    // we are going to sort the list, this is done for merging, but
    // being we have one partition in this test we won't actually be merging.
    result->makeSortedList();

    // the merger was made to merge a fancy result structure, we
    // are going to manually stuff our result into this
    std::vector<openset::result::ResultSet*> resultSets;

    // populate or vector of results, so we can merge
    //responseData.push_back(&res);
    resultSets.push_back(engine->interpreter->result);

    // this is the merging object, it merges results from multiple
    // partitions into a result that can serialized to JSON, or to
    // binary for distributed queries
    openset::result::ResultMuxDemux merger;

    // we are going to populate this
    cjson resultJson;

    // make some JSON
    merger.resultSetToJson(engine->interpreter->macros.vars.columnVars.size(), 1, resultSets, &resultJson);

    return resultJson;
}