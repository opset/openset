#include "oloop_customer.h"
#include "http_serve.h"
#include "table.h"
#include "tablepartitioned.h"
#include "errors.h"
#include "customer.h"
#include "cjson/cjson.h"

using namespace openset::async;

OpenLoopCustomer::OpenLoopCustomer(
    Shuttle<int>* shuttle,
    const openset::db::Database::TablePtr table,
    const int64_t uuid) :
    OpenLoop(table->getName(), oloopPriority_e::realtime),
    shuttle(shuttle),
    table(table),
    uuid(uuid)
{}

void OpenLoopCustomer::prepare()
{}

bool OpenLoopCustomer::run()
{
    auto parts = table->getPartitionObjects(loop->partition, false );

    if (!parts)
    {
        suicide();
        return false;
    }

    const auto personData = parts->people.getCustomerByID(uuid);

    if (!personData) // no customer, not found
    {
        shuttle->reply(
            http::StatusCode::client_error_bad_request,
            openset::errors::Error{
            openset::errors::errorClass_e::query,
            openset::errors::errorCode_e::item_not_found,
            "customer could not be found"
        }.getErrorJSON()
        );
        suicide();
        return false;
    }

    db::Customer person; // Customer overlay for personRaw;
    if (!person.mapTable(table.get(), loop->partition)) // will throw in DEBUG if not called before mount
    {
        partitionRemoved();
        suicide();
        return false;
    }

    person.mount(personData);
    person.prepare(); // this actually decompressed the record and populates the grid

    auto json = person.getGrid()->toJSON();
    auto jsonString = cjson::stringify(&json);

    shuttle->reply(
        http::StatusCode::success_ok,
        &jsonString[0],
        jsonString.length());

    // were done
    suicide();
    return false;
}

void OpenLoopCustomer::partitionRemoved()
{
    shuttle->reply(
        http::StatusCode::client_error_bad_request,
        openset::errors::Error{
            openset::errors::errorClass_e::run_time,
            openset::errors::errorCode_e::partition_migrated,
            "please retry query"
        }.getErrorJSON()
    );
}
