#include "queryindexing.h"
#include "tablepartitioned.h"
#include <sstream>

using namespace openset::query;
using namespace openset::db;

openset::query::Indexing::Indexing() :
    table(nullptr),
    parts(nullptr),
    partition(-1),
    stopBit(0)
{}

Indexing::~Indexing() = default;

void Indexing::mount(Table* tablePtr, Macro_s& queryMacros, int partitionNumber, int stopAtBit)
{
    indexes.clear();
    table = tablePtr;
    macros = queryMacros;
    partition = partitionNumber;
    parts = table->getPartitionObjects(partition, false);
    stopBit = stopAtBit;

    // this will build all the indexes and store them
    // in a vector of indexes using an std::pair of name and index
    for (auto &p : queryMacros.indexes)
    {
        const auto index = buildIndex(p.second, queryMacros.indexIsCountable);
        indexes.emplace_back(p.first, index, queryMacros.indexIsCountable);
    }
}

// returns an index by name
openset::db::IndexBits* Indexing::getIndex(std::string name, bool &countable)
{
    for (auto &idx:indexes)
    {
        if (std::get<0>(idx) == name)
        {
            countable = std::get<2>(idx);
            return &std::get<1>(idx);
        }
    }

    return nullptr;
}

/*
 Mode is ListMode_e from Attributes class - this enumerates a property
and returns values that match the condition.

In getBits we take the last item on the stack and apply all matching indexes to
the bits in the stack entry.
 */
openset::db::IndexBits Indexing::compositeBits(Attributes::listMode_e mode)
{

    auto& entry = stack.back();

    const auto propInfo = table->getProperties()->getProperty(entry.columnName);

    // if the value side is NONE we go check for presence

    auto negate = false;

    if (mode == Attributes::listMode_e::EQ && entry.hash == NONE)
    {
        mode = Attributes::listMode_e::PRESENT;
        negate = true;
    }
    else if (mode == Attributes::listMode_e::NEQ)
    {
        if (entry.hash == NONE)
            mode = Attributes::listMode_e::PRESENT; // != NONE -- same as equals anything
        else
            negate = true; // != VAL -- anything other than VAL
    }

    auto attrList = parts->attributes.getPropertyValues(propInfo->idx, mode, entry.hash);

    auto& resultBits = entry.bits; // where our bits will all accumulate
    resultBits.reset();
    auto initialized = false;

    for (auto attr: attrList)
    {
        // get the bits
        const auto workBits = attr->getBits();

        if (initialized)
        {
            resultBits.opOr(*workBits);
        }
        else
        {
            resultBits.opCopy(*workBits);
            initialized = true;
        }

        // clean up them bits
        delete workBits;
    }

    if (!initialized)
        resultBits.makeBits(64, 0);

    if (negate)
    {
        resultBits.grow((stopBit / 64) + 1); // grow it to it's fullest size before we flip them all
        resultBits.opNot();
    }

    return resultBits;
};

/*
PSH_TBL        | @fruit
PSH_VAL        | banana
EQ             |
PSH_TBL        | @fruit
PSH_VAL        | donkey
EQ             |
AND            |
PSH_TBL        | @fruit
PSH_VAL        | banana
EQ             |
PSH_TBL        | @fruit
PSH_VAL        | pear
EQ             |
AND            |
PSH_TBL        | @fruit
PSH_VAL        | banana
EQ             |
PSH_TBL        | @fruit
PSH_VAL        | pear
NEQ            |
AND            |
PSH_TBL        | @fruit
PSH_VAL        | banana
EQ             |
OR             |
OR             |
OR             |
 */
IndexBits Indexing::buildIndex(HintOpList &index, bool countable)
{

    struct IndexStack_s
    {
        IndexBits bits;

    };

    const auto maxLinId = parts->people.customerCount();

    if (!stopBit)
    {
        // fix
        countable = false;
        IndexBits bits;
        bits.makeBits(maxLinId, 1);
        return bits;
    }

    std::string columnName;

    auto count = 0;

    for (auto &op : index)
    {
        switch (op.op)
        {
        case HintOp_e::UNSUPPORTED: break;
        case HintOp_e::EQ:
            compositeBits(Attributes::listMode_e::EQ);
            ++count;
            break;
        case HintOp_e::NEQ:
            compositeBits(Attributes::listMode_e::NEQ);
            ++count;
            break;
        case HintOp_e::GT:
            compositeBits(Attributes::listMode_e::GT);
            ++count;
            break;
        case HintOp_e::GTE:
            compositeBits(Attributes::listMode_e::GTE);
            ++count;
            break;
        case HintOp_e::LT:
            compositeBits(Attributes::listMode_e::LT);
            ++count;
            break;
        case HintOp_e::LTE:
            compositeBits(Attributes::listMode_e::LTE);
            ++count;
            break;
        case HintOp_e::PUSH_VAL:
            if (!columnName.length())
            {
                // THROW
            }
            stack.emplace_back(columnName, op.value, op.hash);
            break;
        case HintOp_e::PUSH_TBL:
            columnName = op.value.getString();
            break;
        case HintOp_e::BIT_OR:
        {
            auto left = stack.back().bits;
            stack.pop_back();
            auto right = stack.back().bits;
            stack.pop_back();

            left.opOr(right);
                stack.emplace_back(left);

            ++count;
        }
            break;
        case HintOp_e::BIT_AND:
        {
            auto left = stack.back().bits;
            stack.pop_back();
            auto right = stack.back().bits;
            stack.pop_back();

            left.opAnd(right);
                stack.emplace_back(left);

            ++count;
        }
            break;
        default: ;
        }
    }

    // No Index Hints?
    if (!stack.size() || !count)
    {
        IndexBits bits;
        bits.makeBits(maxLinId, 1);
        countable = false;
        return bits;
    }

    auto res = stack.back().bits;
    res.grow((stopBit / 64) + 1);
    return res;

}
