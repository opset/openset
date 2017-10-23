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

Indexing::~Indexing()
{}

void Indexing::mount(Table* tablePtr, Macro_S& queryMacros, int partitionNumber, int stopAtBit)
{
	indexes.clear();
	table = tablePtr;
	macros = queryMacros;
	partition = partitionNumber;
	parts = table->getPartitionObjects(partition);
	stopBit = stopAtBit;

	// this will build all the indexes and store them 
	// in a vector of indexes using an std::pair of name and index
	bool countable;
	for (auto &p : queryMacros.indexes)
		indexes.emplace_back(p.first, buildIndex(p.second, countable), countable);
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

IndexBits Indexing::buildIndex(HintOpList &index, bool &countable)
{

	countable = true;

	auto maxLinId = parts->people.peopleCount();

	if (!stopBit)
	{
		IndexBits bits;
		bits.makeBits(maxLinId, 1);
		return bits;
	}

	/*
	getBits - this little helper function does a lot.
		- it looks up the column (by name) in the schema.
		- gets the actual column number from column info
		- gets all the attributes that match instruction.intValue considering
		  mode (EQ, NEQ, GT, LT, GTE, LTE)
		- OR all those attributes together and return the 
		  cumulative result
	*/
	auto getBits = [&](hintOp_s& instruction, Attributes::listMode_e mode) -> IndexBits
		{
			auto colInfo = table->getColumns()->getColumn(instruction.column);
			auto attrList = parts->attributes.getColumnValues(
				                     colInfo->idx, mode, instruction.intValue);

			IndexBits resultBits; // where our bits will all accumulate
			auto initialized = false;

			for (auto attr: attrList)
			{
				// get the bits
				auto bits = attr->getBits();			
				auto pop = bits->population(stopBit);

				if (initialized)
				{
					resultBits.opOr(*bits);
				}
				else
				{
					resultBits.opCopy(*bits);
					initialized = true;
				}

				// clean up them bits
				delete bits;
			}

			if (!initialized)
				resultBits.makeBits(64, 0);

			auto count = resultBits.population(stopBit);

			return resultBits;
		};


	// clean up any trailing NOPs
	while (index.size() && index.back().op == hintOp_e::PUSH_NOP)
		index.pop_back();

	stack<IndexBits> s;
	IndexBits left, right;

	auto count = 0;

	for (auto& instruction : index)
	{
		if (instruction.op == hintOp_e::PUSH_NOP ||
			instruction.op == hintOp_e::NST_BIT_AND ||
			instruction.op == hintOp_e::NST_BIT_OR )
		{
			countable = false;
			break;
		}
	}

	for (auto& instruction : index)
	{
		Columns::columns_s* colInfo;
		Attributes::AttrList attrList;

		switch (instruction.op)
		{
			case hintOp_e::UNSUPPORTED:
				break;
			case hintOp_e::PUSH_EQ:
				s.push(getBits(instruction, Attributes::listMode_e::EQ));
				++count;
				break;
			case hintOp_e::PUSH_NEQ:
				// getBits returns EQ, we doing NOT EQUAL, because all users not
				// having a value is different than all users who had another
				// value other than this one (which is what a list would
				// return) we are going to value that equal NONE
				if (instruction.numeric && instruction.intValue == NONE)
				{
					s.push(getBits(instruction, Attributes::listMode_e::EQ));
				}
				else
				{
					auto neqBits = getBits(instruction, Attributes::listMode_e::NEQ);
					neqBits.grow((stopBit / 64) + 1); // grow it to it's fullest size before we flip them all
					neqBits.opNot();
					s.push(neqBits);
				}
				++count;
				break;
			case hintOp_e::PUSH_GT:
				s.push(getBits(instruction, Attributes::listMode_e::GT));
				++count;
				break;
			case hintOp_e::PUSH_GTE:
				s.push(getBits(instruction, Attributes::listMode_e::GTE));
				++count;
				break;
			case hintOp_e::PUSH_LT:
				s.push(getBits(instruction, Attributes::listMode_e::LT));
				++count;
				break;
			case hintOp_e::PUSH_LTE:
				s.push(getBits(instruction, Attributes::listMode_e::LTE));
				++count;
				break;
			case hintOp_e::PUSH_PRESENT:
				s.push(getBits(instruction, Attributes::listMode_e::PRESENT));
				++count;
				break;
			case hintOp_e::PUSH_NOP:
				// these are dummy bits... they simply copy the end of the heap
				// and push it back onto the heap
				s.push(IndexBits{});
				s.top().placeHolder = true;
				break;
			case hintOp_e::NST_BIT_OR:
			case hintOp_e::BIT_OR:
				// pop two IndexBits objects off the stack
				right = s.top();
				s.pop();
				left = s.top();
				s.pop();

				if (right.placeHolder && left.placeHolder)
					s.push(left);
				else if (right.placeHolder)
					s.push(left);
				else if (left.placeHolder)
					s.push(right);
				else
				{
					// OR the right into the left and push it back onto
					// the stack
					left.opOr(right);
					s.push(left);
				}
				break;
			case hintOp_e::NST_BIT_AND:
			case hintOp_e::BIT_AND:
				// pop two IndexBits objects off the stack
				right = s.top();
				s.pop();
				left = s.top();
				s.pop();

				if (right.placeHolder && left.placeHolder)
					s.push(left);
				else if (right.placeHolder)
					s.push(left);
				else if (left.placeHolder)
					s.push(right);
				else
				{
					// AND the right into the left and push it back onto
					// the stack
					left.opAnd(right);
					s.push(left);
				}
				break;
			default:
				s.push(IndexBits{});
				s.top().placeHolder = true;
				// TODO some error handling here
				break;

		}
	}

	// *.* query likely (like rows: or all NOPs)
	if (!s.size() || !count)
	{
		IndexBits bits;
		bits.makeBits(maxLinId, 1);
		countable = true;
		return bits;
	}

	auto res = s.top();

	if (res.placeHolder)
		cout << "fucked" << endl;

	s.pop();

	/*
	 *
	stringstream ss;
	ss << "partition: " << partition << "  stop: " << stopBit << "  max lid: " << maxLinId << "  pop: " << res.population(stopBit);
	ss << " " << IndexBits::debugBits(res, maxLinId) << endl;

	if (partition == 3) // 9 
		cout << ss.str() << endl;

	*/
	res.grow((stopBit / 64) + 1);
	return res;
}
