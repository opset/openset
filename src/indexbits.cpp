#include "indexbits.h"
#include "dbtypes.h"
#include "sba/sba.h"
#include "lz4.h"
#include <cassert>

using namespace std;
using namespace openset::db;

IndexBits::IndexBits():
	bits(nullptr), ints(0), placeHolder(false)
{}

// move constructor
IndexBits::IndexBits(IndexBits&& source) noexcept
{
	bits = source.bits;
	ints = source.ints;
	placeHolder = source.placeHolder;
	source.bits = nullptr;
	source.ints = 0;
	source.placeHolder = false;
}

// copy constructor
IndexBits::IndexBits(const IndexBits& source) :
	bits(nullptr), ints(0), placeHolder(false)
{
	opCopy(source);
}

IndexBits::IndexBits(IndexBits* source) :
	bits(nullptr), ints(0), placeHolder(false)
{
	opCopy(*source);
}

IndexBits::~IndexBits()
{
	reset();
}

// move assignment operator
IndexBits& IndexBits::operator=(IndexBits&& other) noexcept
{
	if (this != &other)
	{
		reset();
		bits = other.bits;
		ints = other.ints;
		placeHolder = other.placeHolder;
		other.bits = nullptr;
		other.ints = 0;
		other.placeHolder = false;
	}
	return *this;
}

// copy assignment operator
IndexBits& IndexBits::operator=(const IndexBits& other)
{
	if (this != &other)
		opCopy(other);
	return *this;
}

void IndexBits::reset()
{
	if (bits)
		PoolMem::getPool().freePtr(bits);
	bits = nullptr;
	ints = 0;
	placeHolder = false;
}

void IndexBits::makeBits(int64_t index, int state)
{
	reset();

	auto pos = index >> 6ULL; // divide by 8

	if (pos >= ints) // is our buffer big enough?
		grow(pos + 1);

	memset(bits, (state) ? 0xff : 0x00, ints * 8);

	// if we are 1 filling these bits, we must
	// set every bit after index to zero 
	if (state)
	{
		// zero the rest of the bits in the last int64
		auto lastBit = (pos + 1) * 64LL;
		for (auto i = index; i < lastBit; i++)
			this->bitClear(i);
	}
}

void IndexBits::mount(char* compressedData, int32_t Ints)
{
	reset();

	if (!Ints) // if this is empty then make a buffer
	{
		ints = 1; // LZ4 compressor uses 9 bytes with a bit set with one INT
		auto bytes = ints * sizeof(int64_t);
		auto output = cast<char*>(PoolMem::getPool().getPtr(bytes));

		// empty these bits otherwise we will get false data
		memset(output, 0, bytes);
		
		bits = recast<uint64_t*>(output);
		return;
	}

	auto bytes = Ints * sizeof(int64_t);
	auto output = cast<char*>(PoolMem::getPool().getPtr(bytes));
	memset(output, 0, bytes);

	assert(bytes);
	//auto testSize = POOL->getSize(output);
	//assert(testSize == -1 || testSize >= bytes);

	auto code = LZ4_decompress_fast(compressedData, output, bytes);

	assert(code > 0);

	ints = Ints;
	bits = recast<uint64_t*>(output);

	// cout << "  mount: ";
	// cout << debugBits(*this, 64) << endl;
}

int64_t IndexBits::getSizeBytes() const
{
	return ints * sizeof(int64_t);
}

char* IndexBits::store(int64_t& compressedBytes) 
{
	if (!ints)
		grow(1);

	auto maxBytes = LZ4_compressBound(ints * sizeof(int64_t));

	auto compressionBuffer = cast<char*>(PoolMem::getPool().getPtr(maxBytes));
	memset(compressionBuffer, 0, maxBytes);

	compressedBytes = LZ4_compress_fast(
		recast<char*>(bits), 
		compressionBuffer, 
		ints * sizeof(int64_t),
		maxBytes,
		2);

	assert(compressedBytes <= maxBytes);

	return compressionBuffer;
}

void IndexBits::grow(int32_t required)
{
	if (ints >= required)
		return;

	auto bytes = required * sizeof(uint64_t);
	auto write = cast<char*>(PoolMem::getPool().getPtr(bytes));
	memset(write, 0, bytes);

	if (bits)
	{
		auto read = recast<char*>(bits);

		// copy the old bytes over
		memcpy(write, read, ints * sizeof(uint64_t));	

		// release the old buffer
		PoolMem::getPool().freePtr(read);
	}

	// make active
	bits = recast<uint64_t*>(write);
	ints = required;
}

void IndexBits::bitSet(int64_t index)
{
	auto pos = index >> 6ULL; // divide by 8

	if (pos >= ints) // is our buffer big enough?
		grow(pos + 1);

	bits[pos] |= BITMASK[index & 63ULL]; // mod 64
}

void IndexBits::lastBit(int64_t index)
{
	auto pos = index >> 6ULL; // divide by 8

	if (pos >= ints) // is our buffer big enough?
		grow(pos + 1);
}

void IndexBits::bitClear(int64_t index)
{
	auto pos = index >> 6ULL; // divide by 8

	if (pos >= ints) // is our buffer big enough?
		grow(pos + 1);

	bits[pos] &= ~(BITMASK[index & 63ULL]); // mod 64
}

bool IndexBits::bitState(int64_t index) const
{
	auto pos = index >> 6ULL; // divide by 8

	if (pos >= ints) // is our buffer big enough?
		return false;

	return (bits[pos] & BITMASK[index & 63ULL]);
}

/*
   population(int stopBit);

   stopBit - bit buffers unpack to lengths longer than 
   the desired amount of bits, this is because the
   buffers used for bits grow in chunks, and the number
   of bits does not typically round to the end of the
   last int64_t containing the bits.

   With AND and OR operations this is not a problem,
   but NOT operations will not the whole buffer, and this
   will result in incorrect counts.
*/
int64_t IndexBits::population(int stopBit) const
{
	if (!bits || !ints)
		return 0;

	int64_t count = 0;
	auto pSource = bits;

	// truncates to the one we want
	auto lastInt = stopBit / 64LL;

	// The stopBit might be beyond the end
	// if the 'ints' buffer. In which case
	// we will set lastInt to the size of ints
	// and stopBit to the very last bit (which
	// will stop it from entering the dangling
	// bits loop)

	if (static_cast<int64_t>(stopBit / 64) > ints)
	{
		lastInt = ints;
		stopBit = lastInt * 64;
	}

	auto pEnd = pSource + lastInt;

	while (pSource < pEnd)
	{
#ifdef _MSC_VER
		count += __popcnt64(*pSource);
#else
		count += __builtin_popcountll(*pSource);
#endif 
		++pSource;
	}

	
	// count any dangling single bits
	for (auto idx = lastInt * 64; idx < stopBit; ++idx)
		count += bitState(idx) ? 1 : 0;
	

	return count;
}

void IndexBits::opCopy(const IndexBits& source)
{
	reset();
	grow(source.ints);

	if (source.ints && source.bits)
		memcpy(bits, source.bits, source.ints * sizeof(int64_t));

	placeHolder = source.placeHolder;
}

void IndexBits::opCopyNot(IndexBits& source)
{
	opCopy(source);
	opNot();
}

void IndexBits::opAnd(IndexBits& source)
{
	if (placeHolder || source.placeHolder)
		return;

	if (!source.ints)
		return;

	if (source.ints > ints)
		grow(source.ints);
	else if (source.ints < ints)
		source.grow(ints);

    //auto sizeSource = POOL->getSize(source.bits);
    //auto sizeDest = POOL->getSize(bits);

    //assert(sizeSource == sizeDest);

	volatile auto pSource = source.bits;
	volatile auto pDest = bits;
	volatile auto pEnd = source.bits + source.ints;

	while (pSource < pEnd)
	{
		*pDest = ((*pDest) & (*pSource));
		++pSource;
		++pDest;
	}
}

void IndexBits::opOr(IndexBits& source)
{

	if (placeHolder || source.placeHolder)
		return;

	if (!source.ints)
		return;

	if (source.ints > ints)
		grow(source.ints);
	else if (source.ints < ints)
		source.grow(ints);

    //auto sizeSource = POOL->getSize(source.bits);
    //auto sizeDest = POOL->getSize(bits);

    //assert(sizeSource == sizeDest);

	volatile auto pSource = source.bits;
	volatile auto pDest = bits;
	volatile auto pEnd = source.bits + source.ints;

	while (pSource < pEnd)
	{
		*pDest = ((*pDest) | (*pSource));
		++pSource;
		++pDest;
	}
}

void IndexBits::opAndNot(IndexBits& source)
{
	if (placeHolder || source.placeHolder)
		return;

	if (!source.ints)
		return;

	if (source.ints > ints)
		grow(source.ints);
	else if (source.ints < ints)
		source.grow(ints);

	volatile auto pSource = source.bits;
	volatile auto pDest = bits;
	volatile auto pEnd = source.bits + source.ints;

	while (pSource < pEnd)
	{
		*pDest = ((*pDest) & (~(*pSource)));
		++pSource;
		++pDest;
	}
}

void IndexBits::opNot() const
{
	if (placeHolder)
		return;

	if (!ints || !bits)
		return;

	volatile auto pSource = bits;
	volatile auto pEnd = bits + ints;

	while (pSource < pEnd)
	{
		*pSource = (~(*pSource));
		++pSource;
	}
}

string IndexBits::debugBits(const IndexBits& bits, int limit)
{
	string result;
	int counter = 0;
	for (auto i = 0; i < bits.ints; i++)
	{
		auto i64 = bits.bits[i];
		for (auto b = 0; b < 64; b++)
		{
			if (i64 & 1)
				result += '1';
			else
				result += '0';

			if (b % 8 == 7)
				result += ' ';

			i64 = i64 >> 1;

			++counter;
			if (counter == limit)
				return result;
		}
	}
	return result;
}

/*
linearIter(int64_t &linId, int stopBit)

linId - start Iterating by passing -1. The next linearId if any
will be returned in this reference.

stopBit - Because bit buffers are typically larger than the last
bit we must provide a stopBit for accurate iteration, especially
if NOT operations were run on the buffer. 

return true if a new linear id is found.

recommend using in a while loop.
*/
bool IndexBits::linearIter(int64_t& linId, int stopBit) const
{
	++linId;

	auto currentInt = linId / 64LL;

	while (currentInt < ints)
	{
		if (bits[currentInt])
		{

			int64_t bitNumber = linId % 64;

			//if (bitIndex >= stopBit)
			if (linId >= stopBit)
				return false;

			for (auto i = bitNumber; i < 64; i++)
			{
				if (bits[currentInt] & BITMASK[i])
				{
					linId = (currentInt * 64) + i;
					return true;
				}
			}
		}
		currentInt++;
		linId = (currentInt * 64);
	}

	return false;
}
