#include "indexbits.h"
#include "dbtypes.h"
#include "sba/sba.h"
#include "lz4.h"
#include <cassert>

using namespace std;
using namespace openset::db;

IndexBits::IndexBits()
    : bits(nullptr),
      ints(0),
      placeHolder(false)
{}

// move constructor
IndexBits::IndexBits(IndexBits&& source) noexcept
{
    bits               = source.bits;
    ints               = source.ints;
    placeHolder        = source.placeHolder;
    source.bits        = nullptr;
    source.ints        = 0;
    source.placeHolder = false;
}

// copy constructor
IndexBits::IndexBits(const IndexBits& source)
    : bits(nullptr),
      ints(0),
      placeHolder(false)
{
    opCopy(source);
}

IndexBits::IndexBits(IndexBits* source)
    : bits(nullptr),
      ints(0),
      placeHolder(false)
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
        bits              = other.bits;
        ints              = other.ints;
        placeHolder       = other.placeHolder;
        other.bits        = nullptr;
        other.ints        = 0;
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
    bits        = nullptr;
    ints        = 0;
    placeHolder = false;
}

void IndexBits::makeBits(const int64_t index, const int state)
{
    reset();

    const auto pos = index >> 6ULL; // divide by 8

    if (pos >= ints) // is our buffer big enough?
        grow(pos + 1);

    memset(bits, (state) ? 0xff : 0x00, ints * 8);

    // if we are 1 filling these bits, we must
    // set every bit after index to zero 
    if (state)
    {
        // zero the rest of the bits in the last int64
        const auto lastBit = (pos + 1) * 64LL;
        for (auto i = index; i < lastBit; i++)
            this->bitClear(i);
    }
}

void IndexBits::mount(
    char* compressedData,
    const int32_t integers,
    const int32_t offset,
    const int32_t length,
    const int32_t linId)
{
    reset();

    if (!integers || linId >= 0)
    {
        ints = 1; // LZ4 compressor uses 9 bytes with a bit set with one INT
        bits = cast<uint64_t*>(PoolMem::getPool().getPtr(8));

        *bits = 0;

        if (linId >= 0)
            bitSet(linId);

        return;
    }

    const auto bytes  = integers * sizeof(int64_t);
    const auto output = cast<char*>(PoolMem::getPool().getPtr(bytes));
    memset(output, 0, bytes);

    assert(bytes);

    const int64_t offsetPtr  = offset * 8;
    const int32_t byteLength = length * 8;

    // TODO - check for int overflow here
    const auto code = LZ4_decompress_fast(compressedData, output + offsetPtr, byteLength);

    assert(code > 0);

    ints = integers;
    bits = recast<uint64_t*>(output);

    if (linId >= 0)
        bitSet(linId);
}

int64_t IndexBits::getSizeBytes() const
{
    return ints * sizeof(int64_t);
}

char* IndexBits::store(int64_t& compressedBytes, int64_t& linId, int32_t& offset, int32_t& length, const int compRatio)
{
    if (!ints)
        grow(1);

    if (const auto pop = population(ints * 64); pop == 0)
    {
        linId           = -1;
        compressedBytes = 0;
        offset          = 0;
        length          = 0;
        return nullptr;
    }
    else if (pop == 1)
    {
        linId           = -1;
        compressedBytes = 0;
        offset          = 0;
        length          = 0;

        linearIter(linId, ints * 64);
        return nullptr;
    }

    // find start

    auto idx      = 0;
    auto firstIdx = -1;
    auto lastIdx  = -1;

    while (idx < ints)
    {
        if (bits[idx])
        {
            if (firstIdx == -1)
                firstIdx = idx;
            lastIdx = idx;
        }
        ++idx;
    }

    offset = firstIdx;
    length = (lastIdx - firstIdx) + 1;

    const auto maxBytes          = LZ4_compressBound(length * sizeof(int64_t));
    const auto compressionBuffer = cast<char*>(PoolMem::getPool().getPtr(maxBytes));

    //memset(compressionBuffer, 0, maxBytes);

    compressedBytes = LZ4_compress_fast(
        recast<char*>(bits + offset),
        compressionBuffer,
        length * sizeof(int64_t),
        maxBytes,
        compRatio);

    linId = -1;

    return compressionBuffer;
}

void IndexBits::grow(int64_t required)
{
    if (ints >= required)
        return;

    required += 32;

    const auto bytes = required * sizeof(uint64_t);
    const auto write = cast<char*>(PoolMem::getPool().getPtr(bytes));

    memset(write, 0, bytes);

    if (bits)
    {
        const auto read = recast<char*>(bits);

        // copy the old bytes over
        memcpy(write, read, ints * sizeof(uint64_t));

        // release the old buffer
        PoolMem::getPool().freePtr(read);
    }

    // make active
    bits = recast<uint64_t*>(write);
    ints = required;
}

void IndexBits::bitSet(const int64_t index)
{
    const int64_t pos = index >> 6ULL; // divide by 8

    if (pos >= ints) // is our buffer big enough?
        grow(pos + 1);

    bits[pos] |= BITMASK[index & 63ULL]; // mod 64
}

void IndexBits::lastBit(const int64_t index)
{
    const int64_t pos = index >> 6ULL; // divide by 8

    if (pos >= ints) // is our buffer big enough?
        grow(pos + 1);
}

void IndexBits::bitClear(const int64_t index)
{
    const int64_t pos = index >> 6ULL; // divide by 8

    if (pos >= ints) // is our buffer big enough?
        grow(pos + 1);

    bits[pos] &= ~(BITMASK[index & 63ULL]); // mod 64
}

bool IndexBits::bitState(const int64_t index) const
{
    const int64_t pos = index >> 6ULL; // divide by 8

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
    auto pSource  = bits;

    // truncates to the one we want
    int64_t lastInt = stopBit / 64LL;

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

    const auto pEnd = pSource + lastInt;

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

    volatile auto pSource    = source.bits;
    volatile auto pDest      = bits;
    const volatile auto pEnd = source.bits + source.ints;

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

    volatile auto pSource    = source.bits;
    volatile auto pDest      = bits;
    const volatile auto pEnd = source.bits + source.ints;

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

    volatile auto pSource    = source.bits;
    volatile auto pDest      = bits;
    const volatile auto pEnd = source.bits + source.ints;

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

    volatile auto pSource    = bits;
    const volatile auto pEnd = bits + ints;

    while (pSource < pEnd)
    {
        *pSource = (~(*pSource));
        ++pSource;
    }
}

string IndexBits::debugBits(const IndexBits& bits, int limit)
{
    string result;
    auto counter = 0;
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
bool IndexBits::linearIter(int64_t& linId, const int64_t stopBit) const
{
    ++linId;

    auto currentInt = linId / 64LL;

    while (currentInt < ints)
    {
        if (bits[currentInt])
        {
            const int64_t bitNumber = linId % 64;

            //if (bitIndex >= stopBit)
            if (linId >= stopBit)
                return false;

            for (auto i = bitNumber; i < 64LL; i++)
            {
                if (bits[currentInt] & BITMASK[i])
                {
                    linId = (currentInt * 64LL) + i;
                    return true;
                }
            }
        }
        currentInt++;
        linId = (currentInt * 64);
    }

    return false;
}
