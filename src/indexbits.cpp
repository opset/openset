#include "indexbits.h"
#include "dbtypes.h"
#include "sba/sba.h"
#include "lz4.h"
#include <cassert>

using namespace std;
using namespace openset::db;

void IndexMemory::decompress(char* compressedData)
{
    if (!compressedData)
        return;

    reset();

    auto rawPage = reinterpret_cast<CompPageMemory_s*>(compressedData);

    while (rawPage)
    {
        const auto indexPage = getPageByPageIndex(rawPage->index, false);
        LZ4_decompress_fast(
            rawPage->compressedData,
            reinterpret_cast<char*>(indexPage->bitArray),
            IndexPageDataSize);

        // next block
        rawPage = rawPage->next;
    }
}

char* IndexMemory::compress()
{
    const auto compBuffer = static_cast<char*>(PoolMem::getPool().getPtr(IndexPageDataSize + Overflow));

    RawPageList newRawPages;

    auto pageIdx = -1;
    for (auto indexPage : indexPages)
    {
        ++pageIdx;

        const auto rawPage = getRawPage(pageIdx);

        // we have no bits in this page (skip, and cleanup the old page)
        if (!pagePopulation(indexPage))
        {
            if (rawPage)
                PoolMem::getPool().freePtr(rawPage);
            continue;
        }

        // use existing if we already have compressed version of this and nothing changed
        if (rawPage)
        {
            if (!indexPage->dirty)
            {
                newRawPages.push_back(rawPage);
                continue;
            }
            PoolMem::getPool().freePtr(rawPage);
        }

        indexPage->dirty = false;

        const auto compressedSize = LZ4_compress_fast(
            reinterpret_cast<char*>(indexPage->bitArray),
            compBuffer,
            IndexPageDataSize,
            IndexPageDataSize + Overflow,
            5
        );

        const auto newRawPage = static_cast<CompPageMemory_s*>(PoolMem::getPool().getPtr(CompPageHeaderSize + compressedSize));

        newRawPage->index = pageIdx;
        newRawPage->next = nullptr;
        memcpy(newRawPage->compressedData, compBuffer, compressedSize);

        if (newRawPages.size())
            newRawPages.back()->next = newRawPage;

        newRawPages.push_back(newRawPage);
    }

    PoolMem::getPool().freePtr(compBuffer);

    rawPages = std::move(newRawPages);

    if (rawPages.size())
        return reinterpret_cast<char*>(newRawPages.front());

    return nullptr;
}

IndexBits::IndexBits()
    : data(),
      placeHolder(false)
{}

// move constructor
IndexBits::IndexBits(IndexBits&& source) noexcept
   : data(std::move(source.data))
{
    placeHolder        = source.placeHolder;
    source.placeHolder = false;
}

// copy constructor
IndexBits::IndexBits(const IndexBits& source)
    : data(data),
      placeHolder(false)
{
    opCopy(source);
}

IndexBits::IndexBits(IndexBits* source)
    : data(),
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
        data = std::move(other.data);
        placeHolder       = other.placeHolder;
        other.placeHolder = false;
    }
    return *this;
}

// copy assignment operator
IndexBits& IndexBits::operator=(const IndexBits& other)
{
    if (this != &other)
    {
        data = other.data;
        placeHolder = other.placeHolder;
    }
    return *this;
}

void IndexBits::reset()
{
    data.reset();
    placeHolder = false;
}

void IndexBits::makeBits(const int64_t index, const int state)
{
    reset();

    const auto lastInt = index / 64LL;

    for (auto i = 0; i <= lastInt; ++i)
        *data.getInt(i) = state ? 0xFFFFFFFFFFFFFFFF : 0x0;

    if (state)
    {
        // zero the rest of the bits in the last int64
        const auto lastBit = data.intCount() * 64LL;
        for (auto i = index; i < lastBit; i++)
            bitClear(i);
    }
}

void IndexBits::mount(char* compressedData)
{
    reset();
    data.decompress(compressedData);
}

char* IndexBits::store()
{
    return data.compress();
}

void IndexBits::bitSet(const int64_t index)
{
    const auto bits = data.getBitInt(index);
    *bits |= BITMASK[index & 63ULL]; // mod 64
    data.setDirty();
}

void IndexBits::setSizeByBit(const int64_t index)
{
    data.getBitInt(index);
}

void IndexBits::bitClear(const int64_t index)
{
    const auto bits = data.getBitInt(index);
    *bits &= ~(BITMASK[index & 63ULL]); // mod 64
    data.setDirty();
}

bool IndexBits::bitState(const int64_t index)
{
    const auto bits = data.getBitInt(index);
    return ((*bits) & BITMASK[index & 63ULL]);
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
int64_t IndexBits::population(const int64_t stopBit)
{
    int64_t count = 0;

    // truncates to the one we want
    const auto lastInt = stopBit / 64LL;
    int64_t idx = 0;

    while (idx < lastInt)
    {
        const auto value = data.getInt(idx);
#ifdef _MSC_VER
        count += __popcnt64(*value);
#else
        count += __builtin_popcountll(*value);
#endif
        ++idx;
    }

    // count any dangling single bits
    for (idx = lastInt * 64LL; idx < stopBit; ++idx)
        count += bitState(idx) ? 1 : 0;

    return count;
}

void IndexBits::opCopy(const IndexBits& source)
{
    reset();
    data = source.data;
    placeHolder = source.placeHolder;
    data.setDirtyAllPages();
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

    auto index = 0;
    const auto end = source.data.intCount();

    while (index < end)
    {
        const auto dest = data.getInt(index);
        *dest &= *source.data.getInt(index);
        data.setDirty();
        ++index;
    }
}

void IndexBits::opOr(IndexBits& source)
{
    if (placeHolder || source.placeHolder)
        return;

    auto index = 0;
    const auto end = source.data.intCount();

    while (index < end)
    {
        const auto dest = data.getInt(index);
        *dest |= *source.data.getInt(index);
        data.setDirty();
        ++index;
    }
}

void IndexBits::opAndNot(IndexBits& source)
{
    if (placeHolder || source.placeHolder)
        return;

    auto index = 0;
    const auto end = source.data.intCount();

    while (index < end)
    {
        const auto dest = data.getInt(index);
        *dest = *dest & ~(*source.data.getInt(index));
        data.setDirty();
        ++index;
    }
}

void IndexBits::opNot()
{
    if (placeHolder)
        return;

    auto index = 0;
    const auto end = data.intCount();

    while (index < end)
    {
        const auto dest = data.getInt(index);
        *dest = ~(*dest);
        data.setDirty();
        ++index;
    }
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
bool IndexBits::linearIter(int64_t& linId, const int64_t stopBit)
{
    ++linId;

    const auto count = data.intCount();
    auto currentInt = linId / 64LL;

    while (currentInt < count)
    {
        const auto value = data.getInt(currentInt);

        if (*value)
        {
            const auto bitNumber = linId % 64LL;

            if (linId >= stopBit)
                return false;

            for (auto i = bitNumber; i < 64LL; ++i)
            {
                if (*value & BITMASK[i])
                {
                    linId = (currentInt * 64LL) + i;
                    return true;
                }
            }
        }
        ++currentInt;
        linId = (currentInt * 64);
    }

    return false;
}
