#pragma once

#include "common.h"

namespace openset
{
    namespace db
    {
        class IndexBits
        {
        public:
            uint64_t* bits;
            int32_t ints; // length in int64's
            bool placeHolder;

            IndexBits();
            IndexBits(IndexBits&& source) noexcept;
            IndexBits(const IndexBits& source);
            explicit IndexBits(IndexBits* source);
            ~IndexBits();

            IndexBits& operator=(IndexBits&& other) noexcept;
            IndexBits& operator=(const IndexBits& other);

            void reset();

            // make bits that are on or off
            // index is number of bits, state is 1 or 0
            void makeBits(int64_t index, int state);

            // takes buffer to compressed data and actual size as parameters
            // note: actual size is number of long longs (in64_t)
            void mount(char* compressedData, int32_t integers, int32_t offset, int32_t length, int32_t linId);

            int64_t getSizeBytes() const;

            // returns a POOL buffer ptr, and the number of bytes
            char* store(int64_t& compressedBytes, int64_t& linId, int32_t& offset, int32_t& length, int compRatio = 1);

            void grow(int64_t required, bool exact = true);

            void lastBit(int64_t index);
            void bitSet(int64_t index);
            void bitClear(int64_t index);
            bool bitState(int64_t index) const;

            int64_t population(int stopBit) const;

            void opCopy(const IndexBits& source);
            void opCopyNot(IndexBits& source);
            void opAnd(IndexBits& source);
            void opOr(IndexBits& source);
            void opAndNot(IndexBits& source);
            void opNot() const;

            bool linearIter(int64_t& linId, int64_t stopBit) const;

            class BitProxy
            {
            public:
                IndexBits* bits;
                int idx;
                int value;

                BitProxy(IndexBits* bits, const int idx)
                    : bits(bits),
                      idx(idx)
                {
                    value = bits->bitState(idx);
                }

                ~BitProxy()
                {
                    cout << "destroyed" << endl;
                }

                void operator=(const int rhs)
                {
                    value = rhs;
                    if (rhs)
                        bits->bitSet(idx);
                    else
                        bits->bitClear(idx);
                }

                operator int() const
                {
                    return value;
                }
            };

            static string debugBits(const IndexBits& bits, int limit = 64);

            BitProxy operator[](const int idx)
            {
                return BitProxy(this, idx);
            }

            friend std::ostream& operator<<(std::ostream& os, const IndexBits& source)
            {
                os << debugBits(source, static_cast<int>(os.width() ? os.width() : 128));
                return os;
            }
        };
    };
};
