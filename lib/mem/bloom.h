#ifndef RARELOGIC_CPPLIB_MEM_BLOOM_H_
#define RARELOGIC_CPPLIB_MEM_BLOOM_H_

#include "../include/libcommon.h"

class Bloom
{
public:
	int32_t _Width;
	int32_t _Bits;

	uint64_t* _BitBlock;

	Bloom() { _BitBlock = NULL; };

	void SetSize(int32_t Width)
	{
		_Width = Width;
		_BitBlock = new uint64_t[ Width ];

		memset(_BitBlock, 0, sizeof( int64_t) * _Width);

		_Bits = _Width * 64;
	};

	~Bloom()
	{
		if (_BitBlock)
			delete [] _BitBlock;
	};

	void Set(uint64_t Key)
	{
		Key = Key % _Bits;

		// The Offset of the _BitBlock Array is Key / 64.
		// We Modded the Key above to the number of bits so Key / 64 is the int64_t in the
		// array.
		//
		// The |= (OR assign) is 1 bit shifted left by the Key modded by 64 to get the
		// bit... so the right bit gets set in the right int64_t in the array.
		_BitBlock[Key >> (int64_t)6] |= ((int64_t)1 << (Key % 64));
	};

	bool Check(uint64_t Key)
	{
		Key = Key % _Bits;
		return (_BitBlock[Key >> (int64_t)6] & ((int64_t)1 << (Key % 64))) != 0;
	};
};

#endif // RARELOGIC_CPPLIB_MEM_BLOOM_H_
