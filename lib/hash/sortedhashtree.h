#ifndef RARELOGIC_CPPLIB_HASH_SORTEDHASHTREE_H_
#define RARELOGIC_CPPLIB_HASH_SORTEDHASHTREE_H_

#include "../include/libcommon.h"

#include <stack>
#include <vector>

#include "../mem/bloom.h"
#include "../threads/locks.h"

const int64_t SHT_BLOCKSIZE = 1024 * 16;

class TreeMemory
{
public:

	TreeMemory();
	~TreeMemory();

	char* GetBlock();
	void ReturnBlock(char* block, bool lock = true);

private:
	CriticalSection TreeCS;
	std::stack<char*> BlockList;

	TreeMemory(const TreeMemory& mem);
	TreeMemory& operator=(const TreeMemory& mem);
};

extern TreeMemory* TreePool;
void InitializeSortedHashTree();

template <typename T, typename V>
class SortedTree
{
private:

#pragma pack(push,1)

	struct _Array
	{
		uint8_t Index;
		void* Item;
	};

	struct _branchStrip
	{
		uint8_t SizeBits;
		uint16_t Count;

		_Array Array[256];
	};

	// Lets as look at T which could be a struct or ints or floats
	// or really anything as a byte array
	union _Bytes
	{
		T Original;
		uint8_t Bytes[ sizeof( T) ];
	};

#pragma pack(pop)

	// Array of LastInFirstOut Stacks containing addresses of free space
	// the array index corresponds to the SizeBits in the strip
	std::stack<char*> Recycling[32];

	_branchStrip* _Root;

	int64_t _BlockSize;

	int64_t _MemUsed;

	int32_t _sizeofT;

	char* Block;
	char* BlockTail;

	int64_t MemPages;

	//Bloom		BloomFilter;

public:

	int32_t _DistinctCount;

	std::vector<char*> BlockList;

#pragma pack(push,1)
	struct _Row
	{
		_branchStrip* Branch;
		int64_t BranchOffset;
	};

	struct _Cursor
	{
		_Row Stack[ sizeof( T) ];
		int64_t Depth;
		V Value;
		_Row Row;
		_Row LastRow;
		_Bytes Key;
	};
#pragma pack(pop)

	SortedTree()
	{
		Reset();
	};

	~SortedTree()
	{
		for (int i = 0; i < BlockList.size(); i++)
			TreePool->ReturnBlock(BlockList[i]);
	};

	void Reset()
	{
		for (int i = 0; i < BlockList.size(); i++)
			TreePool->ReturnBlock(BlockList[i]);

		for (int i = 0; i < 32; i++)
			while (!Recycling[i].empty())
				Recycling[i].pop();

		BlockList.clear();

		_DistinctCount = 0;
		_MemUsed = 0;

		MemPages = 1;
		_BlockSize = SHT_BLOCKSIZE * MemPages;

		Block = TreePool->GetBlock(); //new char[ _BlockSize ];
		BlockTail = Block;

		_MemUsed += SHT_BLOCKSIZE;//_BlockSize;

		BlockList.push_back(Block);

		_sizeofT = sizeof( T);

		_Root = (_branchStrip*)BlockTail;
		//memset( _Root, 0, sizeof( _branchStrip ) );
		_Root->Count = 0;
		_Root->SizeBits = 8;

		BlockTail += sizeof( _branchStrip);
	};

	int64_t GetMemUse()
	{
		return _MemUsed;
	};

	int GetIndex(_branchStrip* Strip, uint8_t IndexByte)
	{
		if (Strip->Count == 1)
		{
			if (Strip->Array[0].Index == IndexByte)
				return 0;

			return -1;
		}

		if (Strip->Count == 256)
			return IndexByte;

		int First = 0;
		int Last = Strip->Count - 1;
		int Mid = 0;

		while (First <= Last)
		{
			Mid = (First + Last) >> 1; //  / 2;

			if (IndexByte > Strip->Array[Mid].Index)
				First = Mid + 1;
			else if (IndexByte < Strip->Array[Mid].Index)
				Last = Mid - 1;
			else
				return Mid;
		}

		return -(First + 1);
	};

	_branchStrip* GrowStrip(_branchStrip* OldStrip)
	{
		int SizeBits = OldStrip->SizeBits + 1;

		_branchStrip* New = NewStrip(SizeBits);

		memcpy((char*)New, (char*)OldStrip, (OldStrip->Count * sizeof( _Array)) + 3);

		New->SizeBits = SizeBits;

		DeleteStrip(OldStrip);

		return New;
	};

	_branchStrip* InsertIndex(_branchStrip* Strip, uint8_t IndexByte, void* Pointer)
	{
		int Capacity = 1 << Strip->SizeBits;

		if (Strip->Count + 1 > Capacity)
		{
			Strip = GrowStrip(Strip);
		}

		//if (Pointer == NULL)
		//	printf( "WTF\r\n" );

		int LowPos = 0;

		LowPos = GetIndex(Strip, IndexByte);
		LowPos = -LowPos - 1;

		while (LowPos < Strip->Count)
		{
			if (Strip->Array[LowPos].Index > IndexByte)
			{
				int HighPos = Strip->Count - 1;

				while (HighPos >= LowPos)
				{
					Strip->Array[HighPos + 1].Index = Strip->Array[HighPos].Index;
					Strip->Array[HighPos + 1].Item = Strip->Array[HighPos].Item;
					HighPos --;
				}

				break;
			}

			LowPos++;
		}

		Strip->Array[LowPos].Index = IndexByte;
		Strip->Array[LowPos].Item = Pointer;

		Strip->Count++;

		return Strip;
	};

	_branchStrip* NewStrip(int SizeBits)
	{
		_branchStrip* New;

		int Capacity = 1 << SizeBits;

		if (Recycling[SizeBits].empty())
		{
			//if ((int64_t)(BlockTail - Block) > _BlockSize - ((Capacity * sizeof( _Array ) ) + 4))
			int64_t RemainingBlock = _BlockSize - (BlockTail - Block);
			int64_t RequestSize = (Capacity * sizeof( _Array)) + 3;

			if (RequestSize > RemainingBlock)
			{
				if (SizeBits > 1)
				{
					int64_t Bits = SizeBits - 1;

					while (Bits)
					{
						if ((((int64_t)1 << Bits) * sizeof( _Array) + 3) < RemainingBlock)
						{
							_branchStrip* TempStrip = (_branchStrip*)BlockTail;
							TempStrip->SizeBits = Bits;
							TempStrip->Count = 0;
							DeleteStrip(TempStrip);

							break;
						}

						Bits--;
					}
				}

				MemPages++;
				if (MemPages > 16)
					MemPages = 16;
				_BlockSize = SHT_BLOCKSIZE;// * MemPages;

				Block = TreePool->GetBlock();//new char[ _BlockSize ];
				BlockTail = Block;

				_MemUsed += _BlockSize;

				BlockList.push_back(Block);
			}

			New = (_branchStrip*)BlockTail;
			BlockTail += RequestSize;//(Capacity * sizeof( _Array ) ) + 3;
		}
		else
		{
			New = (_branchStrip*)Recycling[SizeBits].top();
			Recycling[SizeBits].pop();
		}

		memset(New, 0, (Capacity * sizeof( _Array)) + 3);

		New->SizeBits = SizeBits;
		New->Count = 0;

		return New;
	};

	void DeleteStrip(_branchStrip* Strip)
	{
		Recycling[Strip->SizeBits].push((char*)Strip);
	};

	void Set(T Key, V Value)
	{
		_Bytes Parts;
		Parts.Original = Key;

		_branchStrip* Branch = _Root;
		_branchStrip* LastBranch = NULL;
		int LastIndex = 0;

		int StripIndex = 0;

		//for (int i = 0; i < _sizeofT - 1; i++)
		for (int i = _sizeofT - 1; i > 0; i--)
		{
			StripIndex = GetIndex(Branch, Parts.Bytes[i]);

			if (StripIndex < 0)
			{
				_branchStrip* NewBranch = NewStrip(0);

				Branch = InsertIndex(Branch, Parts.Bytes[i], NewBranch);

				// Branches move... so we just update the parent if it exists
				// and re-set the pointer. The root node never resizes so this
				// if LastBranch is null we are at the root and it doesn't matter
				if (LastBranch)
				{
					LastBranch->Array[LastIndex].Item = Branch;
				}

				LastBranch = Branch;
				LastIndex = GetIndex(Branch, Parts.Bytes[i]);

				Branch = NewBranch;

				//printf( "new branch at depth [%u]\r\n", i );
			}
			else
			{
				LastBranch = Branch;
				LastIndex = StripIndex;
				Branch = (_branchStrip*)Branch->Array[StripIndex].Item;
			}
		}

		if (GetIndex(Branch, Parts.Bytes[0]) < 0)
		{
			_DistinctCount++;

			Branch = InsertIndex(Branch, Parts.Bytes[0], (void*)Value);

			//BloomFilter.sSet( Key );

			if (LastBranch)
			{
				LastBranch->Array[LastIndex].Item = Branch;
			}
		}
	};

	bool Get(T Key, V& Value)
	{
		_Bytes Parts;
		Parts.Original = Key;

		register _branchStrip* Branch = _Root;
		register int StripIndex;

		int Loop = _sizeofT - 1;

		while (Loop)
		{
			if ((StripIndex = GetIndex(Branch, Parts.Bytes[Loop])) < 0)
				return false;

			Branch = (_branchStrip*)Branch->Array[StripIndex].Item;

			Loop--;
		}

		StripIndex = GetIndex(Branch, Parts.Bytes[0]);

		if (StripIndex < 0)
			return false; // dead end at last node, not found, fail

		Value = (V)Branch->Array[StripIndex].Item;

		//if (Value == NULL)
		//	return false; // no value, fail also.

		return true;
	};

	bool GetPtr(T Key, V* & Value)
	{
		///		if (!BloomFilter.Check( Key )) return false;

		_Bytes Parts;
		Parts.Original = Key;

		register _branchStrip* Branch = _Root;
		register int StripIndex;

		int Loop = _sizeofT - 1;

		while (Loop)
		{
			if ((StripIndex = GetIndex(Branch, Parts.Bytes[Loop])) < 0)
				return false;

			Branch = (_branchStrip*)Branch->Array[StripIndex].Item;

			Loop--;
		}

		StripIndex = GetIndex(Branch, Parts.Bytes[0]);

		if (StripIndex < 0)
			return false; // dead end at last node, not found, fail

		Value = (V*)&Branch->Array[StripIndex].Item;

		//if (Value == NULL)
		//	return false; // no value, fail also.

		return true;
	};

	bool Contains(T Key)
	{
		//		if (!BloomFilter.Check( Key )) return false;

		_Bytes Parts;
		Parts.Original = Key;

		register _branchStrip* Branch = _Root;
		register int StripIndex;

		int Loop = _sizeofT - 1;

		while (Loop)
		{
			//			StripIndex = GetIndex( Branch, Parts.Bytes[ Loop ] );

			if ((StripIndex = GetIndex(Branch, Parts.Bytes[Loop])) < 0)
				return false;

			Branch = (_branchStrip*)Branch->Array[StripIndex].Item;

			Loop--;
		}

		StripIndex = GetIndex(Branch, Parts.Bytes[0]);

		if (StripIndex < 0)
			return false; // dead end at last node, not found, fail

		return true;
	};

	T GetIteratedKey(_Cursor* Cursor)
	{
		return Cursor->Key.Original;
	};

	_Cursor* IterateSearch(T Key, int Width)
	{
		//if (!BloomFilter.Check( Key )) return false;

		_Cursor* Cursor = new _Cursor;
		Cursor->Depth = 0;
		Cursor->Value = NULL;

		_Bytes Parts;
		Parts.Original = Key;

		register _branchStrip* Branch = _Root;
		register int StripIndex;

		_Row Row;
		_Row LastRow;

		int Loop = _sizeofT - 1;

		while (Loop)
		{
			//			StripIndex = GetIndex( Branch, Parts.Bytes[ Loop ] );

			if ((StripIndex = GetIndex(Branch, Parts.Bytes[Loop])) < 0)
			{
				//Loop++;
				//Cursor->Depth--;
				//Branch = Cursor->Stack[ Cursor->Depth ].Branch;
				//StripIndex = Cursor->Stack[ Cursor->Depth ].BranchOffset - 1;
				//break; 
				//return false;
				//StripIndex = 0;
				StripIndex = -StripIndex;
				StripIndex --;

				if (StripIndex >= Branch->Count)
					StripIndex = Branch->Count - 1;
			}

			Row.Branch = (_branchStrip*)Branch;
			Row.BranchOffset = StripIndex;

			LastRow = Row;

			Cursor->Stack[Cursor->Depth] = Row;
			Cursor->Depth++;

			Branch = (_branchStrip*)Branch->Array[StripIndex].Item;

			Loop--;

			if (Cursor->Depth == Width + 1)
				break;
		}

		while (Cursor->Depth < _sizeofT)
		{
			if (LastRow.Branch && LastRow.Branch->Count == 0)
			{
				delete Cursor;
				return NULL;
			}

			Row.Branch = (_branchStrip*)LastRow.Branch->Array[0].Item;
			Row.BranchOffset = 0;

			Cursor->Stack[Cursor->Depth] = Row;
			Cursor->Depth++;

			Cursor->Value = (V)Row.Branch->Array[Row.BranchOffset].Item;

			LastRow = Row;
		}

		int Count = 0;

		for (int i = _sizeofT - 1; i > -1; i--)
		{
			Cursor->Key.Bytes[Count] = Cursor->Stack[i].Branch->Array[Cursor->Stack[i].BranchOffset].Index;
			Count++;
		}

		/*		StripIndex = GetIndex( Branch, Parts.Bytes[ 0 ] );
		
				Row.Branch = (_branchStrip*)Branch;
				Row.BranchOffset = StripIndex;
		
				Cursor->Stack[ Cursor->Depth ] = Row;
				Cursor->Depth++;
		*/
		//		if (StripIndex < 0)
		//			return NULL;

		Cursor->Stack[Cursor->Depth - 1].BranchOffset--;

		return Cursor;
	};

	_Cursor* IterateStart()
	{
		_Cursor* Cursor = new _Cursor;
		Cursor->Depth = 0;

		_Row Row;
		_Row LastRow;

		Row.Branch = _Root;
		Row.BranchOffset = 0;

		Cursor->Stack[Cursor->Depth] = Row;
		Cursor->Depth++;

		LastRow = Row;

		while (Cursor->Depth < _sizeofT)
		{
			if (LastRow.Branch && LastRow.Branch->Count == 0)
			{
				delete Cursor;
				return NULL;
			}

			Row.Branch = (_branchStrip*)LastRow.Branch->Array[0].Item;
			Row.BranchOffset = 0;

			Cursor->Stack[Cursor->Depth] = Row;
			Cursor->Depth++;

			Cursor->Value = (V)Row.Branch->Array[Row.BranchOffset].Item;

			LastRow = Row;
		}

		int Count = 0;

		for (int i = _sizeofT - 1; i > -1; i--)
		{
			Cursor->Key.Bytes[Count] = Cursor->Stack[i].Branch->Array[Cursor->Stack[i].BranchOffset].Index;
			Count++;
		}

		Cursor->Stack[Cursor->Depth - 1].BranchOffset--;

		//IterateStrip( Cursor );

		return Cursor;
	};

	bool IterateStrip(_Cursor* Cursor)
	{
		if (!Cursor || Cursor->Depth <= 0)
		{
			return false;
		}

		Cursor->Value = NULL;

		register _Row* StackPtr = Cursor->Stack + (Cursor->Depth - 1);

		StackPtr->BranchOffset++;

		Cursor->Row.BranchOffset = StackPtr->BranchOffset;
		Cursor->Row.Branch = StackPtr->Branch;

		Cursor->Key.Bytes[(_sizeofT - (Cursor->Depth))] = StackPtr->Branch->Array[StackPtr->BranchOffset].Index;

		while (Cursor->Row.BranchOffset >= Cursor->Row.Branch->Count)
		{
			Cursor->Depth--;

			if (Cursor->Depth <= 0)
			{
				Cursor->Depth = 0;
				return false;
			}

			StackPtr = Cursor->Stack + (Cursor->Depth - 1);

			StackPtr->BranchOffset++;

			Cursor->Row.BranchOffset = StackPtr->BranchOffset;
			Cursor->Row.Branch = StackPtr->Branch;

			Cursor->Key.Bytes[_sizeofT - (Cursor->Depth)] = StackPtr->Branch->Array[StackPtr->BranchOffset].Index;
		}

		//Cursor->LastRow = Cursor->Row;
		Cursor->LastRow.BranchOffset = Cursor->Row.BranchOffset;
		Cursor->LastRow.Branch = Cursor->Row.Branch;

		while (Cursor->Depth < _sizeofT)
		{
			if (!Cursor->LastRow.Branch->Count)
			{
				Cursor->Depth = 0;
				return false;
			}

			Cursor->Row.Branch = (_branchStrip*)Cursor->LastRow.Branch->Array[Cursor->LastRow.BranchOffset].Item;
			Cursor->Row.BranchOffset = 0;

			Cursor->Stack[Cursor->Depth].BranchOffset = Cursor->Row.BranchOffset;
			Cursor->Stack[Cursor->Depth].Branch = Cursor->Row.Branch;
			Cursor->Depth++;

			Cursor->Key.Bytes[_sizeofT - (Cursor->Depth)] = Cursor->Stack[Cursor->Depth - 1].Branch->Array[Cursor->Stack[Cursor->Depth - 1].BranchOffset].Index;

			//Cursor->LastRow = Cursor->Row;
			Cursor->LastRow.BranchOffset = Cursor->Row.BranchOffset;
			Cursor->LastRow.Branch = Cursor->Row.Branch;
		}

		Cursor->Value = (V)Cursor->Row.Branch->Array[Cursor->Row.BranchOffset].Item;

		return true;
	};
};

#endif // RARELOGIC_CPPLIB_HASH_SORTEDHASHTREE_H_
