#pragma once

#include <cstdint>
#include <vector>
#include <utility>
#include <functional>
#include <cstring>

namespace bigConf
{
	struct big_info_s
	{
		int steps;
		std::vector<int> powers;
		std::vector<int> probing;
	};

	const std::vector<big_info_s> big_info =
	{
		{ // lt_compact
			3,
			{ 32, 256, 2048 },
			{ 2, 4, 8 }
		},
		{ // lt_1_million
			5,
			{ 32, 1024, 4096, 8192, 16384 },
			{ 1, 2, 3, 3, 4 }
		},
		{ // lt_5_million
			5,
			{ 1024, 4096, 8192, 65536, 131072 },
			{ 1, 2, 3, 3, 4, 6 }
		},
		{ // lt_25_million
			6,
			{ 2048, 8192, 65536, 262144, 1048576, 4194304 },
			{ 1, 2, 2, 3, 4, 6 }
		},
		{ // gt_25_million
			7, 
			{ 2048, 8192, 65536, 262144, 1048576, 4194304, 8388608 },
			{ 1, 2, 2, 3, 3, 4, 6 }
		},
		{ // gt_50_million
			8,
			{ 4096, 8192, 65536, 262144, 1048576, 4194304, 8388608, 16777216 },
			{ 1, 2, 2, 3, 3, 4, 4, 6 }
		},
		{ // gt_150_million
			8,
			{ 8192, 65536, 262144, 1048576, 4194304, 8388608, 16777216, 33554432 },
			{ 1, 2, 2, 3, 3, 4, 4, 6 }
		},
		{ // gt_250_million
			6,
			{ 1'048'576, 4'194'304, 8'388'608, 16'777'216, 33'554'432, 67'108'864 },
			{ 2, 2, 3, 3, 4, 6 }
		},
		{ // gt_1_billion
			5,
			{ 8'388'608, 16'777'216, 33'554'432, 67'108'864, 134'217'728 },
			{ 1, 2, 3, 4, 6 }
		}
	};
}

enum class ringHint_e : int
{
	lt_compact = 0,
	lt_1_million = 1,
	lt_5_million = 2,
	lt_25_million = 3,
	gt_25_million = 4,
	gt_50_million = 5,
	gt_150_million = 6,
	gt_250_million = 7,
	gt_1_billion = 8
};

template <typename K, typename V>
struct bigRingPage
{
#pragma pack(push,1)
	using item_s = std::pair<K, V>;
#pragma pack(pop)

	int overflow;
	int size;
	bigRingPage* nextRing;
	item_s items[1];

	bigRingPage(int size, int overflow) :
		overflow(overflow),
		size(size),
		nextRing(nullptr)
	{
		clear();
	}

	void clear()
	{
		memset(items, 0xff, (size + overflow) * sizeof(item_s));
		nextRing = nullptr;
	}
};

template <typename K, typename V>
class bigRing
{
private:	

	using Item = typename bigRingPage<K, V>::item_s;
	using RingPage = bigRingPage<K, V>;

	bigConf::big_info_s conf;

	std::hash<K> hasher;
	char voidItem[sizeof(Item)];

	RingPage* root;

public:
	int branchCount;
	int64_t totalBytes;
	int64_t distinct;

	explicit bigRing(
		ringHint_e sizeHint =
		ringHint_e::gt_25_million) :
		branchCount(0),
		totalBytes(0),
		distinct(0)
	{
		conf = bigConf::big_info[static_cast<int>(sizeHint)];
		root = newbig();
		memset(&voidItem, 0xFF, sizeof(voidItem));
	}

	bigRing(bigRing<K,V> &&other) noexcept
	{
		root = other.root;
		conf = other.conf;
		branchCount = other.branchCount;
		totalBytes = other.totalBytes;
		distinct = other.distinct;

		other.branchCount = 0;
		other.totalBytes = 0;
		other.distinct = 0;
		other.root = nullptr;

		memset(&voidItem, 0xFF, sizeof(voidItem));
	}

	// delete copy operators
	bigRing(const bigRing &other) = delete;
	bigRing& operator=(bigRing const&) = delete;

	RingPage* newbig()
	{
		auto overflow = 
			(branchCount >= conf.steps) ?
				conf.probing[conf.steps - 1] :
					conf.probing[branchCount];

		auto elements = 
			(branchCount >= conf.steps) ?
				conf.powers[conf.steps - 1] :
					conf.powers[branchCount];

		auto sizeBytes = 
			((elements + overflow) * sizeof(Item)) + sizeof(RingPage);

		// placement new
		auto branch = new (new char[sizeBytes])RingPage(elements, overflow);

		++branchCount;
		totalBytes += sizeBytes;

		return branch;
	}
			
	inline bool isEmpty(const Item* item) const
	{
		return (memcmp(item, &voidItem, sizeof(Item)) == 0);
	}

	Item* set(const K key, const V value)
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (true)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
				{
					item->~Item();

					item->first = key;
					item->second = value;
					return item;
				}
					
				if (isEmpty(item))
				{
					++distinct;					

					item->first = key;
					item->second = value;
					return item;
				}
			}

			if (!current->nextRing)
			{
				current->nextRing = newbig();
				++branchCount;
			}

			current = current->nextRing;

		}
	}

	template <class... Args>
	Item* emplace(const K key, Args&&... params)
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (true)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
				{
					item->~Item();

					item->first = key;
					new (&item->second) V(std::forward<Args>(params)...);
					return item;
				}
					
				if (isEmpty(item))
				{
					++distinct;

					item->first = key;
					new (&item->second) V(std::forward<Args>(params)...);
					return item;
				}
			}

			if (!current->nextRing)
			{
				current->nextRing = newbig();
				++branchCount;
			}

			current = current->nextRing;
		}
	}

	Item* emplace(std::pair<K,V>&& p)
	{
		K key = p.first;
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (true)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
				{
					item->~Item();
					new (item) std::pair<K, V>(p);
					return item;
				}
					
				if (isEmpty(item))
				{
					++distinct;
					new (item) std::pair<K, V>(p);
					return item;
				}
			}

			if (!current->nextRing)
			{
				current->nextRing = newbig();
				++branchCount;
			}

			current = current->nextRing;
		}
	}

	template <class... Args>
	bool emplaceTry(const K key, Args&&... params)
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (true)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
					return false;

				if (isEmpty(item))
				{
					++distinct;

					item->first = key;
					new (&item->second) V(std::forward<Args>(params)...);
					return true;
				}
			}

			if (!current->nextRing)
			{
				current->nextRing = newbig();
				++branchCount;
			}

			current = current->nextRing;
		}
	}

	bool get(const K key, V &value) const
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (current)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
				{
					value = item->second;
					return true;
				}
			}

			current = current->nextRing;
		}
		return false;
	}

	Item* get(const K key) const
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (current)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
					return item;
			}

			current = current->nextRing;
		}
		return nullptr;
	}

	/**
	 * [] will return a reference to the Value if key is found,
	 * otherwise it will insert key and return a reference to
	 * a new value.
	 */
	V& operator[](const K &key)
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);

		while (true)
		{
			auto item = current->items +
				(keyHash % current->size);
			auto last = item + current->overflow;

			for (; item < last; ++item)
			{
				if (item->first == key)
				{
					return item->second;
				}
					
				if (isEmpty(item))
				{
					++distinct;
					item->first = std::move(key);
					return item->second;
				}
			}

			if (!current->nextRing)
			{
				current->nextRing = newbig();
				++branchCount;
			}

			current = current->nextRing;
		}
	}

	class iterator
	{
	private:

		const bigRing<K, V>* dict;
		RingPage* current;

		int bigIter;
		int totalIter;
		int totalSize;

	public:

		typedef iterator				   self_type;
		typedef V                          value_type;
		typedef int                        difference_type;
		typedef std::forward_iterator_tag  iterator_category;
		typedef iterator*                  pointer;
		typedef iterator&                  reference;


		explicit iterator(const bigRing* dict) :
			dict(dict),
			current(nullptr),
			bigIter(-1),
			totalIter(0),
			totalSize(0)
		{
			current = dict->root;
			auto t = dict->root;

			// gather all the bigs
			while (t)
			{
				totalSize += t->size + t->overflow;
				t = t->nextRing;
			}

			// move to first data (or end)
			__incr();
		};

		void moveToEnd()
		{
			totalIter = totalSize;
		}

		void moveToLocation(int64_t totalOffset, int64_t ringOffset, int64_t depth)
		{
			current = dict->root;

			// move to the specified ring
			for (auto i = 0; i < depth; i++)
				current = current->nextRing;

			totalIter = totalOffset;
			bigIter = ringOffset;
		}

		void __incr()
		{

			if (totalIter == totalSize || !current)
				return;

			while (true)
			{
				++bigIter;

				if (!current) break;

				if (bigIter == current->size + current->overflow)
				{
					bigIter = -1;
					current = current->nextRing;
					continue;
				}

				++totalIter;

				if (!dict->isEmpty(current->items + bigIter))
					break;
			}
		}

		self_type operator++()
		{
			self_type i = *this;
			__incr();
			return i;
		}

		reference operator++(int junk)
		{
			__incr();
			return *this;
		}

		typename RingPage::item_s& operator*() const {
			return current->items[bigIter];
		}

		typename RingPage::item_s& operator*() {
			return current->items[bigIter];
		}

		typename RingPage::item_s* operator->() const {
			return &current->items[bigIter];
		}

		typename RingPage::item_s* operator->() {
			return &current->items[bigIter];
		}

		typename RingPage::item_s* obj() const {
			return &current->items[bigIter];
		}

		bool operator==(const self_type& rhs) const
		{
			return totalIter == rhs.totalIter;
		}

		bool operator!=(const self_type& rhs) const
		{
			return totalIter != rhs.totalIter;
		}

		typename std::iterator_traits<iterator>::difference_type
			// Note the return type here, truly generic
			distance(iterator first_position, iterator second_position,
				std::input_iterator_tag)
		{
			// note the type of the temp variable here, truly generic 
			typename std::iterator_traits<iterator>::difference_type diff;
			for (diff = 0; first_position != second_position; ++first_position, ++diff) 
			{
			}
			return diff;
		}

	};

	iterator begin() const
	{
		return iterator(this);
	}

	iterator end() const
	{
		auto i = iterator(this);
		i.moveToEnd();
		return i;
	}

	iterator find(const K key) const
	{
		RingPage* current = root;
		const uint64_t keyHash = hasher(key);
		int64_t totalOffset = 0;
		int64_t depth = 0;

		while (current)
		{
			auto ringOffset = keyHash % current->size;
			auto item = current->items + ringOffset;
			auto last = item + current->overflow;

			for (; item < last; ++item, ++ringOffset)
			{
				if (item->first == key)
				{
					totalOffset += ringOffset;
					auto it = begin();
					it.moveToLocation(totalOffset, ringOffset, depth);
					return it;
				}
			}

			depth++;
			totalOffset += current->size + current->overflow;
			current = current->nextRing;
		}

		return end();
	}

	iterator erase(const iterator position)
	{
		if (position != end() &&
			!isEmpty(position.obj()))
		{
			auto item = position.obj();
			item->first.~K();
			item->second.~V();
			
			// set memory to voided value
			memset(item, 0xff, sizeof(item));
			--distinct;
		}

		iterator i = position;
		++i; // move iterator to next item
		return i;
	}

	size_t erase(const K& key)
	{
		const iterator it = find(key);

		if (it != end())
		{
			erase(it);
			return 1;
		}

		return 0;
	}

	bool empty() const
	{
		return (distinct == 0);
	}

	size_t size() const
	{
		return distinct;
	}

	size_t count(const K& key) const
	{
		auto it = find(key);
		return (it == end()) ? 0 : 1;
	}

	void clear(bool deleteAll = false)
	{
		if (!deleteAll && (!root || !distinct))
			return;

		auto ring = root;
		auto index = 0;

		while (ring)
		{
			if (index == ring->size + ring->overflow)
			{
				auto t = ring;
				ring = ring->nextRing;
				if (deleteAll || t != root)
					delete t;
				index = 0;
				continue;
			}

			if (!isEmpty(ring->items + index))
			{
				ring->items[index].first.~K();
				ring->items[index].second.~V();
				if (ring == root)
					memset(ring->items + index, 0xff, sizeof ring->items[index]);
			}
			++index;
		}

		branchCount = 0;
		totalBytes = 0;
		distinct = 0;
		if (!deleteAll)
			root->nextRing = nullptr;
			//root->clear();
	}

	~bigRing()
	{
		clear(true);
		root = nullptr;
	}
};


