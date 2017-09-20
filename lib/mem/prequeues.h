#ifndef RARELOGIC_CPPLIB_MEM_PREQUEUES_H_
#define RARELOGIC_CPPLIB_MEM_PREQUEUES_H_

#include "../threads/locks.h"
#include <deque>

/*

Type T below may be an object or structure and must have "Clear" member
as well as a static "New" that returns a type T*


*/

template <typename T>
class prequeue
{
private:

	CriticalSection _CS;

	int32_t _Max;

	std::deque<T*> _LIFO;

public:

	prequeue()
		: _CS()
	{
		_Max = 10000;
	}

	~prequeue()
	{ }

	T* CheckOut()
	{
		_CS.lock();

		T* Return;

		if (_LIFO.size() == 0)
		{
			Return = T::New();
		}
		else
		{
			Return = _LIFO.back();
			_LIFO.pop_back();
		}

		_CS.unlock();

		return Return;
	}

	void CheckIn(T* ObjectPtr)
	{
		_CS.lock();

		if (_LIFO.size() > _Max)
		{
			ObjectPtr->Clear();
			delete ObjectPtr;
		}
		else
		{
			ObjectPtr->Clear();
			_LIFO.push_back(ObjectPtr);
		}

		_CS.unlock();
	}
};

#endif // RARELOGIC_CPPLIB_MEM_PREQUEUES_H_
