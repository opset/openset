#pragma once
#include "spinlock.h"

class csLock
{
	CriticalSection& cs;
public:
	explicit csLock(CriticalSection& lock) :
		cs(lock)
	{
		cs.lock();
	}

	~csLock()
	{
		cs.unlock();
	}
};
