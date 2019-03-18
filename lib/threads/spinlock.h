#pragma once

#include <atomic>

#ifdef _MSC_VER
#include <intrin.h> // VC++ intrinsics
#endif

/*
 * Adapted (mostly) from the amazing work and research by
 * David Geier (thank you). We added a trylock function, and 
 * inlined the relax functions
 *
 * git: https://github.com/geidav/spinlocks-bench
 * blog: https://geidav.wordpress.com/tag/spinlock/
 *
 */

#ifndef _MSC_VER
#define __forceinline __attribute__((always_inline))
#endif

class CriticalSection
{
public:
	volatile std::atomic_bool locked{false};

	CriticalSection()
	{};

	CriticalSection(const CriticalSection& other) noexcept
	{}

	CriticalSection(CriticalSection&& other) noexcept
	{
		locked = static_cast<bool>(other.locked);
	}

	__forceinline bool tryLock()
	{
		return !locked.exchange(true, std::memory_order_acq_rel);
	}

	__forceinline void lock()
	{
		if (tryLock()) // I mean, if its there, just grab it
			return;

		do
		{
			while (locked.load(std::memory_order_acquire))
#ifdef _MSC_VER
				_mm_pause();
#else
				__asm__ __volatile__ ("pause");
#endif
		}
		while (locked.exchange(true, std::memory_order_acq_rel));
	}

	__forceinline void unlock()
	{
		locked.store(false, std::memory_order_release);
	}
};
