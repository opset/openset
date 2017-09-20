#pragma once

#include "threads/event.h"

using openset::Threads::Event;

namespace openset
{
	class Service
	{
	public:
		static void initialize();
		static bool start();
		static bool stop();
		static bool shutdown();
	};
}; // Salt
