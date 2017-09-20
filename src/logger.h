#pragma once

#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <iostream>

#include "time/epoch.h"
#include "threads/spinlock.h"
#include "include/libcommon.h"

class Logger
{
	CriticalSection cs;
	std::vector<std::string> lines;
	std::atomic<int> backlog;

	bool loggingOn{ true };

	Logger()
	{
		std::thread runner(&Logger::logLoop, this);
		runner.detach();
	}

	~Logger()
	{}

public:
	void info(char Code, std::string line)
	{
		if (!loggingOn)
			return;

		auto now = std::chrono::duration_cast<std::chrono::milliseconds>
			(std::chrono::system_clock::now().time_since_epoch()).count();

		line = Epoch::EpochToISO8601(now) + "  " + std::string(&Code, 1) + " " + line;

		cs.lock();
		++backlog;
		lines.emplace_back(std::move(line));
		cs.unlock();
	}

	void drain() const
	{
		auto count = 0;
		while (backlog)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(55));
			if (++count == 25)
				break;
		}
	}

	void fatal(bool isGood, std::string line) 
	{
		if (!isGood)
		{
			info('!', line);
			drain();
			exit(1);
		}
	}

	void suspendLogging(bool suspend)
	{
		loggingOn = !suspend;
	}
	
	static Logger& get()
	{
		static Logger log;
		return log;
	}

private:
	void logLoop()
	{
		backlog = 0;

		while (true)
		{

			if (!backlog || !cs.tryLock())
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(55));
				continue;
			}

			std::vector<std::string> localLines;

			backlog -= lines.size();
			localLines.swap(lines);
			lines.clear();

			cs.unlock();

			std::string block;

			for (auto line : localLines)
				block += line + '\n';

#ifdef _MSC_VER
			fputs(block.c_str(), stdout); // so we can print UTF8 characters
#else
			std::cout << block;
#endif
		}

	}
};