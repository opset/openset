#pragma once

#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <iostream>

#include "time/epoch.h"
#include "threads/spinlock.h"
#include "include/libcommon.h"

#ifndef _MSC_VER
#include <syslog.h>
#endif

class Logger
{
	enum class level_e : int
	{
		error,
		info,
		debug
	};

	struct Line
	{
		level_e level;
		std::string msg;
		Line(level_e level, std::string msg) : level(level), msg(msg)
		{}
	};

	CriticalSection cs;
	std::vector<Line> lines;
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
	void info(std::string msg)
	{
		if (!loggingOn)
			return;

		cs.lock();
		++backlog;
		lines.emplace_back(Logger::level_e::info, msg );
		cs.unlock();
	}

	void debug(std::string msg)
	{
		if (!loggingOn)
			return;

		cs.lock();
		++backlog;
		lines.emplace_back(Logger::level_e::debug, msg);
		cs.unlock();
	}

	void error(std::string msg)
	{
		if (!loggingOn)
			return;

		cs.lock();
		++backlog;
		lines.emplace_back(Logger::level_e::error, msg);
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
			error(line);
			drain();
			exit(1);
		}
	}

	void fatal(std::string line)
	{
		fatal(false, line);
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
		using namespace std::string_literals;
		backlog = 0;

#ifndef _MSC_VER
		openlog("openset", LOG_NDELAY, LOG_USER);
#endif

		while (true)
		{

			if (!backlog || !cs.tryLock())
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(55));
				continue;
			}

			std::vector<Line> localLines;

			backlog -= lines.size();
			localLines.swap(lines);
			lines.clear();

			cs.unlock();

			auto now = std::chrono::duration_cast<std::chrono::seconds>
				(std::chrono::system_clock::now().time_since_epoch()).count();

			for (auto line : localLines)
			{

				std::string level = (line.level == level_e::info) ? "INFO"s : (line.level == level_e::error) ? "ERROR"s : "DEBUG"s;

				auto txt = Epoch::EpochToISO8601(now) + " " + level + " " + line.msg + "\n";

#ifdef _MSC_VER
				fputs(txt.c_str(), stdout); // so we can print UTF8 characters
#else 
				std::cout << txt; // write to console

				auto LOG_LEVEL = (line.level == level_e::info) ? LOG_INFO : (line.level == level_e::error ? LOG_ERR : LOG_DEBUG);
				syslog(LOG_LEVEL, "%s %s", level.c_str(), line.msg.c_str());
#endif
			}
			
#ifndef _MSC_VER
			std::cout.flush();
#endif
		}

	}
};