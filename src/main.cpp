// main.cpp : Entry point for OpenSetDB
//
#include "common.h"
#include "ver.h"
#include "service.h"
#include "config.h"
#include "logger.h"
#include "../test/unittests.h"
#include <string>

#ifdef _MSC_VER
#include <WinSock2.h>
#include <wincon.h>
#endif

using namespace std::string_literals;

void StartOpenSet(openset::config::CommandlineArgs args)
{

#ifdef _MSC_VER
	// Goofy windows socket subsystem init. 
	// We shouldn't have to call this nowadays Microsoft
	WSADATA wsaData;
	const auto wVersionRequested = MAKEWORD(2, 0);
	const auto err = WSAStartup(wVersionRequested, &wsaData);

	if (err != 0) {
		cout << "! could not initialize sockets." << endl;
		exit(1);
	}

	// Set colors windows style
	SetConsoleCP(CP_UTF8);
	SetConsoleOutputCP(CP_UTF8);
	SetConsoleTextAttribute(GetStdHandle(STD_OUTPUT_HANDLE), 12);
#endif		

	const auto banner = R"banner(
  _______  _______  _______  __    _  _______  _______  _______ 
 |       ||       ||       ||  \  | ||       ||       ||       |
 |   _   ||    _  ||    ___||   \ | ||  _____||    ___||_     _|
 |  | |  ||   |_| ||   |___ |    \| || |_____ |   |___   |   |  
 |  | |  ||    ___||    ___||       ||_____  ||    ___|  |   |  
 |  |_|  ||   |    |   |___ | |\    | _____| ||   |___   |   |  
 |       ||   |    |       || | \   ||       ||       |  |   |  
 |_______||___|    |_______||_|  \__||_______||_______|  |___|  
     
)banner";

	cout << "\x1b[1;31m\b\b\b\b\b\b\b       " << endl;
	cout << banner << "        \x1b[1;30m\b\b\b\b\b\b\b             " << endl;

#ifdef _MSC_VER
	SetConsoleTextAttribute(GetStdHandle(STD_OUTPUT_HANDLE), 8);
#endif
	cout << " v" << __version__ << " | OpenSet, Copyright(c) 2015 - 2019, Crowd Conduct Corp." << endl;
	cout << "\x1b[0m\b\b\b\b        " << endl;

#ifdef _MSC_VER
	SetConsoleTextAttribute(GetStdHandle(STD_OUTPUT_HANDLE), 7);
#endif

	args.fix(); // fix the default startup arguments after WSAStartup (on windows)

	// initialize our global config object
	openset::globals::running = new openset::config::Config(args);

	// Fire this bad boy up (main loop)
	openset::Service::start();
}

int main(const int argc, char* argv[])
{
	openset::config::CommandlineArgs args;

	auto help = false;
	auto test = false;
	
	if (argc)
	{
		for (auto i = 1; i < argc; ++i)
		{
			const std::string arg(argv[i]);
			const std::string nextArg(i == argc - 1 ? "" : argv[i + 1]);

			if (arg == "--host"s)
				args.hostLocal = nextArg;
			else if (arg == "--port"s)
				args.portLocal = std::stoi(nextArg);
			if (arg == "--os-host"s)
				args.hostExternal = argv[i + 1];
			else if (arg == "--os-port"s)
				args.portExternal = std::stoi(nextArg);
			else if (arg == "--data"s)
				args.path = argv[i + 1];
			else if (arg == "--test"s)
				test = true;
			else if (arg == "--help"s)
				help = true;
		}
	}

  	if (test)
	{
		const auto testRes = unitTest();
		exit(testRes ? 0 : 1); // exit with 1 on test fail
	}

	if (help)
	{
		cout << "Command line options:" << endl << endl;
		cout << "    --host     <ip, defaults to 0.0.0.0>" << endl;
		cout << "    --port     <port, defaults to 8080>" << endl;
		cout << "    --os-host  <host/ip, defaults to hostname>  ; optional external host/ip" << endl;
		cout << "    --os-port  <port, defaults to --port value> ; optional external port" << endl;
		cout << "    --data     <relative or absolute path>      ; where commits will be stored" << endl;
		cout << "    --test                                      ; will run unit tests" << endl;
		cout << endl;
		exit(0);
	}

	StartOpenSet(args);

	// drain the log
	Logger::get().drain();

	return 0;
}
