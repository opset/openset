// main.cpp : Entry point for OpenSetDB
//
#include "common.h"
#include "ver.h"
#include "service.h"
#include "config.h"
#include "logger.h"
#include "../test/unittests.h"
#include "var/var.h"
#include <string>
#include <thread>

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
    const auto err               = WSAStartup(wVersionRequested, &wsaData);

    if (err != 0)
    {
        Logger::get().error("Could not initialize socket.");
        Logger::get().drain();
        exit(1);
    }

#endif

    Logger::get().info("OpenSet v" + __version__);
    Logger::get().info("OpenSet, Copyright(c) 2015 - 2019, Seth Hamilton.");

    //const auto workerCount = 16;// TODO make this a switch std::thread::hardware_concurrency();
    //Logger::get().info(to_string(workerCount) + " processor cores available.");

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
