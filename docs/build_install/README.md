# Building and Installing OpenSet

#### Installation prerequisites

OpenSet is written in modern C++ (using some of the latest features from C++11/14/17). There are around 100k lines of code, and you will need a recent compiler to build them.

OpenSet incorporates the following amazing and wonderful open source projects:

- [Simple-Web-Server](https://gitlab.com/eidheim/Simple-Web-Server), a great library by Ole Christian Eidheim that leverages ASIO to provide asynchronous http and https client and server functionality to any C++ project.
- [lz4](https://github.com/lz4/lz4), the extremely fast compression/decompression library by Yann Collet. Event data and Indexes are compressed in OpenSet. Compression allows us to store 10x the data (or more) while realizing a < 10% performance impact (see the [benchmark](https://github.com/lz4/lz4#benchmarks) on GitHub)
- [xxHash](https://github.com/Cyan4973/xxHash), another masterpiece by  Yann Collet. A good hash is hard to find, a fast hash is even harder --- Yann has created both.

You will find recent versions of these projects under [openset/vendor](https://github.com/perple-io/openset/tree/master/vendor) in the OpenSet repo.

#### Linux requirements

- Unbutu 14.04 (16.04 recommended)
- CMake 3.6+
- GCC 7.2+
- GDB 7+ (optional)
- Node.js 5+ (optional for running samples)

> :pushpin: passing `--version` to these tools on the command line will help you verify they are the correct version.  If you need to install these please read the **[build tools](https://github.com/perple-io/openset/blob/master/docs/build_install/build_tools.md)** document.

#### Windows requirements

- A version of Windows made in the last decade.
- Visual Studio 2017 (Version 15.3.4)
- Visual Studio tools for CMake 1.0 (installs CMake 3.6+)

#### Building on Linux

1. Ensure you have the prerequisite build tools listed above.
2. clone this repo (recursively, and probably in your home directory).
```bash
mkdir openset
cd openset
git clone --recursive https://github.com/perple-io/openset.git 
```
3. build OpenSet release:
```bash
cd openset
mkdir Release
cd Release
cmake .. -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 -DCMAKE_BUILD_TYPE=Release 
make
```
&nbsp;&nbsp;&nbsp;&nbsp; or, to build debug:
```bash
cd openset
mkdir Debug
cd Debug
cmake .. -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 
make
```
4. You should now have a file named openset. Copy this to a directory named `openset`and ensure the file has execute permission using `chmod +x openset`

#### Building on Windows

You will need Visual Studio 2017 version 15.4.x (or higher). 

1. Clone this repo (recursively as it has linked repos)
```bash
mkdir openset
cd openset
git clone --recursive https://github.com/perple-io/openset.git 
```
1. Open the `openset` folder in Visual Studio
2. Open the file `CMakeList.txt`, wait for Visual Studio to index it.
3. In the `Project Settings` dropdown select `x64-Release` or `x64-debug`
4. In the `Select Startup Item` dropdown (next to the Project Settings)  select `openset.exe` or `${buildRoot}\openset.exe`.
5. From the `CMake` dropdown select `Build All`
6. From the `CMake` dropdown expand `Cache (build type)` and select `Open Cache Folder`
7. You should now have a `Folder` open, and within it `openset.exe`, copy this file to a folder somewhere else named `openset` 

### Running OpenSet

The following command line options can be used

- `--test` runs internal tests to confirm OpenSet is operating correctly.
- `--host'` specifies the ip/hostname to answer on (optional, defaults to 0.0.0.0)
- `--hostext` specifies an external host name that will be broadcast to other nodes. This can may be required for multi-node setups using docker and VMs (defaults to the machine name)
- `--port` specifies the port that to answer on (optional, defaults to http 8080)
- `--portext` specifies the external port that will be broadcast to other nodes. This can may be required for multi-node setups using docker and VMs if port mapping is used (defaults to the 8080)
- `--data` path to data if using commits (optional, defaults to current directory `./`)
- `--help` shows the help

When you start OpenSet it will wait in a `ready` state. You must initialize OpenSet in one of two ways to make it `active`.

1. Make it the Cluster Leader by sending an `init_cluster` command, this must be done even you only intend to run a cluster of one.
2. Join it to a cluster. You can connect to any node in a running cluster and send a `invite_node` command. The command will be routed through whichever node is the elected leader, and the target node will be invited into the cluster.

> :pushpin: There are samples in the OpenSet samples directory that can get you started.
