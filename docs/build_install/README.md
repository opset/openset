# Building and Installing OpenSet

#### Installation prerequisites

OpenSet is written in modern C++ (using some of the latest features from C++11/14/17). There are around 150k lines of code, as such the most recent compilers are required.

OpenSet incorporates the following amazing and wonderful open source projects:

- [libuv](https://github.com/libuv/libuv), the famous portable async io library written for Node.js. It's extensive and written good old fashioned C, at the moment we primarily use the async TCP components to serve connections.
- [lz4](https://github.com/lz4/lz4), the extremely fast compression/decompression library by [Yann Collet](https://www.linkedin.com/in/yann-collet-5ba1904). Event data and Indexes are compressed in OpenSet. Compression allows us to store 10x the data (or more) while realizing a < 10% performance impact (see the [benchmark](https://github.com/lz4/lz4#benchmarks) on GitHub)
- [xxHash](https://github.com/Cyan4973/xxHash), another masterpiece by  [Yann Collet](https://www.linkedin.com/in/yann-collet-5ba1904). A good hash is hard to find, a fast hash is even harder --- Yann has created both.

You will find recent versions of these projects under [/vendors](#) in the openset repo.

#### Linux requirements

- Unbutu 14.04 (16.04 recommended)
- CMake 3.6+
- gcc 7.2

#### Windows requirements

- A version of Windows made in the last decade.
- Visual Studio 2017 (Version 15.3.4)
- Visual Studio tools for CMake 1.0 (installs CMake 3.6+)

#### Building on Linux

1. Ensure you have the prerequisite build tools listed above.
2. clone this repo (probably in your home directory).
3. build OpenSet release:
```bash
cd openset
mkdir Release
cd Release
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```
or for a debug build:
```bash
cd openset
mkdir Debug
cd Debug
cmake ..
make
```
1. You should now have a file named openset. Copy this to a directory named `openset`and ensure the file has execute permission using `chmod +x openset`

#### Building on Windows

1. Clone this repo.
2. Open the `openset` folder in VisualStudio
3. Open the file `CMakeList.txt`, wait for VisualStudio to index it.
4. In the `Project Settings` dropdown select `x64-Release` or `x64-debug`
5. In the `Select Startup Item` dropdown (next to the Project Settings)  select `openset.exe`
6. From the `CMake` dropdown select `Build All`
7. From the `CMake` dropdown expand `Cache (build type)` and select `Open Cache Folder`
8. You should now have a `Folder` open, and within it `openset.exe`, copy this a folder somewhere called `openset` 

### Running OpenSet

The following command line options can be used

- `--test` runs internal tests to confirm OpenSet is operating correctly.
- `--host'` specifies the ip/hostname to answer on (optional, defaults to 0.0.0.0)
- `--hostext` specifies an external host name that will be broadcast to other nodes. This can may be required for multi-node setups using docker and VMs (defaults to the machine name)
- `--port` specifies the port that to answer on (optional, defaults to 2020)
- `--portext` specifies the external port that will be broadcast to other nodes. This can may be required for multi-node setups using docker and VMs if port mapping is used (defaults to the 2020)
- `--data` path to data if using commits (optional, defaults to current directory `./`)
- `--help` shows the help

When you start OpenSet it will wait in a `ready` state. You must initialize OpenSet in one of two ways to make it `active`.

1. Make it the Cluster Leader by sending an `init_cluster` command, this must be done even you only intend to run a cluster of one.
2. Join it to a cluster. You can connect to any node in a running cluster and send a `invite_node` command. The command will be routed through whichever node is the elected leader, and the target node will be invited into the cluster.
