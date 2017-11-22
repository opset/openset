# Linux Tool Chain

**Disclaimer**

The version numbers for these tools change frequently. Always build on a non-production system, etc.

We cannot guarantee the accuracy of the instructions below. Always check for newer versions and patches of these projects at their respective pages:

* [GCC, the GNU Compiler Collection](https://gcc.gnu.org/)
* [CMake](https://cmake.org/)
* [GDB](https://www.gnu.org/software/gdb/)
* [Node.js](https://nodejs.org/)

> :pushpin: You may want to consider using a VM or Docker for your build environment. Doing so allows you to isolate your build tools, and start over fresh if things go awry. 

## CMake 3.6+

The latest and greatest CMake can always be found [here](https://cmake.org/download/). They even have a `.sh` script you can download that should install it. I've always built it from source.

```
mkdir ~/temp 
cd ~/temp 
wget https://cmake.org/files/v3.9/cmake-3.9.3.tar.gz 
tar xzvf cmake-3.9.3.tar.gz 
cd cmake-3.9.3/

./bootstrap 
make -j4 
sudo make install

cmake --version
```

## GCC 7.x

We can install GCC the package manager way.

```
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update 
sudo apt-get install gcc-7 g++-7

gcc-7 --version
```

## GDB 7.x or 8.x (optional)

GDB is optional, but necessary if you wish to debug on Linux.The copy of GDB on your favorite Ubuntu install is probably as old as time. We can download and build it also. GDB is now in the 8+ range.

```
mkdir ~/temp 
cd ~/temp 

wget http://ftp.gnu.org/gnu/gdb/gdb-8.0.1.tar.xz 
tar -xf gdb-8.0.1.tar.xz 
cd gdb-8.0.1/ 
./configure 
make 

gdb --version
```
You may need this last step to make it available outside the build directory. From the `gdb-8.0.1` Directory type: 
```
sudo cp gdb/gdb /usr/bin/gdb
```

## Node.js (optional) 

Node is used to run samples, you can alternately use `curl`. Install any semi-recent version of Node.js you like (if you have v4, or v5 installed you are probably fine). Current supported versions are 6.x and the fancy new versions are 8.x.

**6.x**
```
curl -sL https://deb.nodesource.com/setup_6.x | sudo -E bash -
sudo apt-get install -y nodejs
```
**or, 8.x**
```
curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
sudo apt-get install -y nodejs
```
