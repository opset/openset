#!/usr/bin/node

/*
Openset - init_cluster.js
*/

fs = require('fs')
async = require('async');
openset = require('openset');

try {
    var args = require('command-line-args')([{
        name: 'help',
        type: String
    }, {
        name: 'host',
        alias: 'h',
        type: String,
        defaultValue: "127.0.0.1"
    }, {
        name: 'port',
        alias: 'p',
        type: Number,
        defaultValue: 2020
    }, {
        name: 'num',
        alias: 'n',
        type: Number,
    }])
} catch (ex) {
    var args = {
        help: true
    }
}

console.log('\n\n---------------------------------------------------');
console.log(' OpenSet - init_cluster');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address>          // default 127.0.0.1');
console.log(' -p --port <port>                // default 2020');
console.log(' -n --num <number of partitions> //');
console.log('    --help                       // display help');
console.log('');

if (!args.num || args.help) {
    console.log('Note: For single nodes setups the "number of partitions"');
    console.log('      should be equal to the number of cores or hyperthreaded');
    console.log('      cores. For multi-node clusters the number of paritions');
    console.log('      should be 10-20% higher than the number of processor cores');
    console.log('      that will ultimately be in the cluster.');
    console.log('\n\n');
    process.exit(0);
}

console.log('+ using ' + args.host + ':' + args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

command = {
    action: "init_cluster",
    params: {
        partitions: args.num
    }
};

conn.rawRequest(
    openset.mode.admin,
    command,
    (err, data) => {

        if (err) {
            console.log("! error");
            console.log('');
            console.log(err);
            console.log("\n\n");
            process.exit(1)
        }

        console.log(JSON.stringify(JSON.parse(data), null, 4));
        console.log('');
        console.log("+ done\n\n");
        process.exit(0);
    }
);
