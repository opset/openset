#!/usr/bin/node

/*
Openset - invite_node.js
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
        },
        {
            name: 'port',
            alias: 'p',
            type: Number,
            defaultValue: 2020
        },
        {
            name: 'newhost',
            alias: 'n',
            type: String
        },
        {
            name: 'newport',
            alias: 'o',
            type: Number
        },
])
} catch (ex) {
    var args = {
        help: true
    }
}

console.log('\n\n-----------------------------------------------------');
console.log(' OpenSet - invite_node');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address>    // default 127.0.0.1');
console.log(' -p --port <port>          // default 2020');
console.log(' -n --newhost <ip address> // required');
console.log(' -o --newport <port>       // required');
console.log('    --help                 // display help');
console.log('');


if (args.help || !args.newhost || !args.newport) {
    console.log('');
    process.exit(0);
}

console.log('+ using ' + args.host + ':' + args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

command = {
    "action": "invite_node",
    "params": {
        "host": args.newhost,
        "port": args.newport
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
