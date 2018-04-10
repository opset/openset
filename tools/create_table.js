#!/usr/bin/node

/*
Openset - create_table.js
*/

fs = require('fs')
async = require('async')
openset = require('openset')

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
        name: 'table',
        alias: 't',
        type: String,
    }, {
        name: 'json',
        alias: 'j',
        type: String,
    }])
} catch (ex) {
    var args = {
        help: true
    }
}

console.log('\n\n-----------------------------------------------------')
console.log(' OpenSet - create_table')
console.log('-----------------------------------------------------\n')
console.log(' -h --host <ip address>          // default 127.0.0.1')
console.log(' -p --port <port>                // default 2020')
console.log(' -t --table <name of table>      // no spaces, underscore allowed')
console.log(' -j --json <JSON table file>     //')
console.log('    --help                       // display help')
console.log('')

if (args.help || !args.table || !args.json) {
    console.log('')
    process.exit(0)
}

try {
    var data = require(args.json);
} catch (ex) {
    console.log('! could not open JSON table file.')
    console.log('')
    console.log('')
    process.exit(1)
}

if (!Array.isArray(data)) {
    console.log('! JSON data must be an array.')
    console.log('')
    console.log('')
    process.exit(1)
}

console.log('+ using ' + args.host + ':' + args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

command = {
    action: "create_table",
    params: {
        table: args.table,
        columns: data // should be array
    }
};

conn.rawRequest(
    openset.rpc.admin,
    command,
    (err, data) => {

        if (err) {
            console.log("! error")
            console.log('')
            console.log(err)
            console.log("\n\n")
            process.exit(1)
        }

        console.log(JSON.stringify(JSON.parse(data), null, 4))
        console.log('')
        console.log("+ done\n\n")
        process.exit(0)
    }
);
