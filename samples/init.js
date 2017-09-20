/*
    Openset - init.js
    ---------------------------------------------------------------------
    Initialize an OpenSet node and create a table for the sample data
*/

fs = require('fs')
async = require('async');
openset = require('openset');

var args = require('command-line-args')([{
    name: 'host',
    alias: 'h',
    type: String,
    defaultValue: "127.0.0.1"
}, {
    name: 'port',
    alias: 'p',
    type: Number,
    defaultValue: 2020
}])

console.log('\n\n-------------------------------------------------');
console.log(' OpenSet Sample Initializer');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address> // default 127.0.0.1');
console.log(' -p --port <port>       // default 2020\n');

console.log('+ using ' + args.host + ':' + args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

// send commands
async.series([

    function activate_new_cluster(next_step_cb) {
        command = {
            action: "init_cluster",
            params: {
                partitions: 4
            }
        };

        conn.rawRequest(
            openset.mode.admin,
            command,
            (err, data) => {

                var end = Date.now();
                console.log(data);

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function create_table(next_step_cb) {
        command = {
            action: "create_table",
            params: {
                table: "highstreet",
                columns: [{
                    name: "product_name",
                    type: "text"
                }, {
                    name: "product_price",
                    type: "double"
                }, {
                    name: "product_shipping",
                    type: "double"
                }, {
                    name: "shipper",
                    type: "text"
                }, {
                    name: "total",
                    type: "double"
                }, {
                    name: "shipping",
                    type: "double"
                }, {
                    name: "product_tags",
                    type: "text"
                }, {
                    name: "product_group",
                    type: "text"
                }, {
                    name: "cart_size",
                    type: "int"
                }]
            }
        };

        conn.rawRequest(
            openset.mode.admin,
            command,
            (err, data) => {

                var end = Date.now();
                console.log(data);

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function describe_table(next_step_cb) {
        command = {
            action: "describe_table",
            params: {
                table: "highstreet"
            }
        };

        conn.rawRequest(
            openset.mode.admin,
            command,
            (err, data) => {

                var end = Date.now();
                console.log(JSON.stringify(JSON.parse(data), null, 4));

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function insert_data(next_step_cb) {
        // this is our sample data
        data = require('./data/highstreet_events.json');

        // you can insert up to 1000 events per call
        var eventBlock = {
            "table": "highstreet",
            "events": data
        };

        conn.rawRequest(
            openset.mode.insert_async,
            eventBlock,
            (err, data) => {

                if (err)
                    console.log("insert error: " + err + "\n\n");
                else
                    console.log("insert complete.\n\n")

                setImmediate(next_step_cb)
            }
        );
    }
], (err) => {
    // all done
    console.log("+ done\n\n")
    process.exit();
});