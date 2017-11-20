/*
    Openset - init.js
    ---------------------------------------------------------------------
    Initialize an OpenSet node and create a table for the sample data
*/

fs = require('fs')
async = require('async');
request = require('request');

var args = require('command-line-args')([{
    name: 'host',
    alias: 'h',
    type: String,
    defaultValue: "127.0.0.1"
}, {
    name: 'port',
    alias: 'p',
    type: Number,
    defaultValue: 8080
}])

console.log('\n\n-----------------------------------------------------');
console.log(' OpenSet Sample Initializer');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address> // default 127.0.0.1');
console.log(' -p --port <port>       // default 8080');
console.log('    --help              // display help');
console.log('');

if (args.help) {
    console.log('\n\n');
    process.exit(0);
}

console.log('+ using ' + args.host + ':' + args.port + '\n');

// send commands
async.series([

    function activate_new_cluster(next_step_cb) {

        request.put(
            "http://" + args.host + ":" + args.port + "/v1/cluster/init?partitions=24",
            (err, response, data) => {

                if (err) {
                    console.log(err);
                    process.exit(1);
                }

                var end = Date.now();
                console.log(data);

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function create_table(next_step_cb) {
        command = {
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
        };

        request.post(
            {
                url: "http://" + args.host + ":" + args.port + "/v1/table/highstreet",
                json: command
            },
            (err, response, data) => {

                if (err) {
                    console.log(err);
                    process.exit(1);
                }

                var end = Date.now();
                console.log(data);

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function describe_table(next_step_cb) {

        request.get(
            {
                url: "http://" + args.host + ":" + args.port + "/v1/table/highstreet",
            },
            (err, response, data) => {

                if (err) {
                    console.log(err);
                    process.exit(1);
                }

                var end = Date.now();
                console.log(JSON.stringify(JSON.parse(data), null, 4));

                // move to next config step
                setImmediate(next_step_cb);
            }
        );
    },
    function insert_data(next_step_cb) {
        // this is our sample data
        inserts = require('./data/highstreet_events.json');

        // you can insert up to 1000 events per call
        request.post(
            {
                url: "http://" + args.host + ":" + args.port + "/v1/insert/highstreet",
                json: inserts
            },
            (err, response, data) => {

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