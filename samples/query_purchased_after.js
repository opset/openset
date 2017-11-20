fs = require('fs')
request = require('request');

var args = require('command-line-args')([
  { name: 'host', alias: 'h', type: String, defaultValue: "127.0.0.1" },
  { name: 'port', alias: 'p', type: Number, defaultValue: 8080 },
  { name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
])

console.log('\n\n-----------------------------------------------------');
console.log(' OpenSet Sample Simple Query');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address>  // default 127.0.0.1');
console.log(' -p --port <port>        // default 8080');
console.log(' -d --debug              // debug the script')
console.log('');

console.log('+ using ' + args.host + ':'+ args.port + '\n');

// load a pyql file
pyql = fs.readFileSync("./pyql/purchased_after.pyql", "utf-8")

debug = args.debug ? "?debug=true" : "";

request.post(
    {
        url: "http://" + args.host + ":" + args.port + "/v1/query/highstreet/events" + debug,
        body: pyql // raw text
    },
    (err, response, data) => {

        if (err)
        {
            console.log("query error: " + err + "\n\n");
            process.exit(1)
        }

        var dataLength = data.length;

        console.log('--------------------------------------------\n');

        if (args.debug) // if this was a debug query, just print the raw text
            console.log(data);
        else
            console.log(JSON.stringify(JSON.parse(data), null, 4));

        console.log('--------------------------------------------\n');
        console.log( "bytes returned: " + dataLength );
        console.log('\n');

        process.exit()
    }
);