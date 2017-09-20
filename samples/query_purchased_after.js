fs = require('fs')
openset = require('openset');

var args = require('command-line-args')([
  { name: 'host', alias: 'h', type: String, defaultValue: "127.0.0.1" },
  { name: 'port', alias: 'p', type: Number, defaultValue: 2020 },
  { name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
])

console.log('\n\n-----------------------------------------------------');
console.log(' OpenSet Sample Simple Query');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address>  // default 127.0.0.1');
console.log(' -p --port <port>        // default 2020');
console.log(' -d --debug              // debug the script')
console.log('');

console.log('+ using ' + args.host + ':'+ args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

// load a pyql file
pyql = fs.readFileSync("./pyql/purchased_after.pyql", "utf-8")

var queryBlock = {
    "action": "query",
    "params": {
        "table": "highstreet",
        "code": pyql
    }
};

conn.rawRequest(
    openset.mode.query_pql,
    queryBlock,
    (err, data) => {

        if (err)
        {
            console.log("insert error: " + err + "\n\n");
            process.exit(1)
        }

        var dataLength = data.length;

        console.log('--------------------------------------------\n');

        if (args.debug) { // if this was a debug query, we will format the output

            parsed = JSON.parse(data);

            if (parsed.debug)
                console.log(parsed.debug)
            else
                console.log(data);
        }
        else {
            console.log(JSON.stringify(JSON.parse(data), null, 4));
        }


        console.log('--------------------------------------------\n');
        console.log( "bytes returned: " + dataLength );
        console.log('\n');

        process.exit()
    }
);