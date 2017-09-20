#!/usr/bin/node

/*
Openset - insert_json.js
*/

fs = require('fs')
async = require('async');
openset = require('openset');
readline = require('readline')

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

console.log('\n\n-----------------------------------------------------');
console.log(' OpenSet - insert_json');
console.log('-----------------------------------------------------\n');
console.log(' -h --host <ip address>             // default 127.0.0.1');
console.log(' -p --port <port>                   // default 2020');
console.log(' -t --table <name of table>         // existing table');
console.log(' -j --json <JSON source directory>  // see note');
console.log('    --help                          // display help');
console.log('');

if (args.help || !args.table || !args.json) {
    console.log('Note: JSON event files must be formated as one JSON document');
    console.log('       per line. Files should be named in a sortable way.');
    console.log('\n\n');
    process.exit(0);
}

console.log('+ using ' + args.host + ':' + args.port + '\n');

// init the OpenSet SDK
conn = openset.init({
    host: args.host,
    port: args.port
});

sourceDir = args.json;

// clean up any trailing slash
if (sourceDir[sourceDir.length - 1] == '/')
    sourceDir = sourceDir.substr(0, sourceDir.length - 1);

lines = []
count = 0;
bytes = 0;
start = Date.now();
rl = null; // readline object

function insert(inserts, done_cb) {

    block = {
        "table": args.table,
        "events": inserts
    }

    conn.rawRequest(
        openset.mode.insert_async,
        block,
        (err, data) => {

            console.log(data)

            var linesPerSec = count / ((Date.now() - start) / 1000);
            var bytesPerSec = bytes / ((Date.now() - start) / 1000);

            console.log(
                '@ ' +
                String(count).replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ', ' +
                String(Math.round(linesPerSec)).replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' lines/sec, ' +
                String(Math.round(bytesPerSec)).replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' bytes/sec')

            setTimeout(done_cb);
        }
    );
}

// shuttleLines - pauses/resumes file file reading
// and queues up blocks up to 1000 events
! function shuttleLines() {

    if (!lines.length) {
        if (rl && lines.length < 25000)
            rl.resume()
        setTimeout(shuttleLines, 50)
        return;
    }

    if (lines.length < 5000) {
        setTimeout(shuttleLines, 50)
        return;
    }

    var inserts = []

    while (lines.length) {

        var row = lines.shift()
        if (row) {
            inserts.push(row)
            count++
        }

        if (inserts.length >= 1000) // we made a 1000 line payload
            break
    }

    if (rl && lines.length < 25000) // low limit, resume log reading
        rl.resume()

    // send payload
    insert(inserts, function () {
        if (lines.length)
            setImmediate(shuttleLines) // run again
        else
            setTimeout(shuttleLines, 1000) // wait for some data to queue up
    })

}() // autostart

function parseLog(fileName, done_cb) {

    rl = readline.createInterface({
        input: fs.createReadStream(sourceDir + '/' + fileName)
    })

    rl.on('line', (json) => {

        bytes += json.length + 1;

        try {
            var j = JSON.parse(json)
        } catch (e) {
            console.log('bad line')
            return
        }

        lines.push(j)

        if (lines.length >= 50000) // high limit, pause log reading
            rl.pause()
    })

    rl.on('close', () => {
        console.log('got close')
        rl.close()
        rl = null
        setImmediate(done_cb)
    })
}

function createImportList(mask, done_cb) {

    fs.readdir(sourceDir, (err, items) => {

        items.sort((a, b) => {
            if (a > b) return 1
            if (a < b) return -1
            return 0
        })

        console.log('+ found ' + items.length + ' logs in ' + sourceDir + '.\n\n');
        setImmediate(done_cb, items)

    })
}

// get a list, and then iterate it
createImportList('', (files) => {

    async.eachLimit(
        files,
        1,
        (file, next_cb) => {

            console.log('+ reading ', file)

            parseLog(file, () => {
                setImmediate(next_cb)
            })
        },
        (err) => {

            console.log('+ imported ' + String(bytes).replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' bytes of data.');
            console.log('+ all done.')
            process.exit(1)
        }
    )

})