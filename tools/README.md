# Tools


We've bundled OpenSet with a few tools to help you get started. 

> :star: More tools are coming as we build out admin features for OpenSet.

These tools share common dependencies. To install them go to the `openset/tools` directory and type:
```
npm install
```

#### The tools

|        Tool              | Description                                                                                                                                                                                                                                |
| :----------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|        `init_cluster.js` | OpenSet starts in standby mode awaiting a role. You must make one node leader to start a cluster. Even if you intend to run a single node installation, you must initialize the cluster                                                    |
|        `invite_node.js`  | To add more freshly started nodes (in standby) to an initialized cluster.                                                                                                                                                                  |
|        `create_table.js` | Creates a table. This requires that you provide a JSON input file containing the table definition. There is an example [here]](https://github.com/perple-io/openset/blob/master/samples/pyql/highstreet_table.json)                        |
|        `insert_json.js`  | A bulk import tool. You provide a directory containing JSON files. The files must have one JSON document per line, each line must define an event. Note: if your file names sort numerically they will be imported from lowest to highest. |



> :pushpin: passing `--help` to any tool will display help.



---
#### The MIT License

Copyright (c) 2015-2017, Seth Hamilton and Perple Corp.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.



