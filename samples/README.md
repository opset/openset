# Samples

OpenSet samples are provided in Node.js. Be sure to have a recent version of node installed.

These examples use the popular node `request` module, but any http client library will work as well as the ever useful `async` module.  To install these modules go to the `openset/samples` directory and type:
```
npm install
```

#### Sample Data and Scripts

- Sample __data__ is found in [openset/samples/data](https://github.com/perple-io/openset/tree/master/samples/data)
- Sample __pyql__ scripts are found in [openset/samples/pyql](https://github.com/perple-io/openset/tree/master/samples/pyql)

#### Init OpenSet and load sample data into `highstreet` table.

Make sure OpenSet is started, then type the following from the `openset/samples` directory.
```
node init.js 
```
> :pushpin: passing `--help` to any sample will show help.

#### Test Programs

A few programs have been included that execute the pyql sample queries in the pyql directory. These scripts use make basic REST requests using http methods:

- `node query_simple.js`
- `node query_total_shipper.js`
- `node query_tags_and_products.js`
- `node query_purchased_after.js`

We will add more samples soon.
___

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



