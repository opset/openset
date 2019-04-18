![OpenSet](docs/img/openset_compact.svg)

OpenSet is a MIT licensed programmable engine for rapidly extracting behavior from user event data.

If you have apps, websites, or IoT devices that generate user event data, you might find OpenSet really useful.

OpenSet is a streaming solution and can ingest data at up to 35,000 lines per second per node (fully indexed and replicated). OpenSet has been tested on datasets with millions of users and billions of rows.

| Platform    | Version | Info                            | Status                                                                                                                                                                     |
| :---------- | :-----: | :------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Linux x64   | 0.2.12  | gcc 7.2, release, debug         | [![Build Status](https://travis-ci.org/opset/openset.svg?branch=master)](https://travis-ci.org/opset/openset)                                                              |
| Windows x64 | 0.2.12  | Visual C++ 2017, release, debug | [![Build status](https://ci.appveyor.com/api/projects/status/pr8jrhfth2bt7j6r/branch/master?svg=true)](https://ci.appveyor.com/project/SethHamilton/openset/branch/master) |

:coffee: **OpenSet is currently in alpha. Please see v0.2.12 release notes below **

## Links

-   [Documentation](https://github.com/perple-io/openset/tree/master/docs)
-   [Docker Images](https://github.com/perple-io/openset/tree/master/docs/docker)
-   [Sample Code](https://github.com/opset/openset_samples)
-   [Admin Tools](https://github.com/perple-io/openset/tree/master/tools)

## Strong Points

1. Conditional Triggering (Re-eventing)
2. Sequence Extraction
3. Analytics & Segmentation
4. Person Extraction

### 1. Conditional Triggering (Re-eventing)

![Segments](docs/img/re-event-matrix.svg)

Translate live event streams into behavioral event streams. OpenSet can ingest high speed data, and emit behavioral events for individual people from that stream.

For example:

-   people who used your app more than 5 times.
-   people that have been actively using your app for 90 days.
-   people that have not used feature X in 30 days.
-   people that used feature X but not feature Y within their first 14 days.
-   people who spent at least $1000 in your app store a year ago, but has spent less than $1000 in the last 365 days.
-   people who were in Spain visited Spain but did not visit another European destination within 180 days.

The event emitted will contain a time, an event name and the user ID that triggered the event. Events are emitted within milliseconds of their due time, where they are queued for consumption. Queues listeners can be configured to receive complete subscriptions, or share a subscription to allow for round robin message distribution.

### 2. Sequence Extraction

![Segments](docs/img/chains-of-events.svg)

Extract the common sequences that lead people to a target behavior.

-   extract the countries of origin for people that visited a given destination.
-   extract the sequence of countries visited by people that arrived at a given destination.
-   extract the app features used by customers before upgrading to your premium plan.

Extract the next thing, or the next _n_ things people do after they perform a target behavior:

-   what products are commonly purchased near product X
-   what are products purchased within the next 30 days after product X
-   what features do users utilize after feature X.

Extract sequences between a source and target behavior:

-   What countries do people visit after visiting country X, but before visiting country Y.
-   What site features do customers use after coming from promo X but before signing up.

### 3. Analytics & Segmentation

OpenSet can generate multi-level pivots with custom aggregations and population counts at parent and child nodes branches. You can specify up to 16 pivot depths, with combinations of event properties, counts, duration values, or date values.

![Pivots](docs/img/pivots.svg)

OpenSet brings behavior to segmentation. With OpenSet you can cluster people into segments using event attributes as well as event sequences. Derivative segments can be generated from other segments using intersection, union, complement and difference. Segments can even be compared to extract differences between their respective populations.

![Segments](docs/img/segment_circles.svg)

### 4. Person Extraction

Load or stream events (app, web, IoT, etc) into OpenSet and extract the history and attributes for single users in milliseconds. It doesn't matter if you have millions of events, or you are inserting thousand of events per second, OpenSet extract users by ID in real-time.

![Segments](docs/img/one-person.svg)

# example using curl

**1**. Clone the Samples:

```bash
cd ~
git clone https://github.com/opset/openset_samples.git
```

**2**. Install [Docker](https://www.docker.com/) and start OpenSet (interactive):

```bash
docker run -p 8080:8080 -e OS_HOST=127.0.0.1 -e OS_PORT=8080 --rm=true -it opset/openset_x64_rel:0.2.12
```

> **Note** The newest docker build can be found on [dockerhub](https://cloud.docker.com/u/opset/repository/docker/opset/openset_x64_rel).

**3**. Open another console (go to home directory):

```bash
cd ~
```

**4**. Initialize OpenSet:

> `json_pp` is part of most linux distributions. If you don't want pretty printed json, remove `| json_pp`

```python
curl -X PUT http://127.0.0.1:8080/v1/cluster/init?partitions=24 | json_pp
```

response:

```json
{
    "server_name": "smiling-donkey"
}
```

**5**. Create a table:

```bash
curl \
-X POST  http://127.0.0.1:8080/v1/table/highstreet \
-d @- << EOF | json_pp
{
    "columns": [
        {"name": "order_id", "type": "int"},
        {"name": "product_name", "type": "text"},
        {"name": "product_price", "type": "double"},
        {"name": "product_shipping", "type": "double"},
        {"name": "shipper", "type": "text"},
        {"name": "total", "type": "double"},
        {"name": "shipping", "type": "double"},
        {"name": "product_tags", "type": "text", "is_set": true},
        {"name": "product_group", "type": "text", "is_set": true},
        {"name": "cart_size", "type": "int"}
    ],
    "z_order": [
      "purchase",
      "cast_item"
    ]
}
EOF
```

response:

```json
{
    "message": "created",
    "table": "highstreet"
}
```

**6**. Let's insert some json data from the `openset_samples/data` folder:

> clone the (opset/openset_samples)[]

```bash
curl \
-X POST http://127.0.0.1:8080/v1/insert/highstreet \
--data-binary @openset_samples/data/highstreet_events.json | json_pp
```

response:

```json
{
    "message": "yummy"
}
```

> :pushpin: view the event data [here](https://github.com/perple-io/openset/blob/master/samples/data/highstreet_events.json)

**7**. Let's run a PyQL `event` query

This script can be found at `openset_samples/pyql/simple.pyql` folder.

```bash
curl \
-X POST http://127.0.0.1:8080/v1/query/highstreet/event \
--data-binary @- << PYQL | json_pp
# our pyql script

aggregate: # define our output columns
    count id
    count product_name as purchased
    sum product_price as total_spent with product_name

# iterate events where the product_group set contains 'outdoor'
# and product_name is 'fly_rod' or 'guilded_spoon'
for row in rows if
        'outdoor' in product_group and
        product_name in ['fly rod', 'gilded spoon']:

    # make a branch: /day_of_week/product_name
    # and aggregate person, product purchase, and total
    tally(get_day_of_week(row['stamp']), product_name)

#end of pyql script
PYQL
```

response (counts are people, count product, sum price):

```json
{
    "_": [
        {
            "g": 2,
            "c": [1, 3, 155.93],
            "_": [
                {
                    "g": "triple hook jigger",
                    "c": [1, 1, 27.99]
                },
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                },
                {
                    "g": "deluxe spinner",
                    "c": [1, 1, 27.99]
                }
            ]
        },
        {
            "g": 5,
            "c": [1, 1, 99.95],
            "_": [
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                }
            ]
        },
        {
            "g": 7,
            "c": [1, 3, 145.9299],
            "_": [
                {
                    "g": "gilded spoon",
                    "c": [1, 1, 27.99]
                },
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                },
                {
                    "g": "double spinner",
                    "c": [1, 1, 17.9899]
                }
            ]
        }
    ]
}
```

**8**. Let's make 4 segments (this script can be found at `openset_samples/pyql/segments.pyql` folder):

```bash
curl \
-X POST http://127.0.0.1:8080/v1/query/highstreet/segment \
--data-binary @- << PYQL | json_pp
# our pyql script

@segment products_home ttl=300s use_cached=True refresh=300s

# match one of these
for row in rows if
        product_group in ['basement', 'garage', 'kitchen', 'bedroom', 'bathroom']:
    tally

@segment products_yard ttl=300s use_cached=True refresh=300s

# match one of these
for row in rows if
        product_group in ['basement', 'garage']:
    tally

@segment products_outdoor ttl=300s use_cached=True refresh=300s

# match one of these
for row in rows if
        product_group in ['outdoor', 'angling']:
    tally

@segment products_commercial ttl=300s use_cached=True refresh=300s

# match one of these
for row in rows if
        product_group == 'restaurant':
    tally

#end of pyql script
PYQL
```

response (counts are people):

```json
{
    "_": [
        {
            "g": "products_commercial",
            "c": [2]
        },
        {
            "g": "products_home",
            "c": [2]
        },
        {
            "g": "products_outdoor",
            "c": [2]
        },
        {
            "g": "products_yard",
            "c": [1]
        }
    ]
}
```

**9**. Let's query a column:

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/column/product_name' | json_pp
```

response (counts are people):

```json
{
    "_": [
        {
            "g": "product_name",
            "c": [3],
            "_": [
                {
                    "g": "panini press",
                    "c": [2]
                },
                {
                    "g": "fly rod",
                    "c": [2]
                },
                {
                    "g": "shag rug",
                    "c": [2]
                },
                {
                    "g": "espresso mmachine",
                    "c": [1]
                },
                {
                    "g": "triple hook jigger",
                    "c": [1]
                },
                {
                    "g": "gilded spoon",
                    "c": [1]
                },
                {
                    "g": "double spinner",
                    "c": [1]
                },
                {
                    "g": "deluxe spinner",
                    "c": [1]
                },
                {
                    "g": "grommet",
                    "c": [1]
                }
            ]
        }
    ]
}
```

**10**. Let's query a column in segment compare mode (all `*` against `products_outdoor`:

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/column/product_name?segments=*,products_outdoor' | json_pp
```

response (counts are people for each segment):

```json
{
    "g": "product_name",
    "c": [3],
    "c2": [2],
    "_": [
        {
            "_": [
                {
                    "g": "panini press",
                    "c": [2],
                    "c2": [1]
                },
                {
                    "g": "fly rod",
                    "c": [2],
                    "c2": [2]
                },
                {
                    "g": "shag rug",
                    "c": [2],
                    "c2": [1]
                },
                {
                    "g": "espresso mmachine",
                    "c": [1],
                    "c2": [0]
                },
                {
                    "g": "triple hook jigger",
                    "c": [1],
                    "c2": [1]
                },
                {
                    "g": "gilded spoon",
                    "c": [1],
                    "c2": [1]
                },
                {
                    "g": "double spinner",
                    "c": [1],
                    "c2": [1]
                },
                {
                    "g": "deluxe spinner",
                    "c": [1],
                    "c2": [1]
                },
                {
                    "g": "grommet",
                    "c": [1],
                    "c2": [0]
                }
            ]
        }
    ]
}
```

**11.** Let's query a numeric column and `bucket` the results by `50` dollar increments

> :pushpin: note that the distinct user counts are properly counted per bucket. This is useful for making a column histogram.

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/column/product_price?bucket=50' | json_pp
```

response (counts are people):

```json
{
    "g": "product_price",
    "c": [3],
    "_": [
        {
            "_": [
                {
                    "g": 0,
                    "c": [3]
                },
                {
                    "g": 50,
                    "c": [3]
                },
                {
                    "g": 600,
                    "c": [1]
                }
            ]
        }
    ]
}
```

**12.** Let's do a histogram query using an aggregator

Let's generate a histogram of cart `total`, and bucket by `250` starting at `0` for each person in the database.

The pyql query in the POST body returns a value generated using an inline aggregation function on the database column `table`.

```bash
curl \
-X POST 'http://127.0.0.1:8080/v1/query/highstreet/histogram/customer_value?bucket=250&min=0' \
--data-binary @- << PYQL | json_pp
# our pyql script

return SUM total if total != None # inline aggregation

#end of pyql script
PYQL
```

response (counts are people):

```json
{
    "_": [
        {
            "g": "customer_value",
            "c": [3],
            "_": [
                {
                    "g": 750,
                    "c": [1]
                },
                {
                    "g": 500,
                    "c": [0]
                },
                {
                    "g": 250,
                    "c": [1]
                },
                {
                    "g": 0,
                    "c": [1]
                }
            ]
        }
    ]
}
```

**13.** Let's do a another histogram query using time

Let's generate a histogram breaking down the number of weeks since `last_event` for each person in the database.

```bash
curl \
-X POST 'http://127.0.0.1:8080/v1/query/highstreet/histogram/days_since' \
--data-binary @- << PYQL | json_pp
# our pyql script

return int(to_days(now - last_event) / 7)

#end of pyql script
PYQL
```

response (counts are people):

```json
{
    "_": [
        {
            "g": "days_since",
            "c": [3],
            "_": [
                {
                    "g": 48,
                    "c": [1]
                },
                {
                    "g": 47,
                    "c": [2]
                }
            ]
        }
    ]
}
```

**14.** Let's do a sequence query.

Let's extract `for each product` the `first` product purchased `immediately after` but `not in the same cart`.

```bash
curl \
-X POST http://127.0.0.1:8080/v1/query/highstreet/event \
--data-binary @- << PYQL | json_pp
# our pyql script

aggregate: # define our output columns
    count id
    count product_name as purchased
    sum product_price as total_revenue

# STEP 1
# search for a purchase events
for purchase_row in rows
    if event == 'purchase':

    # products will hold the `product_name`s that
    # are found in the `cart_item`s associated with
    # the above purchase
    products = set()

    # STEP 2
    # gather the product names for the above purchase
    continue for item_row in rows if
            event == 'cart_item' and
            order_id == purchase_row['order_id']:

        products.add(item_row['product_name'])

    # STEP 3
    # find the just the NEXT purchase (continue for 1)
    continue for 1 sub_purchase_row in rows if
            event == 'purchase' and
            order_id != purchase_row['order_id']: # match one

        # STEP 4
        # for each 'cart_item' row
        # iterate the products capture in
        # Step 2 with the product_name in the row
        continue for sub_item_row in rows if
                event == 'cart_item' and
                order_id == sub_purchase_row['order_id']:

            for product in products:
                # Tally counts for each product in the
                # subusequent purchase for each product in
                # the first match

                if product == sub_item_row['product_name']:
                    continue

                tally(product, sub_item_row['product_name'])

# loop to top

#end of pyql script
PYQL
```

response (counts are people, count product, sum price):

```json
{
    "_": [
        {
            "g": "shag rug",
            "c": [2, 4, 885.92],
            "_": [
                {
                    "g": "panini press",
                    "c": [2, 2, 135.98]
                },
                {
                    "g": "espresso mmachine",
                    "c": [1, 1, 649.99]
                },
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                }
            ]
        },
        {
            "g": "panini press",
            "c": [1, 3, 145.9299],
            "_": [
                {
                    "g": "gilded spoon",
                    "c": [1, 1, 27.99]
                },
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                },
                {
                    "g": "double spinner",
                    "c": [1, 1, 17.9899]
                }
            ]
        },
        {
            "g": "fly rod",
            "c": [1, 2, 45.9799],
            "_": [
                {
                    "g": "gilded spoon",
                    "c": [1, 1, 27.99]
                },
                {
                    "g": "double spinner",
                    "c": [1, 1, 17.9899]
                }
            ]
        },
        {
            "g": "grommet",
            "c": [1, 2, 717.98],
            "_": [
                {
                    "g": "espresso mmachine",
                    "c": [1, 1, 649.99]
                },
                {
                    "g": "panini press",
                    "c": [1, 1, 67.99]
                }
            ]
        }
    ]
}
```

**How does the sequence query work?**

Event queries use row iteration. Internally each person has a row set. Those row sets are sorted by time. A query is run against each person, iterating through the events in the row set looking for matches.

In this case we want to find the products purchased immediately after another purchase. We do so by iterating events and matching the `purchase` event. When we match we perform a nested match for another `purchase` event and `tally` the `product_name`s from second match under the `product_name`s from the first match. The aggregators count `people` and `product_name` and sum `product_price`.

# RoadMap

OpenSet is in Alpha. There may be semi-breaking changes going forward.

-   External data. This feature will allow complex data structures to be passed to OpenSet for use in queries. These will data structures will be global to a table and can be used from any `pyql` script as standard `python` data types.
-   Properties. A way to set non-event attributes on a `person` record. For example, life-time-value, gender or birth-date. Properties will appear as regular named `python` variables in `pyql` scripts.

# Inspiration

My name Seth Hamilton, I've written commercial software my entire life.

My first product was released in 1992 --- a graphical BBS product called RoboBOARD. After that came web analytics (DeepMetrix) and network monitoring (ipMonitor). My last startup (rare.io) did marketing automation. In all I've founded three startups, and I've been involved in two major acquisition --- by all measures I have had an exciting career.

However, in 2015 I found myself writing my first resume and came to a realization. Despite writing millions of lines of code, I actually couldn't prove I had written anything. 100% of the code I had written was owned by someone else at that point in time.

I love programming, I've been doing it since I was kid, and I especially love C++ (C++11 and beyond are game changing), and extra love messing with data (who doesn't). So, I thought this useful solution might be a good project to give back to the community.

**So, why does this even exist?**

Way back in 2005 I came across an interesting problem while at DeepMetrix. We produced an excellent little product called LiveStats. Everyday a million websites got their metrics using our software.

LiveStats created roughly 40 reports. The reports were predefined and continuously updated using data from weblogs and page-scripts.

This approach seemed perfect... until one day Anheuser-Busch called (they make a beer you've probably heard of). Bud wanted to drill into their data, and they wanted to see their data grouped and tabulated as saw fit, and they wanted this all in real-time. It was a compelling problem and they were willing to pay handsomely if we could solve it.

Unfortunately, we had to say no. We didn't have the technology or the capacity to handle their requirements at that time. Back then most servers were 32bits, 4 cores was a lot and 4GB was twice as much as you could actually address... not to mention enterprise class hard drives had less capacity than your typical smartphone today... and... our stack was SQL.

Failure got me thinking, and here we are today.

## OpenSet 0.2.12 release notes

-   support for properties by way of the `props` variable within your scripts. A property can be read or modified during any query, segment, or trigger. The script compiler will optimize if `props` are not used. Reading `props` will slow down execution of your scripts. Writing `props` will slow down execution a little bit more. Expect a 2x slowdown when getting and setting props. It should be noted only changed `props` are written back:

```python
    if 'test' not in props:
        props['test'] = dict()

    # set some props
    props['test']['this'] = 'hello'

```

-   support for reverse iterators:

```python
   for some_row in reverse rows if name == 'Klara':
       # do something with row
```

-   support for continuation from a specific row:

```python
   some_row_number = 4

      continue from some_row_number for some_row in rows:
          # some_row will contain row values iterating forward from row 4
```

or in reverse:

```python
   some_row_number = 4

   continue from some_row_number for some_row in reverse rows:
       # some_row will contain row values iterating in reverse from row 4
```

-   Row searching with `FIRST VALUE`, `LAST VALUE`, `FIRST ROW` and `LAST ROW`. Searches will return `None` when no row or value is found.

```python
   # find the first row number where the fruit column is not "orange"
   matched_row = FIRST ROW where fruit != "orange"

   row_content = get_row(matched_row)

   if row_content['fruit'] == 'apple':
       # do something
```

or

```
   row_content = get_row(FIRST ROW where fruit == "pear")
```

or

```python
   # find the last row number wehre the fruit column is not "orange"
   matched_row = LAST ROW where fruit != "orange"
```

or

```python
   # find the last value of recorded for the fruit column that is not "orange"
   # Note: the value column is provided after search `LAST VALUE` like in other
   # aggregators.
   last_fruit = LAST VALUE fruit where fruit != 'orange'
```

or

```python
   # find the first value of recorded for the fruit column that is not "orange"
   first_fruit = FIRST VALUE fruit where fruit != 'orange'
```

-   `where` keyword has been added as an alternative to `if` in row iterators and searches:

```python
   # the following are identical
   row_content = get_row(FIRST ROW where fruit == "pear")
   row_content = get_row(FIRST ROW if fruit == "pear")
```

> Important - there is currently a limitation with `props` in regards to replication and failover. In build 0.2.12 properties are not replicated. This will be addressed. For now, you can run a query script to recreate the missing props.

# Licensing

#### License

Copyright (c) 2015-2019, Seth Hamilton and Crowd Conduct Corp.

The OpenSet project is licensed under the MIT License.

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
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
