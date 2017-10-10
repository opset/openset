![OpenSet](docs/img/openset_compact.svg)


OpenSet is a MIT licensed programmable engine for rapidly extracting behavior from user event data. 

If you have apps, websites, or IoT devices that generate user event data, you might find OpenSet really useful.

OpenSet is a streaming solution and can ingest data at up to 35,000 lines per second per node (fully indexed and replicated). OpenSet has been tested on datasets with millions of users and billions of rows.

| Platform    | Info                             | Status                                                                                                                                                                     |
| :-----------| :------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Linux x64   | gcc7.2, release, debug           | [![Build Status](https://travis-ci.org/opset/openset.svg?branch=master)](https://travis-ci.org/opset/openset)                                                              | 
| Windows x64 | Visual C++ 2017, release, debug  | [![Build status](https://ci.appveyor.com/api/projects/status/pr8jrhfth2bt7j6r/branch/master?svg=true)](https://ci.appveyor.com/project/SethHamilton/openset/branch/master) |

:coffee: **OpenSet is currently pre-release, pre-beta, pre-alpha etc.**

## Links
*  [Documentation](https://github.com/perple-io/openset/tree/master/docs)
*  [Docker Images](https://github.com/perple-io/openset/tree/master/docs/docker)
*  [Sample Code](https://github.com/perple-io/openset/tree/master/samples)
*  [Admin Tools](https://github.com/perple-io/openset/tree/master/tools)

## Strong Points

1. Re-evening
2. Sequence Extraction
3. Analytics & Segmentation
4. Person Extraction

### 1. Re-eventing

![Segments](docs/img/re-event-matrix.svg)

Translate live event streams into behavioral event streams. For example, translate raw app usage data (clicks and feature usage) into scenario based events. OpenSet can ingest high speed data, and emit behavioral events for single users from that stream. For example:

- people who used your app more than 5 times.
- people that have been actively using your app for 90 days.
- people that have not used feature X in 30 days.
- people that used feature X but not feature Y within their first 14 days.
- people who spent at least $1000 in your app store a year ago, but has spent less than $1000 in the last 365 days.
- people who were in Spain visited Spain but did not visit another European destination within 180 days.

The event emitted will contain a time, an event name and the user ID that triggered the event. Events are emitted within milliseconds of their due time, where they are queued for consumption. Queues listeners can be configured to receive complete subscriptions, or share a subscription to allow for round robin message distribution.

### 2. Sequence Extraction

![Segments](docs/img/chains-of-events.svg)

Extract the common chains of events that lead people to a target behavior.

- extract the countries of origin for people that visited a given destination.
- extract the sequence of countries visited by people that arrived at a given destination.
- extract the app features used by customers before upgrading to your premium plan.

Extract the next thing, or the next _n_ things people do after they perform a target behavior:

- what products are commonly purchased with product X
- what are products purchased within the next 30 days after product X
- what features do users utilize after feature X.

Extract sequences or chains of events between a source and target behavior:

- What countries do people visit after visiting country X, but before visiting country Y.
- What site features do customers use after coming from promo X but before signing up.

### 3. Analytics & Segmentation

OpenSet can generate multi-level pivots with custom aggregations and population counts at parent and child nodes branches. You can specify up to 16 pivot depths, with combinations of event properties, counts, duration values, or date values.

![Pivots](docs/img/pivots.svg)

OpenSet brings behavior to segmentation. With OpenSet you can cluster people into segments using event attributes as well as event sequences. Derivative segments can be generated from other segments using intersection, union, complement and difference. Segments can even be compared to extract differences between their respective populations. 

![Segments](docs/img/segment_circles.svg)


### 4. Person Extraction

Load or stream events (app, web, IoT, etc) into OpenSet and extract the history and attributes for single users in milliseconds. It doesn't matter if you have millions of events, or you are inserting thousand of events per second, OpenSet extract users by ID in real-time.

![Segments](docs/img/one-person.svg)

# Examples

OpenSet uses a Python-like macro language called PyQL to define it's queries. 

#### Simple query:

Lets get  a breakdown by day of week and product name when the product purchased is a kitchen products. For each product  aggregate people, total purchases, and total spent.

```python
aggregate: # define our output columns
    count person
    count product_name as purchased
    sum product_price as total_spent with product_name

# iterate events where product_group is 'outdoor'
match where product_group is 'outdoor':
    # make a branch /day_of_week/product_name and
    # aggregate it's levels
    tally(get_day_of_week(event_time()), product_name)
```
> :pushpin:  Check out the event data for this query [here](https://github.com/perple-io/openset/blob/master/samples/data/highstreet_events.json).

Which might return something like:
```javascript
{
   "_": [
      {
          "g": 2, // day_of_week: tuesday
          "a": [1, 3, 155.93], // aggregates
          "_": [ // sub-group for product_name
              {
                  "g": "triple hook jigger", // product_name
                  "a": [1, 1,27.99] // aggregates
              },
              {
                  "g": "fly rod", // product_name
                  "a": [1, 1,99.95] // aggregates
              },
              {
                  "g": "deluxe spinner", // product_name
                  "a": [1, 1,27.99] // aggregates
              }
          ]
      },
      {
          "g": 5, // day_of_week: thursday
          "a": [1, 1, 99.95],
          "_": [ // sub-group for product_name
              {
                  "g": "fly rod", // product_name
                  "a": [1, 1,99.95] // aggregates
              }
          ]
      },
      ...
    ]
}
```

#### Basic sequence query

Lets query a sequence. We want to find the products purchased immediately after another purchase. It does so by iterating events and matching `purchase` event, then performs a nested match for another `purchase` event and tallies the product name from second match under the product name from the first match. The aggregators count people, purchase count and total revenue. 

```python
aggregate: # define our output columns
    count person
    count product_name as purchased
    sum product_price as total_revenue with product_name

sort:
    person # sort using person aggregate

# search for a purchase event
match where action is 'purchase': # match one
    # store the name of the product matched
    first_matching_product = product_name

    # move to next row in user record. We don't want
    # match products from the initial purchase
    iter_next()

    # match 1 row, or only the products in the purchase event
    # immediately following the product match above
    match 1 where action is 'purchase' and
            product_name is not first_matching_product:
        tally(first_matching_product, product_name)

    # loop back to top
```
> :pushpin:  Check out the event data for this query [here](https://github.com/perple-io/openset/blob/master/samples/data/highstreet_events.json).

Which will return a tree of initial products and subsequent products purchased:
```javascript
{
    "_": [
        {
            "g": "shag rug", // initial purchase
            "c": [2, 4, 885.92], // aggregates sub-branches
            "_": [
                {
                    "g": "panini press", // next purchase
                    "c": [2, 2, 135.98] // aggregates
                },
                {
                    "g": "espresso mmachine", // next purchase
                    "c": [1, 1, 649.99] // aggregates
                },
                {
                    "g": "fly rod", // next purchase
                    "c": [1, 1, 99.95] // aggregates
                }
            ]
        },
        {
            "g": "fly rod", // initial purchase
            "c": [1, 2, 45.9799], // aggregates sub-branches
            "_": [
                {
                    "g": "gilded spoon", // next purchase
                    "c": [1, 1,27.99] // aggregates
                },
                {
                    "g": "double spinner", // next purchase
                    "c": [1, 1, 17.9899] // aggregates
                }
            ]
        },
        ... // more data
    ]
}
```

> :pushpin: more sample code can be found here [here](https://github.com/perple-io/openset/tree/master/samples).

# RoadMap

OpenSet is pre-alpha. There are many items on the wish list, then there are things that need to be done so we can move to an alpha release:

- user properties (allow for storage and querying of non-event user data, i.e. name) 
- more error handling
- consistent behavior _during_ failover events
- internal rebalancing of nodes (cluster balancing works well)
- better handling of event emitters during failover/rebalance.
- commit to disk. This is partially written, as it relies on the code that serializes data for rebalancing and redistribution of nodes, but as of right now, you cannot commit the database (meaning, for now, if you stop it, you must re-load your data).
- HTTP/HTTPS API proxy. 
- refactoring (add more consts, more member initialization, tidy up some ugly stuff)

# Inspiration

Hello, my name Seth.

I've written commercial software my entire life. 

My first product was released in 1992  ---  a graphical BBS product called RoboBOARD. After that came web analytics (DeepMetrix) and network monitoring (ipMonitor). My last startup (rare.io) did marketing automation. In all I've founded three startups, and I've been involved in two major acquisition --- by all measures I have had an exciting career. 

However, in 2015 I found myself writing my first resume and came to a realization. Despite writing millions of LOC, I actually couldn't prove I had written anything. 100% of the code I had written was owned by someone else at that point in time. 

I love programming, I've been doing it since I was kid, and I especially love C++, and extra love messing with data. So, I thought this unique data solution written in C++ might be a good project to give back to the community.

#### Origin of the idea...

Way back in 2005 I came across an interesting problem while at DeepMetrix. We produced an excellent little product called LiveStats. Everyday over a million websites got their metrics using our software.

LiveStats created around 40 reports. The reports were predefined and continuously updated using data from weblogs and page-scripts.

This approach seemed perfect... until one day Anheuser-Busch called (they make a famous beer). They wanted to drill into their data, and they wanted to see their data was grouped and tabulated as saw fit, and they wanted this all in real-time. It was a compelling problem and they were willing to pay handsomely if we could solve it.

Unfortunately, we had to say no. We didn't have the technology or the capacity to handle their requirements at that time. Back then most servers were 32bits, 4 cores was a lot and 4GB was twice as much as you could actually address... not to mention enterprise class hard drives had less capacity than your typical smartphone. 

Failure got me thinking.

# Licensing

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




