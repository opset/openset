###

![OpenSet](docs/img/openset_blue.svg)

#### OpenSet is a MIT licensed programmable Customer Data Platform.

###

| Platform    |  Version | Info                            | Status                                                                                                                                                                     |
| :---------- | :------: | :------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Linux x64   |  0.4.3   | gcc 7.2, release, debug         | [![Build Status](https://travis-ci.org/opset/openset.svg?branch=master)](https://travis-ci.org/opset/openset)                                                              |
| Windows x64 |  0.4.3   | Visual C++ 2017, release, debug | [![Build status](https://ci.appveyor.com/api/projects/status/pr8jrhfth2bt7j6r/branch/master?svg=true)](https://ci.appveyor.com/project/SethHamilton/openset/branch/master) |

:coffee: OpenSet is currently in alpha. Please see v0.4.3 release notes below.

# What's it do?

-   Customer Analytics
-   Customer Segmentation
-   Eventing on Customer state changes and conditions
-   Sequence Analysis (cohorts, funnels, paths)
-   Customer History

# Introduction

OpenSet was designed to make Customer Data easier leverage for developers.

OpenSet can scale to support customer bases in the millions of customers.

OpenSet can ingest data from real-time streams.

OpenSet is programmable with a well featured scripting language - meaning you can ask questions that would be difficult to express with SQL or GraphQL.

OpenSet understand sequence, so asking before and after questions about Customers is greatly simplified (i.e. paths, funnels, next action, cohorts and attribution).

OpenSet can maintain up-to-date segments based on complex rules.

OpenSet can emit events when someone enters or exits a segment.

OpenSet uses an HTTP/REST interface making it accessible from your favorite language.

OpenSet can be clustered so you can scale for performance and redundancy.

# Links

-   [Documentation](https://github.com/opset/openset/tree/master/docs)
-   [Docker Images](https://github.com/opset/openset/tree/master/docs/docker)

# Examples using curl

We've put together a few examples to get you started. These examples require `git`, `docker` and `curl`.

You should be able to **cut and paste** the steps below on OSX, Linux or WSL.

**1. clone the samples in the `openset_samples` repo.**

```bash
cd ~
git clone https://github.com/opset/openset_samples.git
```

**2. Install [Docker](https://www.docker.com/) and start OpenSet (in interactive mode).**

```bash
docker run -p 8080:8080 -e OS_HOST=127.0.0.1 -e OS_PORT=8080 --rm=true -it opset/openset_x64_rel:0.4.3
```

> **Note** The OpenSet images can always be found on [dockerhub](https://cloud.docker.com/u/opset/repository/docker/opset/openset_x64_rel).

**3. Open another console (go to home directory).**

```bash
cd ~
```

**4. Initialize OpenSet.**

> :bulb: `json_pp` is part of most Linux distributions. If you don't want "pretty printed" JSON, or don't have `json_pp` then remove `| json_pp` from the following examples.

```python
curl -X PUT http://127.0.0.1:8080/v1/cluster/init?partitions=24 | json_pp
```

response:

```json
{
    "server_name": "smiling-donkey"
}
```

**5. Create a table named `highstreet`.**

```bash
curl \
-X POST  http://127.0.0.1:8080/v1/table/highstreet \
-d @- << EOF | json_pp
{
    "properties": [
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
    "event_order": [
      "purchase",
      "cart_item"
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

**6. Let's insert some json data from the `openset_samples/data` folder.**

```bash
curl \
-X POST http://127.0.0.1:8080/v1/insert/highstreet \
--data-binary @openset_samples/data/highstreet_events.json | json_pp
```

> :bulb: please take a look at `highstreet_events.json`, this is the format used to insert data into OpenSet. You will see that the object keys match the properties defined in step #5.

response:

```json
{
    "message": "yummy"
}
```

> :bulb: view the event data [here](https://github.com/opset/openset_samples/blob/master/data/highstreet_events.json)

**7. Let's perform an `event` query.**

This query searches through each customer looking for matching events in a customers history.

When a matching event is found the `<<` statement pushes the event through the aggregator where it is tabulated and grouped. `group by` functionality is provided by providing grouping names to the `<<` statement.

A cool feature of OpenSet grouping is that all branches of the result set will be correctly counted (including unique user counting).

```ruby
curl \
-X POST http://127.0.0.1:8080/v1/query/highstreet/event \
--data-binary @- << EOF | json_pp

# define which properties we want to aggregate
select
    count id
    count product_name as purchased
    sum product_price as total_spent
end

# for each person iterate events where the product_group set
# contains 'outdoor' and product_name is 'fly_rod' or 'gilded_spoon'
each_row where
        product_group.is(contains 'outdoor') &&
        product_name.is(in ['fly rod', 'gilded spoon'])

    # push the current row into the aggregater so
    # the properties selected in the `select` block are
    # updated. Group aggregations by `day_of_week` and
    # `product_name`
    << get_day_of_week(stamp), product_name
end

# end of script
EOF
```

response (counts are people, count product, sum price):

```json
{
    "_": [
        {
            "g": 2,
            "c": [1, 1, 99.95],
            "_": [
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
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
            "c": [1, 2, 127.94],
            "_": [
                {
                    "g": "fly rod",
                    "c": [1, 1, 99.95]
                },
                {
                    "g": "gilded spoon",
                    "c": [1, 1, 27.99]
                }
            ]
        }
    ]
}
```

**8. Let's make 5 segments**

```ruby
curl \
-X POST "http://127.0.0.1:8080/v1/query/highstreet/segment?debug=false" \
--data-binary @- << EOF | json_pp
# our osl script
@segment products_home use_cached=false refresh=5_minutes on_insert=true

# match one of these

if product_group.ever(any ['basement', 'garage', 'kitchen', 'bedroom', 'bathroom'])
  return(true)
end

@segment products_yard use_cached=true refresh=5_minutes on_insert=true

if product_group.ever(contains 'basement') || product_group.ever(contains 'garage')
    return(true)
end

@segment products_outdoor use_cached=true refresh=300s on_insert=true

if product_group.ever(contains 'outdoor') || product_group.ever(contains 'angling')
    return(true)
end

@segment products_commercial use_cached=true refresh=5_minutes on_insert=true

if product_group.ever(contains 'restaurant')
    return(true)
end

@segment grommet_then_panini use_cached=true refresh=5_minutes on_insert=true

# iterate rows where the properties match the conditions
each_row where
    event.is(== 'cart_item') &&
    product_name.is(== 'grommet')

  # nested row iteration continuing on the next row after
  # the above match
  each_row.continue().next() where
      event.is(== 'cart_item') &&
      product_name.is(== 'panini press')
    return(true)
  end
end

# end of script
EOF
```

response (counts are people):

```json
{
    "_": [
        {
            "g": "products_outdoor",
            "c": [2]
        },
        {
            "g": "products_commercial",
            "c": [2]
        },
        {
            "g": "products_home",
            "c": [2]
        },
        {
            "g": "products_yard",
            "c": [1]
        },
        {
            "g": "grommet_then_panini",
            "c": [1]
        }
    ]
}
```

**9. Let's query a property**

This will return customer counts for all the values for a property.

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/property/product_name' | json_pp
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

**10. Let's query a property in segment compare mode**

Same query as above, but now we are comparing a all customers `*` vs customers in the segment `products_outdoor`:

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/property/product_name?segments=*,products_outdoor' | json_pp
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

**11.** Let's query a numeric property and `bucket` the results by `50` dollar increments

> :bulb: note that the distinct user counts are properly counted per bucket. This is useful for making a histogram of all the value in a property.

```bash
curl \
-X GET 'http://127.0.0.1:8080/v1/query/highstreet/property/product_price?bucket=50' | json_pp
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

**13. Let's make a another histogram query using time**

Let's generate a histogram breaking down the number of weeks since `last_stamp` for each person in the database.

> :bulb: `last_stamp` is a built in variable that holds the timestamp for the last event in a customers dataset. `now` is a variable that holds the current timestamp.

```bash
curl \
-X POST 'http://127.0.0.1:8080/v1/query/highstreet/histogram/weeks_since' \
--data-binary @- << EOF | json_pp
# our osl script

return( to_weeks(now - last_stamp) )

#end of osl script
EOF
```

response (counts are people, weeks will vary as the dataset ages):

```json
{
    "_": [
        {
            "g": "weeks_since",
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

**14. Let's execute a complex sequence query.**

This query looks at each product in a cart, and reports which products were purchased immediately after in a subsequent cart, along with a distinct customer count, number or purchases, and total revenue of subsequent purchases.

The sample dataset includes two types of events, `purchase` events, which contain metrics about the shopping cart, and `cart_item` events. The `cart_item` events always follow the `purchase` events (when they share the same timestamp) because we specified a `z-order` when we created the table.

This query searches for `purchase` events and records all the subsequent `product_name` values for each `cart_item` event associated with a matched `purchase`.

The query then searches for the next subsequent `purchase` event and records the `order_id`. Lastly it matches each `cart_item` event with that `order_id`. For each matching `cart_item` even, the query pushes the row to aggregator grouping by the original `product_name` and the subsequent `product_name`.

```ruby
curl \
-X POST http://127.0.0.1:8080/v1/query/highstreet/event \
--data-binary @- << EOF | json_pp
# our osl script

select # define the properties we want to count
    count id
    count product_name as purchased
    sum product_price as total_revenue
end

# STEP 1
# search for a purchase events
each_row where event.is(== 'purchase')

  # products will hold the `product_name`s that
  # are found in the `cart_item`s associated with
  # the above purchase
  products = set()
  matched_order_id = order_id

  # STEP 2
  # gather the product names for the above purchase
  each_row.continue().next() where
      event.is(== 'cart_item') &&
      order_id.is(== matched_order_id)

    # product_names from cart_item events to set
    products = products + product_name

    # STEP 3
    # find the just the NEXT purchase (continue for 1)
    each_row.continue().next().limit(1) where
        event.is(== 'purchase') &&
        order_id.is(!= matched_order_id) # match one

      subsequent_order_id = order_id

      # STEP 4
      # for each 'cart_item' event iterate the products
      # captured in Step 2
      each_row.continue().next() where
          event.is(== 'cart_item') &&
          order_id.is(== subsequent_order_id)

        for product in products
          # Tally counts for each product in the
          # subusequent purchase for each product in
          # the first match

          if product == product_name # remove repurchases
            continue
          end

          << product, product_name
        end

      end
    end
  end
end
# loop to top

#end of osl script
EOF
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

# Inspiration

Way back in 2005 I came across an interesting problem while at DeepMetrix. We produced an excellent little product called LiveStats. Everyday a million websites got their metrics using our software.

LiveStats created roughly 40 reports. The reports were predefined and continuously updated using data from weblogs and page-scripts.

The "40 reports" model seemed please most everyone. That was until one day Anheuser-Busch called wanting something that didn't seem possible. They wanted a solution that could drill into customer data any way they saw fit - and they had a lot of data.

The problem wasn't mining the data, the problem was doing so in a timely fashion. Computers weren't fast, and SQL would take hours or days to compute some of the results if was able to complete the task.

Ultimately DeepMetrix had to say no to Bud, but that failure planted a seed.

# OpenSet 0.4.3 release notes
- switched from using the bigRing hash. It performed well, but was memory hungry. Switched to [robin-hood-hasing](https://github.com/martinus/robin-hood-hashing) by Martin Ankerl. Martins robin-hood hash table is STL `unordered_map` compatible, super fast, and resource friendly.
- corrected a nasty bug where the property encoding cache wasn't cleared between subsequent encoding causing the memory growth and slowdowns.
- switched the index change cache to use standard containers rather than the heap allocated linked list used before. New system is faster and maintains order.
- updated query optimizer to understand customer properties.

# OpenSet 0.4.2 release notes
- code changes and API in order to rename concepts in OpenSet. 
    * `people` are now `customers` 
    * `person` is now `customer`
- code changes and API changes rename `columns` to `properties`. Properties now have two types:
    1. event properties
    2. customer properties
- fixed a parsing issue with filters (chained functions) parsing past the closing `)` bracket
- fixed error where conditions with a `nil/none` value would evaluate to `true`.

# OpenSet 0.4.1 release notes

- indexed customer properties that can be used by the query optimizer
- props read/written anytime in any OSL script like regular user variables. The OSL Interpreter will determine whether a property has changed.

# OpenSet 0.4.0 release notes

-   new OpenSet Langauge (OSL) introduced to replace PyQL language. It was difficult to ask easy questions with PyQL. OSL was designed to be expressive and make reading and writing queries more natural.
-   new query optimizer that takes advantage of OSL language constructs to create smarter pre-query indexes.

# Licensing

#### License

Copyright (c) 2015-2019, Seth Hamilton.

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
