# OSL Language Reference

## Query Layout

```

select
  # things_to_count
end

sort
  # how you want things sorted
end

# OSL script

```

The `select` and `sort` sections are optional. By default OpenSet will aggregate customer id's and sort customer totals in descending order.

## Aggregators

```python
agg:
  count {{property}} [as {{alias}}] [with {{other key}}] [all]
  sum {{property}} [as {{alias}}] [with {{other key}}] [all]
  min {{property}} [as {{alias}}] [with {{other key}}] [all]
  max {{property}} [as {{alias}}] [with {{other key}}] [all]
  avg {{property}} [as {{alias}}] [with {{other key}}] [all]
```

## Built-in properties

OpenSet automatically provides properties for your convenience within each row in a dataset:

| Property      | Type  | Note                                                                                                                                                                                                                                          |
| ----------- | :---: | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **stamp**   | int64 | Time in milliseconds. Either provided with event on insert, or added upon insert                                                                                                                                                              |
| **event**   | text  | A name used to indicate what the event row represents                                                                                                                                                                                         |
| **id**      | text  | Customer or User ID. This is a synthetic property added to an event row at time of query. For efficiency it is only provided if referenced in a script.                                                                                         |
| **session** | int64 | This is a synthetic property generated based on inactivity period. Default is 30 minutes, but can be overridden using the `session_time` parameter on a query URL. For efficiency it is only calculated and provided if referenced in a script. |

## Types: variables, dicts, sets and lists

OSL has built in support for text, integer, floating point, list, set and dictionary types.

Creating a variable is as simple as using and assigning it a value:

```ruby
some_int = 1234
some_float = 1234.5678
some_text = "hello"
```

OSL will maintain and convert types as needed internally.

#### Properties (accessing row level data)

Properties are referenced by name in OSL scripts. 

Depending on the context a property variable may have different meanings.

For example:

```ruby

if some_property == "dog"
  # some_property will be the value of whatever row the row cursor is on
  # useful for ifs in the code blocks of `each_row` iterators.
end

if some_property.ever(== "dog")
  # does some_property ever have a value of "dog"?
end

if some_property.within(3_months, now).never(== "dog")
  # does some_property NEVER have a value of "dog" within the last 3 months?
end

```

Modifiers make property logic more powerful.

Modifiers allow you to scan an entire property, match the cursor row, or test for ever/never scenarios within timeframes. 

:bulb: if using date constraints, you must use the `.is`, `.ever` or `.never` pattern.

-   `.ever( comparator )` - does comparator ever match
-   `.never( comparator )`- does comparator never match
-   `.is( comparator )`  -  does comparator match @ row in `each_row` iteration.
-   `.is_not` - does comparator not-match @ row in `each_row` iteration.
-   `.range( start_stamp, end_stamp )` - between to dates
-   `.within( time_span, relative_stamp )` - within a time frame of
-   `.look_ahead( #, # )` - same as within, but only looks forward
-   `.look_back( #, # )` - same as within, but only looks back
-   `.next()` - used by `.ever`, `.never` when used with `.look_ahead` or `.look_back` to move the cursor past the current row.

:bulb: the `.is` and `.is_not` modifier can only be used the `where` component of a `each_row` query, or within the code block or a `each_row` iterator. This is because the row cursor must be set.

:bulb: the `.is` and `.is_not` modifier may not be used with date  modifiers. See `each_row` modifiers to constrain a row search.

#### customer properties (properties marked with `is_customer`)

Properties defined with `is_customer` can be used like normal variables within an OSL script.

Generally Customer Properties would be used for non-event based customer facts.

Reading and writing a Customer Property is as easy of using the property by name. If you had a Customer Property named `total_purchase_value` and an `on_insert` script that runs when a customers data changes:

```ruby

total_purchase_value = sum(product_price).within(1_year, now) where event.is(== "purchase")

```

The OSL interpreter will detect that a Customer Property has been modified, write it back to the customer record and update indexes. Reading and writing Customer Properties is not as efficient as a regular row property, so if you don't need a Customer Pproperty in an OSL script, don't reference one.

#### dict

Dictionaries are variables holding keys and values.

```python
# create an empty dictionary
my_dict = {}
# or
my_dict = dict()

# create a dict with values
my_dict = {
    "hello": "goodbye",
    "many": [1,2,3,4]
}
```

> **Note:** unlike python the `dict` function cannot be used for type conversions.

> **Note2:** dictionary keys must be quoted.

#### list

Lists are also known as arrays and vectors.

```python
# create an empty list
my_list = []
# or
my_ist = list()

# create a list with values
my_list = ["this", "is", "cool", 1234]
```

> **Note:** unlike python the `list` function cannot be used for type conversions.

#### set

Sets are like dictionaries but without the value. They are often used to distinct count items or keep track of things you've done.

```python
# create an empty set
my_set = set()

# create a set with values
my_set = set("apple", "orange", "teacup")
```

> **Note:** unlike python the `set` function cannot be used for type conversions.

#### int

Converts non-integer types into integer types. When `int` fails to convert it will return a zero value.

```python
some_int = int("1234")
```

Floating point types converted to integers will be truncated. There are no conversion for container types to int.

#### float

Converts non-floating point types into integer types. When `float` fails to convert it will return a zero value.

```python
some_float = float("1234.567")
```

There are no conversion for container types to float.

#### str

Convert non-string types into strings.

```python
some_string = str(1234)
```

There are no conversions for container types (list, set, object) to string.

#### len

return the number of elements in a list, dictionary or set, or return the length of a string. Upon failure `len` returns a zero value.

```python
the_length = len("see spot run")
```

#### appending a container using  `+`

You can add to any of the container types using `+` operator.

```ruby

my_dict = {
    "hello": "goodbye",
    "high": "low"
}

# append (or .add or .update)
my_dict = my_dict + {"fresh": "prince"}

# merge two dictionaries with +
other_dict = {"good": "apples"} + {"hunt": "red october"}

```

#### removing a key or value from a container using `-`

You can remove items from containers using '-' operator.

```ruby
my_dict = {}

my_dict['cheeses'] = {
    "orange": ["chedder"],
    "hard": "parmesan",
    "soft": ["mozza", "cream"]
}

# removes 'orange' from my_dict['cheeses']
my_dict['cheeses'] = my_dict['cheeses'] - ['orange'] 

# remove `hard` and `soft` from cheeses
my_dict['cheeses'] = my_dict['cheeses'] - ['hard', 'soft']

```

#### `in` (and not `in`)

```ruby
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

if "hello" in my_dict
    log('found')

if ('pickle' in my_dict) == false:
    log('not found')
```

#### keys

Returns the keys in a dictionary container as a List.

```ruby
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

some_keys = keys(my_dict)
# some_keys will be ["hello", "high"]
```

## each_row Iterator

The `each_row` statement is a special iterator.

An `each_row` iterator always needs a `where` condition with at least one comparison.

An `each_row` iterator defines the beginning of a code block. Each code block requires a matching `end`.

`each_row` accepts modifiers. These modifiers can be chained to the `each_row` statement to filter or modify which rows will be evaluated:

-   `.reverse()` - iterate set in reverse
-   `.forward()` - iterate forward (default unless outer iterator is in reverse)
-   `.continue()` - continue on current row for nested iterations
-   `.from( # )` - start a specific row
-   `.within( range, relative_stamp )` - within a time frame of
-   `.look_ahead( #, # )` - same as within, but only looks forward
-   `.look_back( #, # )` - same as within, but only looks back
-   `.range( start_stamp, end_stamp )` - between to dates
-   `.next()` - used `.continue` with `.look_ahead`, and `.look_back` to exclude the current row from the search.
-   `.limit(#)` # limit number of matches

```ruby
each_row where event.is(== "purchase")
  # do something on each matching row
end
```

```ruby
each_row.limit(5) where event.is(== "purchase")
  # do something on the first 5 matches
end
```

```ruby
each_row.reverse().limit(5) where event.is(== "purchase")
  # do something on the last 5 matches
end
```

```ruby
each_row.limit(1) where event.is(== "purchase")
  # match one more thing after this first match
  each_row.continue().next().limit(1) where event.is(== "return")
    # do something with first return found after first purchase
  end
end
```

:bulb: Nested iterators maintain their own positions (the row index is not global). The inner match (the second match in the last example) will start on the same row as outer match because we added the `.continue()` modifier, and advance one row before evaluating because we specified the `.next()` modifier.  If the `.continue()` is not specified, nested iterators will start at row `0` (or the last row for reverse iterators).

#### cursor

built in variable `cursor` returns the current row.

 ```ruby
 current_row = cursor
 ```

#### row_count

built-in variable `row_count` returns the number of events (rows) in the event set.

```python
# count people by row_count bucketed to the nearest 5
<< bucket( row_count, 5 )
```

## Iterators and Flow

#### if

`if` requires conditions. 

`if` signifies the beginning of a code block. Each code block must be completed with an `end`.

> :bulb: OSL does not currently support `else` or `else if`. Support will be added soon.

```ruby
if x > 0
    # do something
end
```

#### for / in (without `rows` keyword)

OSL supports `for`/`in` iterators.

When iterating lists and sets the value parameter will be populated with the values in the container.

```ruby
for some_value in some_list
    # do something with some_value
end
```

When iterating dictionaries the value parameter will be populated with the keys in the dictionary.

```ruby
for some_key in some_dict
    # do something with some_key
end
```


#### return

`return` can be called alone or with parameters.

Unlike many languages OSL requires that `return` uses braces `()` if returning a a value.

```ruby
 each_row where event.is(== "purchase")
  return true
end
```

#### continue

`continue` returns execution back to the top of a loop, and proceeds to the next iteration. `continue` can be used in `match` and `for/in` loops.

```ruby
for some_key in some_dict

    if some_dict[some_key] < 5 # if value is < 5 go back to top of loop
        continue
    end
    # do something
end
```

#### break

`break` will exit a loop. `break` can be used in `match` and `for/in` loops

```python
for some_key in some_dict

    if some_dict[some_key] < 5 # if value is < 5 stop looping
        break
    end
    # do something
end
```

OSL supports breaking at multiple depths using `break(#)`. Specifying a number will cause break to exit the specified number of nested loops.

#### exit

Stop evaluating the current customer and move to the next customer.

## Time

#### now

`now` is a reserved variable which always contains the current time in milliseconds.

```ruby
each_row.limit(1) where event.is(== "purchase") # just match first found
  first_match_time = event_time # save the time of this event
end

# how long ago in milliseconds was the first purchase
how_long_ago = now - first_match_time
```

#### event time

The event time can be accessed using the built in property variable `stamp`. The `stamp` will always be the stamp for the current `cursor` in the row set. 

```ruby
first_match_time = nil

each_row where event.is(== "purchase") # match first purchase

    if first_match_time == nil:
        first_match_time = stamp
    end

    last_match_time = stamp # save the time of this event
end

# time between first and last purchase (in milliseconds) converted to days
days_between = to_days(last_match_time - first_match_time)
```

#### first_stamp

`first_stamp` will return the Unix epoch milliseconds for the first event in the customer event set.

```ruby
user_active_span_in_weeks = to_weeks(last_stamp - first_stamp)
```

#### last_stamp

`last_stamp` will return the Unix epoch milliseconds for the last event in the customer event set.

```ruby
user_active_span_ms = last_stamp - first_stamp
```

#### fix

`fix` converts an integer, floating point or string (contain a number) into a fixed decimal place string. Banker rounding is applied.

The first parameter is the value to fix. The second parameter is the number of decimal places. If you simply want to truncate (and round) you can pass 0.

```ruby
total_money_spent = 37.05782

total_money_spent = fix(total_money_spent, 2)
# total_money_spenct now is "37.06"

total_money_spent = fix(total_money_spent, 0)
# total_money_spenct now is "37"
```

#### to_seconds, to_minutes, to_hours, to_days and to_weeks

the `to_seconds`, `to_minutes`, `to_hours` and `to_days` convert millisecond time-spans into their respective unit. Each function takes one parameter. Time is truncated to the nearest lower unit (the second, minute, hour or day the millisecond occurred in).

```ruby
each_row.limit(1) where event.is(== "purchase") # match first purchase
    first_match_time = event_time # save the time of this event
end

# how long ago in milliseconds was the first purchase
how_long_ago_in_days = to_days(now - first_match_time)
```

#### get_second, get_minute, get_hour, get_month, get_quarter, and get_year

When given a timestamp, the `get_second`, `get_minute`, `get_hour`, `get_month`, `get_quarter`, and `get_year` functions will the numeric value for the period they correspond to.

For example, `get_minute(event_time)` will return a value between `0-59`.

```python
# count all people that purchased by year and quarter
match where action is 'purchase': # match all purchase events
    << get_year(stamp), get_quarter(stamp)
```

#### get_day_of_week, get_day_of_month, and get_day_of_year

The `get_day_of_week`,`get_day_of_month`, and `get_day_of_year` functions are similar to the other `get_` date functions, but provide variations for **day** for week, month and year.

```ruby
# count all people that purchased by day of week
each_row where event.is(== "purchase") # match all purchase events
    << get_day_of_week(event_time)
end
```

#### start_of_second, start_of_minute, start_of_hour, start_of_day, start_of_week, start_of_month, start_of_quarter and start_of_year

The `start_of_*` functions round timestamps down to the nearest period as indicated by the function name.

For example, `get_month(stamp)` would return the month number for the event `stamp`, whereas `start_of_month(event_time)` would return a new timestamp value which starts exactly at the beginning of the specified month.

```ruby
# count all people by date for month of purchase
each_row where event.is(== "purchase") # match all purchase events
    << start_of_month(event_time)
end    
```

#### inline time spans

OSL supports expressive formats for time spans, these are expanded into their equivalent value in milliseconds at compile time.

```ruby
one_second = 1_second
ten_seconds = 10_seconds
one_hour = 1_hour
ten_hours = 10_hours
ten_minutes = 10_minutes
ten_days = 10_days
ten_weeks = 10_weeks
ten_months = 10_months # 31 day months
ten_years = 10_years # 365 day years
```

## `<<` push to aggregator

The `<<` function has defines groupings (pivots) for the result set.

Aggregators specified in the the `select` section will be executed using the properties found on current row `cursor`.

The following OSL script will generate a tree:

```ruby
<< "root", "branch1", "leaf1"
<< "root", "branch1", "leaf2"
<< "root", "branch2", "leaf1"
<< "root", "branch2", "leaf2"
```

that has this structure:

```
root
    /branch1
        /leaf1
        /leaf2
    /branch2
        /leaf1
        /leaf2
```

Tally creates pivot tables. For example if your events contained the attributes `country` and `product` you could generate a tree of `products` by `country` with total customers by  selecting (aggregating) `id`:

```ruby
select 
  count id
end

each_row where event.is(== "purchase") # match all purchase events
    << country, product
end
```

or if you wanted to see `product` by `country`:

```ruby
select
  count id

each_row where event.is(== "purchase") # match all purchase events
    << product, country # how do you want it grouped?
end
```

## Session Functions

#### session

Sessions are calculated at run-time and are enumerated from 1. Session time is defaulted at 30 minutes, however, when a query is requested a session timeout can be specified (using the `session_time` URL parameter) and sessions boundaries will be calculated using that value.

```ruby1
select 
  count id
end

each_row where event.is(== "purchase") && session.is(== 5)
    << product # count up products bought on 5 session
end
```

:pushpin: sessions are based on periods of continued activity, an inactivity period longer than the session timeout will increment the session count for the subsequent activity period.

#### session_count

returns the number of sessions in a persons row set.

```ruby
if session_count > 5:
    # do something
end
```


## OSL built-in functions and variables

These are functions and variables not covered in the above sections.

#### function: bucket

`bucket(value, size)`

Rounds `value` down to the nearest multiple of `size`. Supports integer and decimal values.

```
nearest_50_cents = bucket(23.26, 0.50) # returns 23.00
nearest_25_dollars = bucket(27.11, 25) # returns 25
```

#### function: round

rounds a decimal number up or down (bank rounding) to it's nearest integer.

```
rounded_value = round(0.5) # returns 1
rounded_value = round(0.05) # return 0
```

#### function: trunc

truncates (chops off) the decimal part of a number and returns an integer (rounds down)

```
truncated_value = round(5.5) # returns 5
```

#### function: fix

`fix(value, decimals)`

rounds a decimal number to a fixed set of decimal places. Returns a string.

```
dollars_cents = fix(24.9499, 2) # returns "24.95"
```

#### function: iso8601_to_stamp

convert an ISO string into an OpenSet timestamp (milliseconds since 1970)

> :bulb: requires full ISO date with time and zone information or `Z`. Will also accept ISO8601 dates with milliseconds.

#### variable: session_count

contains number of sessions in users record

#### function: len

returns the length or number of elements in a string, dictionary, list, or set.

#### function: log

logs parameters to console

```python
log(some_property, some_var, some_dict, 'hello')
```

#### function: debug

Pushes a value into a debug log maintained by the OSL interpreter. This function is primarily used when writing tests for the OpenSet engine.

```python
debug(row_count == 0)
```

#### variable: last_stamp

returns time stamp of last event (most recent) in the dataset associated with a user id.

#### variable: first_stamp

returns time stamp of first event (oldest) in the dataset associated with a user id.

#### variable: row_count

returns the number of rows in the dataset associated with a user id.

#### variable: cursor

returns the current index in the rowset

#### variable: now

conreturnstains the current time as an OpenSet timestamp (milliseconds since 1970).

#### function: get_row

return the contents of a row at the provided index. 

```ruby
data_in_row = get_row(cursor)
```

> :bulb: this function is expensive. If you only need a value from a row, it may be best to capture it during iteration.

#### function: url_decode

If you are storing raw URLs in your dataset, this function will return a dictionary containing all the parts of the URL.

> :bulb: this function is expensive. If you are constantly using this function it might be worth storing the URL elements you need as a property in the dataset.

```ruby
some_url = "http://somehost.com/this/is/the/path?param1=one&param2=two&param3"
parts = url_decode(some_url)
```

the returned dictionary will be structed as such:

```
{
    "host": "somehost.com",
    "path": "/this/is/the/path",
    "query": "param1=one&param2=two&param3",
    "params": {
        "param1": "one",
        "param2": "two",
        "param3": True
    }
}
```

> :bulb: in the above example `param3` did not have a value, so it is assigned a value of `True`.

## Aggregations and Searches (experimental)

Aggregations and Searches are one line iterators to perform filtered aggregations or tests. 

| aggregator | purpose                                                                                                  |
| :--------: | :--------------------------------------------------------------------------------------------------------|
| row        | find a row matching the `where` conditions or return that row number or `nil`                            |
| test       | return true/false if a row matches the `where` conditions.                                               |   
| sum        | return the sum for the expression passed to `sum` where rows passed the `where` conditions               | 
| count      | return the count for the expression passed to `count` where rows passed the `where` conditions           |  
| dcount     | return the distinct count for the expression passed to `dcount` where rows passed the `where` conditions |
| min        | return the minimum value for the expression passed to `min` where rows passed the `where` conditions     |
| max        | return the max value for the expression passed to `min` where rows passed the `where` conditions         |
| avg        | return the average value for the expression passed to `avg` where rows passed the `where` conditions     |

> :bulb: aggregations that take expressions are expecting the expression to result in a value. The most common expression would be a table property. If the expression has a `nil` value, it will not be counted.

```ruby
matching_row = row.reverse().within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.is(contains 'red')

is_red_basement = test.reverse().within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.is(contains 'red')

count_red_basement = count(product_name).within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.is(contains 'red')

max_price_red_basement = max(product_price).within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.is(contains 'red')

avg_spent_red_basement = avg(product_price).within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.is(contains 'red')

total_spend_red_basement = sum(product_price).within(1_year, now) where
    product_group.is(contains 'basement') && product_tags.row(contains 'red')

```
