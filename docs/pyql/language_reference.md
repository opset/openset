# PyQL Language Reference

## Query Layout

```
agg:
    {{things_to_count}}
    ...

{{pyqL code}}
```

The `agg` and `sort` sections are optional. Not specifying `agg` will default aggregation to people. Excluding sort will return unsorted data.

## Aggregators

```python
agg:
  count {{table column}} [as {{alias}}] [with {{other key}}] [all]
  sum {{table column}} [as {{alias}}] [with {{other key}}] [all]
  min {{table column}} [as {{alias}}] [with {{other key}}] [all]
  max {{table column}} [as {{alias}}] [with {{other key}}] [all]
  avg {{table column}} [as {{alias}}] [with {{other key}}] [all]
  val {{table column}} [as {{alias}}]
  var {{table column}} [as {{alias}}] [<< {{pyql code}}]]
```

## Built-in columns

OpenSet automatically provides columns for your convenience within each row in a dataset:

| Column      | Type  | Note                                                                                                                                                                                                                 |
| ----------- | :---: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **stamp**   | int64 | Time in milliseconds. Either provided with event on insert, or added upon insert                                                                                                                                     |
| **event**   | text  | Used to indicate what the event row represents                                                                                                                                                                       |
| **id**      | text  | Synthetic column added to event rows at time of query. For efficiency it is only provided if referenced in a script.                                                                                                 |
| **session** | int64 | Synthetic column generated based on inactivity period. Default is 30 minutes, but can be overridden using the `session_time` parameter on a query URL. For efficiency it is only provided if referenced in a script. |

## Types: variables, dicts, sets and lists

PyQL has built in support for text, integer, floating point, list, set and dictionary types.

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

#### pop

return a value, while removing it from it's original container. If the container is empty `pop()` will return `None`.

```python
# pop from dictionary
my_dict = {
    "hello": "goodbye",
	"many": [1,2,3,4]
}

# pop will return a random entry from the dictionary
random_pair = my_dict.pop()

# pop from set
my_set = set("apple", "orange", "teacup")
# pop will return a random value from the set
ramdom_item = my_set.pop()

# pop from a list
my_list = ["this", "is", "cool", 1234]

# pop the last item from the list
last_item = my_list.pop()
```

> **Note:** PyQL `pop()` does not take any parameters.

#### .append .update, .add, `+` and `+=`

You can add to any of the container types using `.append()`, `.update()` or `.add()`. Unlike Python, PyQL makes not distinction (so use the one you like). Additionally, you can use math operators add values to containers.

```python
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

# append (or .add or .update)
my_dict.append({"fresh": "prince"})

# merge two dictionaries with +
other_dict = {"good": "apples"} + {"hunt": "red october"}

# append a dictionary with +=
other_dict += {"angles": "sang"}
```

#### .remove, .del and `-=`

You can remove items from containers using `.remove()` or `del`. Additionally you can use the math operator `-=` to remove items.

```python
my_dict = {}

my_dict['cheeses'] = {
	"orange": ["chedder"],
	"hard": "parmesan",
	"soft": ["mozza", "cream"]
}

# remove item using del
del my_dict['cheeses']['orange'] # removes 'orange' from my_dict['cheeses']

# remove using .remove()
my_dict['cheeses'].remove('hard')

# remove with -=
my_dict -= 'cheeses'
```

#### `in` and `not in`

Like Python, `in` and `not in` are logical operators that test for membership in a dictionary or set. In PyQL you can test for membership in list types as well.

```python
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

if "hello" in my_dict:
    log('found')

if 'pickle' not in my_dict:
    log('not found')
```

#### .clear

Clear will empty a container.

```python
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

my_dict.clear() # dict will be empty
```

#### .keys

Returns the keys in a dictionary container as a List.

```python
my_dict = {
    "hello": "goodbye",
    "high": "low"
}

some_keys = my_dict.keys()
# some_keys will be ["hello", "high"]
```

## Row Iterators

#### for ... rows

`rows` is a reserved word. A `for` loop iterating `rows` becomes a specialized event row iterator.

A user record contains a sorted list of events, a `for/rows` iterator will move the row cursor, starting at row `0`. `for/rows` can be nested and continued.

If a `#` follows the `for` keyword, this signifies an iteration limiter. OpenSet will perform the specified number of matches.

When an `if` or `where` follows the `for` statement the iterator becomes a filter. Only rows matching the filter will be visible to the `for` iterators code block. When an iteration limiter (`#`) is provided only the specified number of matches will be passed to the code block (if available).

```python
for row in rows:
    do_something() # on each row
```

```python
for 5 row in rows:
    do_something() # on the first five matches
```

```python
for row in rows if row['some_column'] is 'some_value':
    do_something() # on each row that matches the filter
```

```python
for 1 row in rows if row['some_column'] == 'some_value':
    do_something() # on the first match that matches the filter
```

```python
for 1 row in rows if row['some_column'] == 'some_value':
    do_something() # on the first match that matches the filter

    continue for 1 sub_row in rows if sub_row['some_column'] == 'some_other_value':
        do_something_else() # on firt matching row after the last match
```

:pushpin: Nested iterators maintain their own positions (the row index is not global). The inner match (the second match in the last example) will start on the row following the the first match because `continue` has been specified. If `continue` is not specified, nested iterators will start at row `0` (or the last row for reverse iterators).

#### for ... reverse rows

Reverse iterators work identically to the standard forward iterator but start at the most recent event rows in the dataset.

```python
for row in reverse rows:
    do_something() # on each row
```

Just like forward iterators they can be continued for nested recursion.

```python
for 1 row in reverse rows if row['some_column'] == 'some_value':
    do_something() # on the first match that matches the filter

    continue for 1 sub_row in reverse rows if sub_row['some_column'] == 'some_other_value':
        do_something_else() # on firt matching row after the last match
```

#### continue from ... for ... rows

The `continue from` notation allows you to iterate from a specific index in the event row set.

If the row specified does not exist, the iterator will fail gracefully.

> **Note:** he `continue from` iterator will start exactly on the row provided, unlike the conventional use of `continue` (from the examples above) that starts on the row after the last matching row.

```python
some_row_number = 4

continue from some_row_number for some_row in rows:
    # do something - some_row will contain row values iterating forward from row 4
```

same as above but only provide one (first) match:

```python
some_row_number = 4

continue from some_row_number for 1 some_row in rows:
    # do something - some_row will contain row values iterating forward from row 4
```

#### row_count

`row_count()` returns the number of events (rows) in the event set.

```python
# count people by row_count rounded down to the nearest 5
tally(bucket( row_count(), 5))
```

#### row_within

Is the timestamp for the current row iterator between two dates.

Match rows within 5 days of the users first event. Notice the `5 days` notation, in PyQL you can use `# unit` where unit is `seconds`, `minutes`, `hours` or `days`

```python
for row in rows if row_within(5 days, first_event):
    do_something() # on rows within 5 days of first_event
```

Match rows within 12 hours of the users last event.

```python
for row in rows if row_within(12 hours, last_event):
   do_something() # on rows within 12 hours of last_event
```

Nested match, where the inner match event is within 12 hours of the outer match (`first_match`, `prev_match`, `last_event` and `first_event` are automatically created and can be used in your filters, or any other function).

```python
for row in rows if row['event'] is 'purchase':
    continue for 1 sub_row in rows if row_within(12 hours, last_match):

        do_something() # on rows within 12 hours of last_event in dataset
```

See the [time](#) section to learn more about `iter_row`.

#### row_between

Match between two dates. The date must be an ISO 8601 strings, Unix epoch seconds, JavasScript milliseconds or a value from the `stamp` built in column).

```python
for row in rows if
    row _between('2017-09-21T22:00:00Z', '2017-09-21T22:59:59Z'):

    do_something() # on rows between these dates
```

See the [time](#) section to learn more about `iter_between`.

## Functions, Iterators and Flow Control

#### def

`def`defines a function in PyQL, just as it does in Python. There are some notable differences though, mainly that PyQL doesn't support named parameters or `**args`. In that sense PyQL functions are a bit old-school.

```python
def my_function(a_string, an_int):
    return a_string + ' ' + an_int

bad_tv = my_function('Beverly Hills', 90210)
```

> :pushpin: **all variables** in PyQL are **global** to the script, this includes parameter names. This part of what makes PyQL is _very fast_. Be sure to give all variables, including parameter names unique names. You may reuse a parameter name subsequent function if there is no concern that the value will be overwritten.

#### return

`return` can be called alone or with parameters. Unlike python, PyQL does not support returning multiple values. If you would like to return multiple values, the work-around is to use a dictionary or list, and extract the values you need from the calling side.

```python
def some_function():
   return
```

```python
def ten_x(some_number):
    return some_number * 10
```

```python
def ten_x(some_value, another_value):
    return { 'value1': some_value, 'value2': anotehr_value }
```

#### if, elif and else

Conditionals in PyQL work as they do in Pyhon.

```python
if x > 0:
    do_something_postitive()
elif x < 0:
    do_something_negative()
else:
    do_something_neutral()
```

#### for / in (without `rows` keyword)

PyQL supports `for`/`in` loops.

When iterating lists and sets the value parameter will be populated with the values in the container.

```python
for some_value in some_list:
    do_something(some_value)
```

When iterating dictionaries the value parameter will be populated with the keys in the dictionary.

```python
for some_key in some_dict:
    do_something(some_key)
```

When iterating dictionaries you can ask for both the key and the value by providing two value variables.

```python
for some_key, some_value in some_dict:
    do_something(some_key, some_value)
```

#### continue

`continue` returns execution back to the top of a loop, and proceeds to the next iteration. `continue` can be used in `match` and `for/in` loops.

```python
for some_value in some_set:
    if some_value < 5: # if value is < 5 go back to top of loop
        continue
    do_something(some_value)
```

#### break

`break` will exit a loop. `break` can be used in `match` and `for/in` loops

```python
for some_value in some_set:
    if some_value < 5: # if value is < 5 stop looping
        break
    do_something(some_value)
```

Unlike Python, the `break` statement in PyQL has a few added abilities.

-   `break all` will break all nested loops including the outermost (top) loop. Code will resume running on the line of code following the top loop.
-   `break top` will beak all nested loops within the outermost (top) loop. The top loop will continue iterating.
-   `break #` will break a certain number of nested loops (`#`). A `break` without any additional decoration is same as specifying `break 1`.

#### exit

Exit the PyQL script. The next persons data will be queued and the script will be restarted.

## Time

#### now

`now` is a reserved variable which always contains the current time in milliseconds.

```python
for 1 row in rows if action == 'purchase': # just match first found
    first_match_time = event_time # save the time of this event

# how long ago in milliseconds was the first purchase
how_long_ago = now - first_match_time
```

#### event time

The event time can be accessed using the built in column variable `stamp`.

```python
first_match_time = None

for row in rows if event is 'purchase': # match first purchase

    if first_match_time == None:
        first_match_time = row['stamp']

    last_match_time = row['stamp'] # save the time of this event

# time between first and last purchase (in milliseconds) converted to days
days_between = to_days(last_match_time - first_match_time)
```

#### first_event

`first_event` will return the Unix epoch milliseconds for the first event in the users event set.

```python
user_active_span_in_weeks = to_days(last_event - first_event) / 7
```

#### last_event

`last_event` will return the Unix epoch milliseconds for the last event in the users event set.

```python
user_active_span_ms = last_event - first_event
```

#### fix

`fix` converts an integer, floating point or string (contain a number) into a fixed decimal place string. Banker rounding is applied.

The first parameter is the value to fix. The second parameter is the number of decimal places. If you simply want to truncate (and round) you can pass 0.

```python
total_money_spent = 37.05782

total_money_spent = fix(total_money_spent, 2)
# total_money_spenct now is "37.06"

total_money_spent = fix(total_money_spent, 0)
# total_money_spenct now is "37"
```

#### to_seconds, to_minutes, to_hours and to_days

the `to_seconds`, `to_minutes`, `to_hours` and `to_days` convert millisecond time-spans into their respective unit. Each function takes one parameter. Time is truncated to the nearest lower unit (the second, minute, hour or day the millisecond occurred in).

```python
for 1 row in rows if action == 'purchase': # just match first found
    first_match_time = event_time # save the time of this event

# how long ago in milliseconds was the first purchase
how_long_ago_in_days = to_days(now - first_match_time)
```

#### get_second, get_minute, get_hour, get_month, get_quarter, and get_year

When given a timestamp, the `get_second`, `get_minute`, `get_hour`, `get_month`, `get_quarter`, and `get_year` functions will the numeric value for the period they correspond to.

For example, `get_minute(event_time)` will return a value between `0-59`.

```python
# count all people that purchased by year and quarter
match where action is 'purchase': # match all purchase events
    tally(get_year(event_time), get_quarter(event_time))
```

#### get_day_of_week, get_day_of_month, and get_day_of_year

The `get_day_of_week`,`get_day_of_month`, and `get_day_of_year` functions are similar to the other `get_` date functions, but provide variations for **day** for week, month and year.

```python
# count all people that purchased by day of week
match where action is 'purchase': # match all purchase events
    tally(get_day_of_week(event_time))
```

#### date_second, date_minute, date_hour, date_day, date_week, date_month, date_quarter and date_year

The `date_*` functions round timestamps down to the nearest period as indicated by the function name.

For example, `get_month(event_time)` would return the month number for the `event_time`, whereas `date_month(event_time)` would return a new timestamp value which starts exactly at the beginning of the specified month.

```python
# count all people by date for month of purchase
match where action is 'purchase': # match all purchase events
    tally(date_month(event_time))
```

#### inline time spans

PyQL supports expressive formats for time spans, these are expanded into their equivalent value in milliseconds

```
ten_seconds = 10 seconds
ten_hours = 10 hours
ten_minutes = 10 minutes
ten_days = 10 days
```

## `tally` and `schedule`

#### schedule

The `schedule` function is used in OpenSet triggers. `schedule` takes two parameters, the first being the future time to schedule at, and the second being the name of the function in the trigger script to execute at that time.

```python
# count product_skus in purchases, when they reach 100
# emit an event and schedule a folloup tirgger to run in 90 days
def on_insert():
    counter = 0
    match where action is 'purchase' and product_sku is not None:
        counter += 1
        if counter >= 100:
            schedule(90 days, "check_for_200")
            return "reached one hundred purchases")
```

> **Note:** the `return` statement at the end of the script is the message that will be emitted to any listening webhook.

#### tally

The `tally` function has two modes, one for queries, and the other for segmentation.

When used in a query, the `tally` takes one or more grouping parameters. Results returned by OpenSet are in a tree format. Aggregators specified in the the `agg:` section will be executed using the current event iterator for each level of the tree.

The following PyQL will generate a tree:

```python
tally("root", "branch1", "leaf1")
tally("root", "branch1", "leaf2")
tally("root", "branch2", "leaf1")
tally("root", "branch2", "leaf2")
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

Tally creates pivot tables. For example if your events contained the attributes `country` and `product` you could generate a tree of `products` by `country` aggregating `id`, and any other output columns for each `tally` called:

```python
agg:
    people

for row in rows if product != None and country != None:
    tally(row['country'], row['product'])
```

or if you wanted to see `product` by `country`:

```python
agg:
    people

for row in rows if product != None and country != None:
    tally(row['product'], row['country'])
```

> :pushpin: Tally used for segmentation will be discussed in a segmentation document.

## Session Functions

#### session

Sessions are calculated at run-time and are enumerated from 1. Session time is defaulted at 30 minutes, however, when a query is requested a session timeout can be specified (using the `session_time` URL parameter) and sessions boundaries will be calculated using that value.

```python
agg:
    people

for row in rows if row['session'] == 5:
    do_something() # do something if row is on the 5th session
```

:pushpin: sessions are based on periods of continued activity, an inactivity period longer than the session timeout will increment the session count for the subsequent activity period.

#### session_count

returns the number of sessions in a persons row set.

```
if session_count > 5:
    # do something
```

## Inline Aggregators

Inline aggregators allow you to perform filtered counts on the specified column.

The keywords for Inline aggregators must be capitalized.

The values generated are specifically for the user being evaluated at query time, unlike the `agg:` section at the top of a query which performs similar aggregations for entire populations when the `tally` function is called.

The `if` or `where` statement is optional, but if provided will the inline aggregator will act as a filter or search.

#### COUNT

returns a `COUNT` for a column matching an optional filter.

```
product_pages_viewed = COUNT page if page_group = 'product'
```

##### SUM

returns a `SUM` for a column matching an optional filter.

```
total_spent = SUM cart_total if page_group = 'product'
```

##### MIN

returns the `MIN` value for a column with optional filter.

```
smallest_cart = MIN cart_total if page_group = 'product'
```

##### MAX

returns the `MAX` value for a column with optional filter.

```
largest_cart = MAX cart_total if page_group = 'product'
```

##### AVG

returns the `AVG` value for a column with optional filter.

```
avg_cart_size = AVG cart_total if page_group = 'product'
```

#### COUNT DISTINCT

returns the `COUNT DISTINCT` value for a column with optional filter.

```
unique_products = COUNT DISTINCT product_name if product_group = 'outdoor'
```

## Segment Math

##### population

##### intersection

##### union

##### compliment

##### difference

## PyQL built-in functions, variables

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

#### variable: session_count

contains number of sessions in users record

#### function: len

returns the length or number of elements in a string, dictionary, list, or set.

#### function: log

logs parameters to console

```python
log(some_column, some_var, some_dict, 'hello')
```

#### function: debug

Pushes a value into a debug log maintained by the PyQL interpreter. This function is primarily used when writing
tests for the OpenSet engine.

```python
debug(row_count == 0)
```

#### variable: last_event

contains time stamp of last event (most recent) in the dataset associated with a user id.

#### variable: first_event

contains time stamp of first event (oldest) in the dataset associated with a user id.

#### variable: row_count

contains the number of rows in the dataset associated with a user id.

#### variable: now

contains the current time as an OpenSet timestamp (milliseconds since 1970).

#### function: get_row

return the contents of a row iterator or row number as a dictionary of values.

```python
data_in_row = get_row(4)
```

#### function: url_decode

If you are storing raw URLs in your dataset, this function will return a dictionary containing all the parts of the URL.

> **Note:** this function is expensive. If you are constantly using this function it might be worth storing the URL elements you need as a column in the dataset.

```python
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

> **Note:** in the above example `param3` did not have a value, so it is assigned a value of `True`.

## string functions

#### function: string.find

returns first index of substring or -1

takes an optional second parameter for start index.

```python
some_string = 'the rain in spain'
index = some_string.find('rain')
# index = 4
index = some_string.find('rain', 8)
# index = -1
```

#### function: string.rfind

returns last index of substring or -1

takes an optional second parameter for start index.

```python
some_string = 'the rain in spain'
index = some_string.rfind('in')
# index = 15 (matches end of spain)
```

#### function: string.split

returns a List of string components sliced out of the provided string by the provided delimiter.

if the delimiter is not found in the string, a list containing just the source string will be returned.

```python
some_string = 'see spot run'
parts = some_string.split(' ')
# parts[0] == 'see' and parts[1] == 'spot' and parts[2] == 'run'
```

#### function: string.clean

removes leading and trailing white space (spaces, tabs, newlines, carriage returns) and returns a clean string.

```python
some_string = '\t  this is a string \r\n'
clean = some_string.strip()
# clean == 'this is a string'
```

## slicing string and lists

PYQL allows you to slice strings and lists using python slicing notation.

```python
result = thing_to_slice[ start_index : end_index ]
```

The notation uses the `[` and `]` bracket following a list or string variable. Within the brackets an optional start and
end index are provided separated by a `:` character.

index values can be negative if you would prefer to slice from the end of an array.

List examples:

```python
some_array = ['zero', 'one', 'two', 'three', 'four', 'five']

new_array = some_array[1:3]
# new_array == ['one', 'two']

new_array = some_array[:2]
# new_array == ['zero', 'one']

new_array = some_array[2:]
# new_array == ['two', 'three', 'four', 'five']

new_array = some_array[:]
# new_array = ['zero', 'one', 'two', 'three', 'four', 'five']  --- full copy

new_array = some_array[-1:]
# new_array = ['five']

new_array = some_array[-3:-2]
# new_array = ['three']
```

String examples:

```python
some_string = 'the rain in spain'

new_string = some_string[-5:]
# new_string == 'spain'

new_string = some_string[:3]
# new_string == 'the'

new_string = some_string[4:8]
# new_string == 'rain'
```

## PyQL built-in segmentation functions

#### function: union

#### function: difference

#### function: compliment

#### function: population

#### function: intersection
