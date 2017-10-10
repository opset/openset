# PyQL Language Reference

## Query Layout

```
agg: 
    {{things_to_count}}
    ...
sort:
    {{aggregate to sort by}}
    
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



## Variables, Dicts, Sets and Lists

PyQL has built in support for text, integer, floating point, list, set and dictionary types.

##### Dictionaries

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

At current the `dict` function cannot be used to perform container conversions.

##### Lists

Lists are also known as arrays and vectors. 

```python
# create an empty list
my_list = []
# or
my_ist = list()

# create a list with values
my_list = ["this", "is", "cool", 1234]
```
At current the `list` function cannot be used to perform container conversions.

##### Sets

Sets are like dictionaries but without the value. They are often used to distinct count items or keep track of things you've done.

```python
# create an empty set
my_set = set()

# create a set with values
my_set = set("apple", "orange", "teacup")
```
At current the `set` function cannot be used to perform container conversions.

##### int

Converts non-integer types into integer types. When `int` fails to convert it will return a zero value.
```python
some_int = int("1234")
```
Floating point types converted to integers will be truncated. There are no conversion for container types to int.

##### float

Converts non-floating point types into integer types. When `float` fails to convert it will return a zero value.
```python
some_float = int("1234.567")
```
There are no conversion for container types to float.

##### str

Convert non-string types into strings.

```python
some_string = str(1234)
```

There are no conversion for container types to string.

##### len

return the number of elements in a list, dictionary or set, or return the length of a string. Upon failure `len` returns a zero value.

```python
the_length = len("see spot run")
```

##### pop

return a value, while removing it from it's original container.  If the container is empty `pop()` will return `None`. 

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

PyQL `pop()` does not take any parameters.

##### .append .update, .add, `+` and `+=`

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

##### .remove, .del and `-=`

You can remove items from containers using `.remove()`  or `del`. Additionally you can use the math operator `-=` to remove items.

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

##### `in` and `not in`

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

##### .clear

Clear will empty a container.

```python
my_dict = { 
    "hello": "goodbye",
    "high": "low"
}

my_dict.clear() # dict will be empty
```

##### .keys

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

##### match

`match` is a row Iterator. When a user record is mounted, a PyQL script started is started with the iterator on the first row of their event list. `match` can be nested, and will maintain an independent iterator for each level of `match`

```python
match:
    do_something() # on each row
```

```python
match 5:
    do_something() # on the first five matches
```

```python
match where some_column is 'some_value':
    do_something() # on each row that matches the filter
```

```python
match 1 where some_column is 'some_value':
    do_something() # on the first match that matches the filter
```

Nested, both matches are maintaining their own iterator. The inner match (the second match) will start on the exact row the outer match (the first match)  matched on.
```python
match where action is 'purchase': 
   match 1 where iter_within(12 hours, last_match):
       do_something() # on rows within 12 hours of last_event
```

##### iter_get

`iter_get()` returns the exact location of the current iterator. If you are inside a match block, you will receive the iterator location for that block. If called from outside a `match` the iterator will be it's default (row 0) or last position.

```python
my_iter_pos = iter_get() # get the position
# do something
iter_set(my_iter_pos)
```

##### iter_set

`iter_set(position)` will set the position of the current iterator. If you are inside a match block, you will be setting the iterator position for that block.

```python
my_iter_pos = iter_get() # get the position
# do something
iter_set(my_iter_pos)
```

##### iter_move_first

`iter_move_first()` moves the current iterator to the first row in the users event set. This can be useful when you want to run multiple `match` blocks in the same query or if you are using `match` to test for the presence of a value prior to performing an analysis. 

```python
match 1 where product is 'rubber duck':
    iter_move_first() # reset the iterator
    match:
        do_something() # do_something() will be called on all rows
```

##### iter_move_last

`iter_move_last()`, exactly the same as `iter_move_first()` but moves to the very last row in the event set.

##### iter_next

`iter_next()` moves to the current iterator to the next available event. This is useful when nesting `match` blocks if you do not want the nested `match` to evaluate the same event as the the outer `match`. 

```python
match where action is 'purchase': 
  iter_next() # move past match purchase, look for more purchases
   match 1 where action is 'purchase':
       do_something() 
```

> :pushpin: `iter_next()` takes into consideration virtual rows in complex events, and will move past virtual rows in the same event to the first row in the next event.

##### iter_prev

> :pushpin: Reverse iteration is coming soon.

##### iter_reset_event

> :pushpin: Not implemented. iter_reset_event is coming shortly.

When dealing with a complex event containing virtual rows (i.e. an event that has multiple permutation) a `match` could be made on any of these virtual rows. If you need to perform an evaluation of the entire event on an inner `match` it is useful to temporarily reset the iterator to the top of the event.

```python
# This would return a tree of products that are commonly purchased together.
# This assumes a complex event is used with product line items.
match where action is 'purchase':
    
    saved_product = product
    
    saved_iter = iter_get() # save iterator (could be on virtual row)
    iter_reset_event() # move to the first row or virtual row
    
    # match 1 will iterate all virtual rows in the current event
    match 1 where action is 'purchase':  
       tally(saved_product, product)
       
    iter_set(save_iter) # resume the outer match where it left off
```

##### event_count

`event_count()` returns the number of events in the event set. 

```python
tally(bucket( event_count(), 5)) # count people by event_count in groups of 5
```
> :pushpin: `event_count()` takes into consideration virtual rows in complex event sets, and will count the actual number of events, not the virtual rows. This is an expensive function, as such, the value is cached to reduce the cost of subsequent calls.

##### iter_within
Is the timestamp for the current row iterator between two dates.

Match rows within 5 days of the users first event. Notice the `5 days` notation, in PyQL you can use `# unit` where unit is `seconds`, `minutes`, `hours` or `days`
```python
match where iter_within(5 days, first_event):
   do_something() # on rows within 5 days of first_event
```
Match rows within 12 hours of the users last event.
```python
match where iter_within(12 hours, last_event):
   do_something() # on rows within 12 hours of last_event
```
Nested match, where the inner match event is within 12 hours of the outer match (`first_match`, `prev_match`, `last_event` and `first_event` are automatically created and can be used in your filters, or any other function).
```python
match where action is 'purchase': 
   match 1 where iter_within(12 hours, last_match):
       do_something() # on rows within 12 hours of last_event
```

See the [time](#) section for useful data_values for `iter_within`. 

##### iter_between

Match between two dates (ISO 8601 strings, Unix epoch seconds/milliseconds).
```python
match where iter_between('2017-09-21T22:00:00Z', '2017-09-21T22:59:59Z'):
   do_something() # on rows between these dates
```

## Functions, Iterators and Flow Control

##### def

`def`defines a function in PyQL, just as it does in Python. There are some notable differences though, mainly that PyQL doesn't support named parameters or `**args`. In that sense PyQL functions are a bit old-school.

```python
def my_function(a_string, an_int):
    return a_string + ' ' + an_int
    
bad_tv = my_function('Beverly Hills', 90210)
```

> :pushpin: **all variables** in PyQL are **global** to the script, this includes parameter names. This part of what makes PyQL is _very fast_. Be sure to give all variables, including parameter names unique names. You may reuse a parameter name subsequent function if there is no concern that the value will be overwritten. 

##### return

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
    # do something
    return { 'value1': some_value, 'value2': anotehr_value }
```

##### if, elif and else

Conditionals in PyQL work as they do in Pyhon.

```python
if x > 0:
    do_something_postitive()
elif x < 0:
    do_something_negative()
else:
    do_something_neutral()
```

##### for / in

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

##### continue

`continue` returns execution back to the top of  a loop, and proceeds to the next iteration. `continue` can be used in `match` and `for/in` loops. 

```python
for some_value in some_set:
   if some_value < 5: # if value is < 5 go back to top of loop
       continue
   do_something(some_value)
```

##### break

`break` will exit a loop. `break` can be used in `match` and `for/in` loops

```python
for some_value in some_set:
   if some_value < 5: # if value is < 5 stop looping
       break
   do_something(some_value)
```

Unlike Python, the `break` statement in PyQL has a few added features. With PyQL being focused on iteration having being able to provide more direction to `break` is convenient. 

- `break all` will break all nested loops including the outermost (top) loop. Code will resume running on the line of code after the top loop.
- `break top` will beak all nested loops within outermost (top) loop. The top loop will continue iterating. 
- `break #` will break a certain number of nested loops (`#`). A regular `break` is same as specifying `break 1`. 

##### exit

Exit the PyQL script. The next user will be queued and the script will be restarted.

## Time 

##### now

Get the current time in Unix epoch milliseconds. 

```python
match 1 where action is 'purchase': # just match first found
   first_match_time = event_time # save the time of this event

# how long ago in milliseconds was the first purchase
how_long_ago = now - first_match_time          
```

##### event_time

Returns the the `event_time` in Unix epoch milliseconds at the current event iterator position.

```python
match 1 where action is 'purchase': # match first purchase

   first_match_time = event_time # save the time of this event
   iter_next()
   
   match where action is 'purchase': # match remaining purchases
       last_match_time = event_time
       
# time between first and last purcahse
time_between = last_match_time - first_match_time
```

##### first_event

`first_event` will return the Unix epoch milliseconds for the first event in the users event set.

```python
user_active_span = last_event - first_event
```

##### last_event

`last_event` will return the Unix epoch milliseconds for the last event in the users event set.

```python
user_active_span = last_event - first_event
```

##### prev_match

`prev_match` is useful for nested `match` iterators. The timestamp of the event that triggered the last match (the nearest outer match) will be returned.

```python
match where action is 'purchase': # match all purchase events

   # note - prev_match was set on the match above
   iter_next()
   
   match where action is 'purchase': # match remaining purchases
       if event_time - prev_match > 30 days:
           do_something() # 30 or more days between these purchases
```

> :pushpin: `prev_match` will be correct at all depths in a nested `match` loop. If you `break` a loop `prev_match` will contain the correct value for the new level of loop nesting.

##### first_match

See `prev_match`. The difference between `first_match` and `prev_match` is that on deeply nested `match` loops, `first_match` will always contain the match time for the very most outer loop.

##### fix

`fix` converts an integer, floating point or string (contain a number) into a fixed decimal place string. Bankers rounding is applied.

The first parameter is the value to fix. The second parameter is the number of decimal places. If you simply want to truncate (and round) you can pass 0.

```python
total_money_spent = 37.05782
total_money_spent = fix(total_money_spent, 2)
# total_money_spenct now is "37.06"
total_money_spent = fix(total_money_spent, 0)
# total_money_spenct now is "37"
```

##### to_seconds, to_minutes, to_hours and to_days

the `to_seconds`, `to_minutes`, `to_hours` and `to_days` convert millisecond time-spans into their respective unit. Each function takes one parameter. Time is truncated to the nearest lower unit (the second, minute, hour or day the millisecond occurred in). 

```python
match 1 where action is 'purchase': # just match first found
   first_match_time = event_time # save the time of this event

# how long ago in milliseconds was the first purchase
how_long_ago_in_days = to_days(now - first_match_time)
```

##### get_second, get_minute, get_hour, get_month, get_quarter, and get_year

When given a Unix epoch timestamp, the `get_second`, `get_minute`, `get_hour`, `get_month`, `get_quarter`, and `get_year` functions will the numeric value they correspond to. For example, `get_minute(event_time)` will return a value between `0-59`.

```python
# count all people that purchased by year and quarter
match where action is 'purchase': # match all purchase events
    tally(get_year(event_time), get_quarter(event_time))
```

##### get_day_of_week, get_day_of_month, and get_day_of_year

The `get_day_of_week`,` get_day_of_month`, and `get_day_of_year` functions are similar to the other `get_` date functions, but process variations of `day`. When given a Unix epoch timestamp they will return the value they correspond to.

```python
# count all people that purchased by day of week
match where action is 'purchase': # match all purchase events
    tally(get_day_of_week(event_time))
```

##### date_second, date_minute, date_hour, date_day, date_week, date_month, date_quarter and date_year

The `date_` functions are similar to the `day_` function, but instead of returning cardinal values for things like `get_day`, they return properly truncated and interval correct Unix timestamps. 

For example, `get_month(event_time)` would return the month number for the `event_time`, whereas `date_month(event_time)` would return the Unix stamp for the very millisecond that month started. 

```python
# count all people by date for month of purchase
match where action is 'purchase': # match all purchase events
    tally(date_month(event_time))
```

## Tally, Emit, Schedule

##### emit

The `emit` function is used in OpenSet triggers, it is used to emit a named event to a trigger channel. 

```python
# count product_skus in purchases, when they reach 100
# emit an event and schedule a folloup tirgger to run in 90 days
def on_insert():
    counter = 0    
    match where action is 'purchase' and product_sku is not None:
        counter += 1
        if counter >= 100:
            schedule(90 days, "check_for_200")
            emit("reached one hundred purcahsed")
```

> :pushpin: `emit` also exits a script

##### schedule

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
            emit("reached one hundred purcahsed")
```

##### tally

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

Generally tally is used to create useful pivots. For example if your events contained the attributes `country` and `product` you could generate a tree of `products` by `country` easily aggregating `people` for each `tally`:

```python
agg:
    people
    
match where product is not None and country is not None:
    tally(country, product)
```
or, if you wanted to see `country` by `product`:
```python
agg:
    people
    
match where product is not None and country is not None:
    tally(product, country)
```

> :pushpin: Tally used for segmentation will be discussed in a segmentation document.


## Session Functions

##### session
coming soon

##### session_count
coming soon

## Inline Aggregators

##### DISTINCT
##### COUNT
##### SUM
##### MIN
##### MAX
##### AVG

## Segment Math

##### population
##### intersection
##### union
##### compliment
##### difference

## Misc 

##### bucket
##### round
##### trunc
##### fix



