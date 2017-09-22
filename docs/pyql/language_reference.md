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

Match is the standard row Iterator. When a user record is mounted, a PyQL script started is started with the iterator on the first row of their event list.

```
match:
    do_something() # on each row
```

``` 
match 5:
    do_something() # on the first five matches
```

``` 
match where some_column is 'some_value':
    do_something() # on each row that matches the filter
```

```
match 1 where some_column is 'some_value':
    do_something() # on the first match that matches the filter
```

```
match 
```

##### iter_get
##### iter_set
##### iter_move_last
##### iter_move_first
##### iter_next
##### iter_prev
##### event_count

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

Match between two dates (ISO 8601 strings, unix epoch seconds or unix epoch milliseconds).
```python
match where iter_between('2017-09-21T22:00:00Z', '2017-09-21T22:59:59Z'):
   do_something() # on rows between these dates
```

## Functions, Iterators and Flow Control

##### def
##### return
##### for/in
##### continue
##### break
##### exit

## Time 

##### now
##### event_time
##### last_event
##### first_event
##### prev_match
##### first_match
##### fix
##### to_seconds
##### to_minutes
##### to_hours
##### to_days
##### get_second
##### date_second
##### get_minute
##### date_minute
##### get_hour
##### date_hour
##### date_day
##### get_day_of_week
##### get_day_of_month
##### get_day_of_year
##### date_week
##### date_month
##### get_month
##### get_quarter
##### date_quarter
##### get_year
##### date_year

## Tally, Emit, Schedule

##### emit
##### tally
##### schedule

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



