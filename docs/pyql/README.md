# The PyQL Query Language

OpenSet uses a language called PyQL to define it's queries. PyQL is inspired by the Python programming language. A scripting language based on Python was the right choice for a scripting language because:
1. It's expressive.
2. Widely understood by data scientists and other numbers people.
3. Quick to learn if you don't know it.
4. Easy to extend.
5. Simple to implement (more or less).

You can find the language refernce [here](https://github.com/perple-io/openset/blob/master/docs/pyql/language_reference.md).

#### What PyQL is not...

PyQL is not Python. 

Many of the robust features make Python great for all sorts of development are not found in PyQL. Many datatypes, list comprehensions, list slicing, and many other things you love about Python are not found here. 

> :baby: OpenSet is new. With time we will add more of your Python favorites.

#### What PyQL is...

PyQL was designed to be lightweight and fast. During a query a PyQL script can be executed millions of times, entering and exiting the script has to have no overhead.

PyQL was designed to be reset and restarted with nominal overhead. It avoids performance killers like garbage collection. It compiles down to opcode and is interpreted using a simple stack language. Internal functions are marshaled to C++ with no overhead. 

PyQL is an iteration language. Data in OpenSet consists of event sets (one for each persons data). PyQL has robust row iterators that carefully blend the concepts of Python and SQL to make iterating and matching simple. Iterations can be nested and branched.

PyQL is meant to be small. Small is fast.

# Anatomy of an  OpenSet query

To understand how a query works in OpenSet you must understand how it organizes data, and how it simplifies complex problems. The best way to explain this is by example.

#### The scenario

* You installed OpenSet.
* You've collected 100,000 events from 5,000 people using your app.
* You want to extract some behavioral analytics. 

**The Question:** You would like to tabulate a list of the **first feature** each of those 5000 users accessed when they first used your app --- and for each **first features** you would like to see a sub-list  the **next 3 feature** accessed by each of those users (but not if the next feature used is the same as the first feature). Oh... and you want distinct customer counts for the main list and it's sub-lists.

> :confused:  This sort of query is not well suited to SQL (it can probably be done, but it won't be pretty), and while easy enough to solve with a map-reducer, it will require multiple passes, more effort than necessary, and it won't be fast as it should be. 

#### Lets simplify the problem. 

What if you only had to solve this for one person. You write a script. The events for that one person are a time sequenced list in your script. You might iterate through until you find a **feature** event and record it. Then iterate for the **next 3 feature events** and record those. 

For output you make a JSON document and store the **first feature**, and under it you record the **next three features**. You store each of these with a **1** because you analyzed one customer.

I bet this script has less than 10 lines code. 

Fortunately, this is how OpenSet solves most problems. All behavioural problems are treated as single customer problems. That's because internally all user events are pre-mapped to individual users where they are sorted into compressed user event sets. If you have a million users in your dataset, you will essentially have a million really small tables.

Data that's pre-mapped, pre-sorted and pre-indexed means you only have to focus on the question (no more mapping and chaining jobs, or nested queries and joins).

How might this look. Lets assume that when a user performs an action, an with an `action`of `feature_used` is stored, and with that `action` a property of `feature_name` is stored.

```python
aggregate: # what we want to count
    people
      
# match one row where action is 'feature_used'
match 1 where action is 'feature_used':

    first_feature = feature_name # save the feature name
    iter_next() # move past first match
    
    # match 3 rows where action is 'feature_used' and
    # the feature_name isn't the same as the first match
    match 3 where action is 'feature_used' and 
            feature_name is not 'first_feature':
            
        # store it in our result set, with the new feature_name
        # grouped under the first_feature and aggregate people
        tally(first_feature, feature_name)
```

That's it. :boom: Just 5  lines of code (plus 2 to define the aggregator).

This little PyQL script will be compiled and distributed across all your nodes and all the processor cores within those nodes. You will get a giant JSON document in milliseconds.

> :fearful: Wait a second, what is `match`? Earlier we mentioned that PyQL is an iteration language. The match statement `match` is a fancy event iterator that combines an optional limiter and the functionality of an SQL where clause. `match` iterators can contain conditions and date ranges, and can be nested, continued and broken like regular iterators.



