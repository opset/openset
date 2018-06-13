# The PyQL Query Language

OpenSet uses a language called PyQL to define it's queries. PyQL is inspired by the Python programming language. A scripting language based on Python was the right choice for a scripting language because:
1. It's expressive.
2. Widely understood by data scientists and other numbers people.
3. Quick to learn if you are new to programming.
4. Easy to extend.
5. Simple to implement (more or less).

You can find the language refernce [here](https://github.com/perple-io/openset/blob/master/docs/pyql/language_reference.md).

#### What PyQL is not...

PyQL is not Python. 

Many of the features make Python great for all sorts of development are not found in PyQL. Some datatypes, features like list comprehensions and lamdas, and most of the SDK are not found here. 

> :baby: OpenSet is new. With time we will add more of your Python favorites.

#### What PyQL is...

PyQL was designed to be lightweight and fast. When a query is run  a PyQL script could be executed millions of times, entering and exiting the script must not have no overhead.

PyQL avoids performance killers like garbage collection. It compiles down to opcode and is interpreted using a simple stack language much like machine language. Internal functions are marshaled to C++ with no overhead. 

PyQL is an iteration language. Data in OpenSet consists of event sets (a contained set of rows for each person in the database). PyQL implements custom row iterators that carefully blend the concepts of Python and SQL to make iterating and matching simple. Iterations can be nested and branched, nested iterators can continue where previous iterators left off.

PyQL is meant to be small. Small is fast.

# Anatomy of an  OpenSet query

To understand how a query works in OpenSet you must understand how it organizes data, and how it simplifies complex problems. The best way to explain this is by example.

#### The scenario

* You installed OpenSet.
* You've collected 100,000 events from 5,000 people using your app.
* You want to extract some behavioral analytics. 

**The Question:** You would like to tabulate a list of the **first feature** each of those 5000 users accessed when they first used your app --- and for each **first features** you would like to see a sub-list  the **next 3 feature** accessed by each of those users (but not if the next feature used is the same as the first feature). You want distinct user counts for each feature and it's sub-features.

> :confused:  This sort of query is not well suited to SQL (it can probably be done, but it won't be pretty), and while easy enough to solve with a map-reducer, it will require multiple passes, more effort than necessary, and it won't be as fast a solution like OpenSet which was designed to solve this problem.

#### Lets simplify the problem. 

If you only had to solve this for one person, you could see how easy it would be to write a script. Lets imagine the events for that one person are a time sequenced list. You might iterate through until you find a **feature**, store that, then iterate for the **next 3 feature events** and store those. 

For output you make a JSON document and store the **first feature**, and under it you record the **next three features**. You store each of these with a **1** because you analyzed one customer.

You've probably solved this in a dozen lines or less.

Fortunately, this is how OpenSet solves most problems. All behavioural problems are treated as single customer problems. That's because internally all user events are pre-mapped to individual users where they are sorted into sequenced event sets. If you have a million users in your dataset, you will actually have a million really small event logs.

Data is pre-mapped, pre-sorted and pre-indexed meaning you only have to focus on the question (instead multiple map/reduce iterations).

Here is an example - lets assume that when a user performs an action, an with an `event`of `feature` is stored, and with that event a property of `feature_name` is stored.

```python

aggregate: # what we want to count
    people
      
# match one row (the first) where event is 'feature'
for 1 first_match in rows if 
        first_match['event'] == 'feature':
    
    # match the next 3 rows where event is 'feature' and
    # the 'feature_name' isn't the same as the first match
    continue for 3 sub_match in rows if 
           sub_match['event'] == 'feature' and 
           sub_match['feature_name'] != first_match['feature_name']:
            
        # store it in our result set, with the new feature_name
        # grouped under the first_feature and aggregate people
        tally(first_match['feature_name'], sub_match['feature_name'])
        
```

That's it. :boom: Just 3  lines of code (6 with formatting, and  2 to define the aggregator).

This little PyQL script will be compiled and distributed across  your OpenSet cluster and all the processor cores within those nodes. You will get a JSON document in milliseconds.

> :fearful: Wait a second, what is going on with those `for` loops? Earlier we mentioned that PyQL is an iteration language. The `for` loop when iterating `rows` is treated as an event iterator that combines a filter. This is similar to `where` in SQL. Likewise, the number following `for` is optional, but sets an match limit (much like `limit` or `top` in SQL), Lastly the `continue for` syntax tells OpenSet to continue matching the row after the outer match.



