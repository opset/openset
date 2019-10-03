# The OpenSet Language (OSL)

OpenSet uses a language called OSL to define it's queries. 

OSL borrow most of concepts from Ruby and Python, but introduces features specific to customer analysis. 

Likewise, many of the cooler features in Ruby and Python are not included in OSL. The idea being that OSL is small and lightweight - small is fast.

OSL queries are compiled into op codes and executed. In addition to being compiled the logic identified in a query is analyzed to optimize the query, this is why OSL uses chain functions such as `.within()` or `.ever()` to make clear intent of the logic.

You can find the language reference [here](https://github.com/perple-io/openset/blob/master/docs/osl/language_reference.md).

# Anatomy of an OpenSet query

To understand how a query works in OpenSet you must understand how it organizes data, and how it simplifies complex problems. The best way to explain this is by example.

#### The scenario

* You installed OpenSet.
* You've collected 10,000,000 events from 50,000 customers (purchases, returns, pageviews).

You would like a report containing a list of all your products, and for each of those, all the products purchased in the next order each customer made. For that subsequent purchase you also want the distinct count of customers, the count of products ordered and the sum value of those products (this is one of the examples in the main README.md).

:confused:  This sort of query is not well suited to SQL - it can probably be done, but it won't be pretty and will involve lots of joins and perhaps temp tables. 

:grimacing: This problem is more easily solved with map-reduction, but it won't be simple. The data will require multiple passes, require more programming effort than necessary, and it won't be as fast a solution like OpenSet which was designed specifically to solve these sorts of problems.

#### How OpenSet simplifies the problem. 

If you only had to solve the above question for one person, you could see how easy it would be to write a script. Lets imagine the events for that one person are a time sequenced list. You might iterate through until you find a **purchase**, store the products, then iterate until you found the **next purchase** and account for those. 

For output you make a JSON document and store the **first product**, and in a sub-branch you record the **next purchase**. You store each of these with a **1** and the **total price** because you analyzed one customer.

:sunglasses:  In your favorite programming language you've probably solved this in a dozen lines of code or less.

What you've done is exactly what OpenSet does.

All behavioural problems are treated as single customer problems. That's because internally all user events are pre-mapped to individual users where they are sorted into sequenced event sets. If you have a million users in your dataset, you will actually have a million really small sequenced event logs.

Data is pre-mapped, pre-sorted and pre-indexed at insert, meaning you only have to focus on the question in it's simplest form.

Here is an example - lets assume that when a customer performs an action, with an `event`name of `did_foo`. Each `did_foo` event has a `color_selected` property:

```python

select # what we want to count
    id
end
      
# match one row (the first) where event is 'feature'
for_each.limit(1) where event.is(== 'did_foo')

  # store the first color selected
  first_color = color_selected
      
  # match the next 3 rows where event is 'did_foo' and
  # the 'color_selected' isn't the same as the first match
  for_each.continue().next().limit(3) where event.is(== 'did_foo') && color_selected.is(!= first_color)     
    # store it in our result set, with the new feature_name
    # grouped under the first_feature and aggregate people
    << first_color, color_selected   
  end
end
```

:boom: That's it. 

Just 2 lines of actual logic plus a few lines to define the aggregations we want, store a temp variable and the `<<` to push the row to the aggregator. 

This little PyQL script will be compiled and distributed across  your OpenSet cluster, and distributed across all the cores within those nodes. 

The result set is shared between all these parallel queries. 

You will get a JSON result back in milliseconds.
