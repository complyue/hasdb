# HasDB - Haskell Native Database

**HasDB** is a
[DBMS](https://en.wikipedia.org/wiki/Database#Database_management_system)
that manages
[in-memory](https://en.wikipedia.org/wiki/In-memory_database)
[Graphs](https://en.wikipedia.org/wiki/Graph_database)
those manifesting
[out-of-core](https://en.wikipedia.org/wiki/Out-of-core)
[Arrays](https://en.wikipedia.org/wiki/Array_DBMS)
.

**HasDB** is implemented on top of
[GHC](https://www.haskell.org/ghc/)
, powered by
[Software Transactional Memory](http://hackage.haskell.org/package/stm)
for **Atomicity** and **Isolation** in
[ACID](https://en.wikipedia.org/wiki/ACID)
, thus native to
[Haskell](https://haskell.org)
in the first place.

**HasDB** takes advantage of
[Edh](https://github.com/e-wrks/edh)
for easy programming of **Consistency** (the **C** in
[ACID](https://en.wikipedia.org/wiki/ACID)
) and **Concurrency** as a whole (again thanks to
[STM](http://hackage.haskell.org/package/stm)
),
in both the implementation of the **DBMS** itself, and implementations
of various database applications.

**Durability** in
[ACID](https://en.wikipedia.org/wiki/ACID)
is obtained through
[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
for the **Graph**, and from
[Non-Volatile-Storage](https://en.wikipedia.org/wiki/non-volatile)
(practically i.e. clusters of hard disks or **SSD**s) backed
[Virtual Memory](http://en.wikipedia.org/wiki/Virtual_memory)
for the **Arrays** .
