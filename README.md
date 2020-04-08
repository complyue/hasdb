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
for the **Graph**, and by
[Non-Volatile-Storage](https://en.wikipedia.org/wiki/non-volatile)
(practically i.e. clusters of hard disks or **SSD**s) backed
[Virtual Memory](http://en.wikipedia.org/wiki/Virtual_memory)
for the **Arrays** .

The
[Magic Supers](https://github.com/e-wrks/edh/tree/master/Tour#magical-supers)
mechanism from
[Edh](https://github.com/e-wrks/edh)
[Object System](https://github.com/e-wrks/edh/tree/master/Tour#inheritance-hierarchy)
makes the language itself directly viable as both the
[DDL](https://en.wikipedia.org/wiki/Data_definition_language)
and
[DML](https://en.wikipedia.org/wiki/Data_manipulation_language)
for database modeling, querying and updating, thus **neither** an
[Object-Graph Mapping Layer](https://github.com/neo4j/neo4j-ogm)
**nor** a separate
[Graph Query Language](https://github.com/graphql/graphql-spec)
is needed. While transactional semantics already carried by the object
attribute model, intrinsic to **Edh** the language, the persistence
capability is added to a vanilla **Edh** object, so as to be either an
**entity object** or a **relationship object**, as unintrusively as
wearing an exoskeleton.
