# Mediawiki history reconstruction -- Overview

As stated in the manual.md file, mediawiki history reconstruction
process has three main steps:
* Users history reconstruction
* Pages history reconstruction
* Revisions augmentation and denormalization

Users and pages history reconstruction have no dependencies
among them and can be run in any order. Revisions
augmentation and denormalization depends on both first ones.

A visualisation of the data flow is available as dia and png files
(`docs/dataflow.dia` and `docs/datafow.png`, not drawing them here,
they're way too big!)


## Users and pages history

We detail those two steps together because they are very similar,
both in term of conceptual problem to solve and therefore code to
work them.


### Functional aspects

For users and pages, history is reconstructed using their current states
from user and page tables, and the information of what happened in the past
from the logging table. This logging table is both a blessing and a curse.
Without it, no historical aspect of users nor pages could have been
reconstructed, so we're glad it exists. However, it has proven difficult
to consolidate histories using those events, mostly due to missing key information
for joining events together: page_ids for page_move events and user_ids for
user_rename events.

The only fields we could use to group events belonging to the same user or page
together are page title and namespace for pages, and username for users. But those
are subject to change with page moves and user renames. So the algorithm has to
keep track of those renames and moves while rebuilding page and user lineages.
With the assumption that only one page / user can have a given
(page title, namespace) / username at any given point in time, iterating through
descending-time-ordered events provides a safe way to keep track of the lineages.

The architecture of the job looks the same for users and pages:
1. Read avro data and normalize it into individual objects
2. Partition the data into workable splits (see next section for more details)
3. Apply the descending-time-ordered event linking algorithm to the splits
4. Apply a time-ascending propagation of certain values (now that lineage is rebuilt)
5. Write results to the given output path


### Scalability aspects

#### The problem

The algorithm described above uses a dictionary to keep in memory the state of every
page / user, changing while evolving backward in time. While it works well for a small
number of pages / users, it doesn't scale as-is over multiple machines.
More precisely partitioning the state dictionary can easily be done, but partitioning
the events data is tricky. Given that an initial state is in one and only one partition,
we need the entire lineage of that state to be in the same partition in order to allow
backward linking. But, lineage grouping *is* the precise problem that is hard to solve
in our situation, having no unique key per lineage!

#### The solution -- Graph Partitioning

A solution we found both correct, elegant and scalable is to swap our representation of states
and events to a graph one. Vertices are the event values allowing to link events between them, so in
our case (page title, namespace) or username, and the edges represent the existence of a move /
rename event from a (page title, namespace) / username to another. This representation enforces
every entire lineages to be contained in a
[connected component](https://en.wikipedia.org/wiki/Connected_component_(graph_theory))
of the graph (a connected component can contain multiple lineages, but a lineage is always
in one and only one connected component). Finally, graph representations and connected
components extraction algorithms are scalable and usual in graph libraries.

Yay !


## Revisions and denormalization

The problems solved by this job are not as functionally complex as the users and pages jobs ones.
They nonetheless took a lot of tweaking to get the job right from a scalability perspective.


### Functional aspects

This job has two main parts:
1. Enrich revisions with computed information
2. Denormalize revisions, users, and pages data

#### Revisions enrichment

Revisions as stored in sql (and avro) miss some interesting information that can be computed.

* Delete time - We know about deleted revisions by getting the archive table. However this table
doesn't store the timestamp of deletion. We therefore extract it from, in order:
  1. joining to the page information for delete events
  2. If no match, take the biggest timestamp of deleted revisions for the deleted page
  3. If no match (undefined page id), take the event timestamp.


* Difference of text size in bytes - We self-join the revision data (archive + live)
 on rev_id = parent_rev_id, and subtract child text length from its parent one. If a revision
 is the first (parent_rev_id = 0), we use the original text length value, and we use null in case
 of a valid parent_rev_id but no join was made (incomplete data).

* Per-user and per-page revision metrics: cumulative revision count per user and per page,
 and seconds from previous revision per user and per page. This involves two stages, one for
 per-user and the other for per-page, with similar process. First, group by user/page and sort
 each group by timestamp (see the secondary sorting trick section), and then apply a mapping
 function that provides its previous item (if any) to the currently worked one in order to
 compute cumulative count and distance.

* Revert information (reverted, reverting, and dates) - We extract reverts lists by grouping the revisions
 by page id and sha1 and sorting them by timestamp. The first revision of each of those group is
 what we call the revert-base (first revision to have a specific sha1 in time), and the following ones
 are reverts to that specific base. Those lists are then grouped by page id and sorted by revert-base,
 to be joined to revisions grouped by page id and sorted by timestamp. Then we update revisions as being
 reverting if it is in one of the revert lists (but not the base), and update revisions between a
 revert-base and a revert as being reverted.

#### Denormalization

After revisions are enhanced as described above, we denormalize them with their related user / page
(user is the user that made the revision, and page the page in which the revision belongs).
We say denormalization because it actually ends up having all data in the same table, but what
it actually means is a join using user id or page id (for users or pages states), with the constraint
of using the correct user / page states in time. For instance if a revision happened 3 years ago, we want
to join it with the user / page states that was true at that point in time.

In order to do that we group revisions by user id/ page id and sort then within each group by timestamp.
We do the same for users / pages states, and then we join them taking advantage of the sorting to assign
correct states to revisions. Finally, we do that same exact denormalization process for users events and
page events, to enhance them with data on the user that made the changes. We end up unioning the revisions,
users and pages denormalized data, and writing the results to the provided output path.


### Scalability aspects

#### The problem

The denormalization algorithm is based on grouping data by a key (user id or page id), and then sort
within each of the groups (by timestamp). For small-ish data, no problem: data of one full group fits
in memory, and sorting can happen easily. Problem occurs when the data of a group doesn't fit in memory.
In that case, sorting is way more complicated because of the need to spill data to disk.

#### The solution -- The secondary sorting trick

Solution to this problem is a well known trick in hadoop world: *secondary sorting*. What that means is,
instead of loading the data of the group into memory to sort it, we use a built-in mechanism of the
distributed computation system to **group by a key AND sort within each of the groups**.


Let's explain how this secondary sorting works.

When grouping by key in a distributed world, data is first grouped by the key on the workers (let's call them mappers)
where each portion of the data lives. Each of these grouped portions of data contain some keys (up to full key set if
it is small), and a subset of the values associated with those keys. This data is then split and sent to another set
of workers (let's call them reducers), with the insurance that a specific key is always sent, from any mapper, to the
same reducer. By doing that, the various values that were split among mappers for a given key are now in the same reducer.
But, Since data is received by a reducer from many mappers, some work needs to be done to merge those various input to
ensure all values associated with the same key are worked together. To facilitate this merging, data is in fact *grouped
and sorted* in the mappers, like that merging in the reducer is way easier.

Now that we know sorting happens when we group by key, why not take advantage of that sorting step? Let's not
only sort by the grouping key, but by a *more complex* key. The only (strong) constraint is that the complex sorting
keys are a superset of the simple grouping ones, in order to keep the grouped data collocated (if not, the grouping
aspect of the step actually fails ...).


And back to our problem ...

The way we implement the described behavior is by defining a *complex key* for our data:
* The partition key is used to group data (in our case it'll be either user id or page id)
* The rest of the key is used in addition to the partition key when sorting (in our case, timestamp).

With this complex key, we group by the partition key and then sort within each group by the rest of the key.

Hurray :)
