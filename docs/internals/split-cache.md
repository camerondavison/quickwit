
# Split cache

Quickwit includes a split cache. It can be useful for specific workloads:
- to improve performance
- to reduce the cost associated with GET requests.

The split cache stores entire split files on disk.
It works under the following configurable constraints:
- bandwidth limitation
- number of concurrent download
- number of in-memory tracked splits
- amount of disk space
- number of on-disk files.

Searcher get tipped by indexers about the existence of splits (for which they have the best affinity).
They also might learn about split existence, when they need to perform a search request.

The searcher is then in charge of maintaining an in-memory datastructure with a bounded list of splits it knows about
and their score.

A download task then continuously attempts to download splits and insert them in the cache.
The task just loops on just pops the best candidate according to some score.
If the score is higher than the best file in cache, it does all necessary cache evictions and initiates download.

During their lifetime, splits go through the following stage.
Exist
Last Read date


