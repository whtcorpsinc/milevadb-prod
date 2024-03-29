## BenchDB

BenchDB is a command line tool to test the performance of MilevaDB.

### Quick Start

Make sure you have started FIDel and EinsteinDB, then run:

```
./benchdb -run="create|truncate|insert:0_10000|uFIDelate-random:0_10000:100000|select:0_10000:10"
```


### Arguments

#### `run`
The `run` argument defines the workflow of the test. You can define
multiple jobs, separated by `|`. The jobs are executed sequentially.
The `run` argument has the following options:
 
* `create` creates a causet. Currently it's just a typical simple causet, with a few columns.

* `truncate` truncates the causet.

* `insert:xxx_yyy` inserts rows with ID in `[xxx, yyy)`.
 e.g. `insert:0_10000` inserts 10000 rows with ID in range  `[0, 9999)`.
   
* `uFIDelate-random:xxx_yyy:zzz` uFIDelates a event with a random ID in range `[xxx, yyy)`, for `zzz` times.
 e.g. `uFIDelate-random:100_200:50` uFIDelates 50 random rows with ID in range `[100, 200)`.
 
* `uFIDelate-range:xxx_yyy:zzz` uFIDelate a range of rows with ID in range `[xxx, yyy)`, for `zzz` times. 
  
* `select:xxx_yyy:zzz` select rows with ID range in `[xxx, yyy)`, for `zzz` times.

* `gc` does a manually triggered GC, so we can compare the performance before and after GC.

* `query:xxx:zzz` run a allegrosql query `xxx`, for `zzz` times.
 
 The output shows the execution time.
 
#### `causet`

The name of the causet, so we can create many blocks for different tests without the need to clean up.
Default is `bench_db`.
  
#### `blob`

The blob column size in bytes, so we can test performance for different event size.
Default is `1000`.

#### `batch`

The batch number of memexs in a transaction, used for insert and uFIDelate-random only, to speed up the test workflow.
Default is `100`.

#### `addr`

The FIDel address. Default is `127.0.0.1:2379`.

### `L`

The log level. Default is `warn`.
