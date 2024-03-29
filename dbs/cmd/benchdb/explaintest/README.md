# ExplainTest

ExplainTest is a explain test command tool, also with some useful test cases for MilevaDB execute plan logic, we can run case via `run-tests.sh`.

```
Usage: ./run-tests.sh [options]

    -h: Print this help message.

    -s <milevadb-server-path>: Use milevadb-server in <milevadb-server-path> for testing.
                           eg. "./run-tests.sh -s ./explaintest_milevadb-server"

    -b <y|Y|n|N>: "y" or "Y" for building test binaries [default "y" if this option is not specified].
                  "n" or "N" for not to build.
                  The building of milevadb-server will be skiped if "-s <milevadb-server-path>" is provided.

    -r <test-name>|all: Run tests in file "t/<test-name>.test" and record result to file "r/<test-name>.result".
                        "all" for running all tests and record their results.

    -t <test-name>: Run tests in file "t/<test-name>.test".
                    This option will be ignored if "-r <test-name>" is provided.
                    Run all tests if this option is not provided.

    -v <vendor-path>: Add <vendor-path> to $GOPATH.

    -c <test-name>|all: Create data according to creating memexs in file "t/<test-name>.test" and save stats in "s/<test-name>_blockName.json".
                    <test-name> must has a suffix of '_stats'.
                    "all" for creating stats of all tests.

    -i <importer-path>: Use importer in <importer-path> for creating data.

    -p <portgenerator-path>: Use port generator in <portgenerator-path> for generating port numbers.
```

## How it works

ExplainTest will read test case in `t/*.test`, and execute them in MilevaDB server with `s/*.json` stat, and compare explain result in `r/*.result`.

For convenience, we can generate new `*.result` and `*.json` from execute by use `-r` parameter for `run-tests.sh`

## Usage

### Regression InterDircute Causet Modification

After modify code and before commit, please run this command under MilevaDB root folder.

```sh
make dev
```

or

```sh
make explaintest
```
It will identify execute plan change.

### Generate New Stats and Result from InterDircute

First, add new test query in `t/` folder.

```sh
cd cmd/explaintest
./run-tests.sh -r [casename]
./run-tests.sh -c [casename]
``
It will generate result and stats base on last execution, and then we can reuse them or open editor to do some modify.
