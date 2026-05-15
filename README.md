# mongo-catalog-tracker
Data analysis for reporting evolution of the an active collection set over time for a MongoDb application with user-created collections

## Example use scripts
A collection of scripts are provided under scripts

### scripts/getSample.sh

This is an illustration of a schedulable task for collecting daily keyhole index operation counts from a uniquely named MongoDB deployment

### scripts/basicExtract.sh 
### scripts/rawToCsv.pl 

basicExtract.sh is used to convert the output from `keyhole --index <mongoDBUrl>` to a CSV format by stripping away some common header and footer output that keyhole
will often produce and then calls rawToCsv.pl to do the format conversion.  Run it while the current working directory of your shell is the root of a daily MonogDB index
counts report dump.

rawToCsv.pl is used by basicExtract.sh after stripping out header and footer that would otherwise require special handling here.  It is only meant to be called from `basicExtract.sh`.   It produces a CSV file with the following columns (an no header)
-- collection name
-- index spec
-- operation count
-- first observed timestamp

### scripts/defaultRun.sh

To exercise the loading and reporting pipeline without observing a live system, the mongos_worksim subproject contains a simulator that models the rolling focus, occasional
deletion, and pre-existing reactivation patterns of a hypothetical use community, then drives MongoDB just enough to re-create a sufficient but minimal workload to recreate theworkload pattern described by runing the model and collects Keyhole captures at the boundary between simulated days, yielding a directory structure of simulated MongoDB index rreports for use with the dahsboard side of this project.

### scripts/createSankey.sh

Given a directory structure of Keyhole index reports, all named `idx.log` in a directory of subdirectories, each named with an encoding of the date when the report it contains
was acquired.  The format uses the number of days since Unix Epoch for the encoded date and the number of seconds past midnight on the day of that moment, separatated by
an underscore: `<days_since_epoch>_<seconds_since_midnight>`

This takes one argument, either `orc` or `parquet`, specifying which format strategy to use.

The location scanned is currently embedded in the script, but it should become an argument (See Jira Epic XXX).   What the script does its as follows:
1) Iterate through the root directory structure of past MongoDB daily Index reports:
1a) Call basicExtract.sh in each subdirectory to transform index report there to a CSV for ingestion through PySpark.
2) Using either orc or parquet's implementation, load and analyze the counter data from the first data set collected to the last
3) Apply an inactivity policy and extract a report and sankey diagram showing how MongoDB's inactive collection set accumulates over time, particularly how many collections will be treated for inactivity and restored for returning to an active state.
4) Call scripts/finishSankey.sh to dump the sankey diagram in a form importable by `Sankeymatic`

### scripts/finishSankey,sh

Given a that Spark's orc or parquet data lake has been loaded and enriched with labels decribing how each is trated from day to day under a given days to inactivity policy

## Using Sankeymatic

These tools produce a text document in the format expected by a utility for generating Sankey diagrams found at http://XXX/
