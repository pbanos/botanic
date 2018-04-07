# botanic
Regression-tree library and tool written in Golang

[![GoDoc](https://godoc.org/github.com/pbanos/botanic?status.svg)](https://godoc.org/github.com/pbanos/botanic)
[![GoReportCard](https://goreportcard.com/badge/github.com/pbanos/botanic)](https://goreportcard.com/report/github.com/pbanos/botanic)

<!-- TOC -->

- [botanic](#botanic)
    - [Basic concepts](#basic-concepts)
    - [botanic CLI](#botanic-cli)
        - [Installation](#installation)
        - [Use](#use)
            - [Help command](#help-command)
            - [Dataset command](#dataset-command)
                - [Metadata YAML file](#metadata-yaml-file)
                - [CSV datasets](#csv-datasets)
                - [Split subcommand](#split-subcommand)
            - [Tree command](#tree-command)
                - [Grow subcommand](#grow-subcommand)
                - [Test subcommand](#test-subcommand)
                - [Predict subcommand](#predict-subcommand)
            - [Version command](#version-command)
    - [State and roadmap](#state-and-roadmap)

<!-- /TOC -->

## Basic concepts

* Feature. In machine learning, a feature is 'an individual measurable property or characteristic of a phenomenon being observed' [according to Wikipedia](https://en.wikipedia.org/wiki/Feature_(machine_learning)). In botanic we distinguish between continuous features, which can take any real number value, and discrete features, which take values from a list of strings.
* Sample. In botanic a sample represents a phenomenon being observed in terms of specific values for the features that are measured.
* Dataset. In botanic a dataset is a compendium of samples. You will usually start with a dataset that has all your data as samples and split it into a training dataset, with a majority of the samples and to be used for growing a tree, and a testing dataset, with a minority of the samples and to be used to test the grown tree.
* Tree. A tree in botanic is a decision or regression tree, also known as Classification and Regression Tree (CART). With botanic you can grow trees that predict a discrete feature (called the label feature) training them with a dataset. Once they are grown, you can test them with another dataset to see how well they predict the label feature, and use them to predict the label feature of a sample.

## botanic CLI

The botanic command is a CLI tool that allows you to manage

* datasets: converting them between formats or event splitting them into training and testing datasets.
* regression trees: growing them from a dataset, testing them with another dataset or using them interactively to predict the value for a sample.

### Installation

Assuming you have Go set up in your system, just run:

`go get -u github.com/pbanos/botanic/cmd/botanic`

Make sure the $GOPATH/bin is listed in your PATH.

### Use

The botanic command currently accepts the following commands:

#### Help command

Running `botanic help` we get to see the available botanic commands and a description for them:
```
$ botanic help
A tool to grow regression trees from your data, test them, and use them to make predictions

Usage:
  botanic [command]

Available Commands:
  dataset     Manage datasets
  help        Help about any command
  tree        Manage regression trees
  version     Print the version number of botanic

Flags:
  -h, --help      help for botanic
  -v, --verbose

Use "botanic [command] --help" for more information about a command.
$
```
#### Dataset command
The `botanic dataset` command allows dumping an existing dataset with samples into another dataset, each in any of the following formats:
- CSV (as a file ending in .csv or by default read from STDIN or dumped to STDOUT if nothing is specified)
- an SQLite3 database (specified as a file ending in .db)
- a PostgreSQL database (specified as a [PostgreSQL Connection URI](https://www.postgresql.org/docs/current/static/libpq-connect.html#AEN45571))
- a MongoDB databse (specified as a [MongoDB Connection URI](https://docs.mongodb.com/manual/reference/connection-string/))

We can see the flags and subcommands available for it running it with the `--help` or `-h` flag:
```
$ botanic dataset --help
Manage datasets

Usage:
  botanic dataset [flags]
  botanic dataset [command]

Available Commands:
  split       Split a dataset into two datasets

Flags:
  -h, --help              help for dataset
  -i, --input string      path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)
  -m, --metadata string   path to a YML file with metadata describing the different features available available on the input file (required)
  -o, --output string     path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL to dump the output dataset (defaults to STDOUT in CSV)

Global Flags:
  -v, --verbose

Use "botanic dataset [command] --help" for more information about a command.
$
```
When working with this command or its subcommands, we will always need to specify the input dataset with the `-i` or `--input` flag, as well as the metadata YAML file that describes the features of the dataset with the `-m` or `--metadata` flag (explained [below](#metadata-yaml-file)). For example, the following command will read a dataset from the data.csv file and output it into an SQLite3 database on the data.db file:
```
botanic dataset --input data.csv -m metadata.yml -o data.db
```

##### Metadata YAML file

When working with botanic datasets, we will need a metadata YAML file that describes the features we are working with. The schema for the metadata YAML file is very simple:
- There is a `features` key at the root
- Under the `features` key there will be a key with the name of every feature available (as identified on the datasets this describes). The value for each feature will be:
  - The string `continuous` if the feature is continuous
  - An array of string values that are valid for the feature if the feature is discrete

Example:
```
features:
  Age: continuous
  Education:
    - "high school"
    - "bachelors"
    - "masters"
  Income:
    - low
    - high
  Marital Status:
    - married
    - single
  Prediction:
    - "will buy"
    - "won't buy"
```

##### CSV datasets

CSV will probably be the entry format for data into a botanic CLI workflow: nothing prevents you from generating a DB-based dataset from scratch, but CSV is easier.
CSV datasets should have a first row or header with the name of all the features of your samples (or at least those listed on your [metadata YAML file](#metadata-yaml-file)). Every row after that one represents a sample,
and should contain the values for the different features in the order of the row.

An example of CSV dataset with 3 samples would be:
```
Marital Status,Prediction,Age,Education,Income
married,won't buy,23,bachelors,high
married,won't buy,42,bachelors,low
married,will buy,56,masters,low
```

##### Split subcommand

The `botanic dataset split` command allows splitting an input dataset into 2 different datasets with different samples: the output dataset and the split dataset. This will come in handy when you want to split your data into a training dataset and a test dataset.

We can see the available flags for this subcommand running it with the `--help` or `-h` flag:
```
$ botanic dataset split --help
Split a dataset into an output dataset and a split dataset

Usage:
  botanic dataset split [flags]

Flags:
  -h, --help                    help for split
  -s, --split-output string     path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL to dump the output of the split dataset (required)
  -p, --split-probability int   probability as percent integer that a sample of the dataset will be assigned to the split dataset (default 20)

Global Flags:
  -i, --input string      path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)
  -m, --metadata string   path to a YML file with metadata describing the different features available available on the input file (required)
  -o, --output string     path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL to dump the output dataset (defaults to STDOUT in CSV)
  -v, --verbose
$
```

Most flag are self-explanatory, but the `-p` or `--split-probability` flag deserves a special mention: with it we can specify the probability that a sample will end up on the split dataset a percent (without the % symbol).

For example, to split the dataset in the data.db we generated before into a train dataset in a train.db SQLite3 file and a test dataset in a PostgreSQL database accesible at postgres://myuser:mypasswd@mypostgress.server.com/mydb, with a 90%-10% distribution among datasets, we would run:

```
botanic dataset split -i data.db -o train.db -s postgres://myuser:mypasswd@mypostgress.server.com/mydb -m metadata.yml --split-probability 10
```

#### Tree command
The `botanic tree` command works with trees.

On its own it shows a tree on the standard output, but it has some available subcommands. We can see the flags and subcommands available for it running it with the `--help` or `-h` flag:
```
$ botanic tree --help
Manage regression trees and use them to predict values for samples

Usage:
  botanic tree [flags]
  botanic tree [command]

Available Commands:
  grow        Grow a tree from a dataset
  predict     Predict a value for a sample answering questions
  test        Test the performance of a tree

Flags:
  -h, --help              help for tree
  -m, --metadata string   path to a YML file with metadata describing the different features used on a tree or available on an input dataset (required)
  -t, --tree string       path to a file from which the tree to show will be read and parsed as JSON (required)

Global Flags:
  -v, --verbose

Use "botanic tree [command] --help" for more information about a command.
$
```

To show a given tree, previously generated with the grow subcommand described below, we will specify the following flags:
- the path to the metadata YAML file describing the features in the tree, with the `--metadata` or `-m`flag
- the path to the json file with the tree to show, with the `--tree` or `-t` flag

For example, to show a tree in a tree.json file with our metadata.yml as metadata file we would run:
```Bash
botanic tree -m metadata.yml -t tree.json
```

##### Grow subcommand
The `botanic tree grow` command grows a tree from an input dataset: the training dataset.

We can see the flags available for it running it with the `--help` or `-h` flag:
```
$ botanic tree grow --help
Grow a tree from a dataset to predict a certain feature.

Usage:
  botanic tree grow [flags]

Flags:
      --concurrency int        limit to concurrent workers on the tree and on DB connections opened at a time (defaults to 1) (default 1)
      --cpu-intensive          force the use of cpu-intensive subsetting to decrease memory use at the cost of increasing time
  -h, --help                   help for grow
  -i, --input string           path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)
  -l, --label string           name of the feature the generated tree should predict (required)
      --memory-intensive       force the use of memory-intensive subsetting to decrease time at the cost of increasing memory use
  -o, --output string          path to a file to which the generated tree will be written in JSON format (defaults to STDOUT)
  -p, --prune string           pruning strategy to apply, the following are valid: default, minimum-information-gain:[VALUE], none (default "default")
      --queue-backend string   URI for a redis key (that will be used as prefix) so that a redis DB is used as backend for a queue for the tasks needed to develop the tree. If not given an in-memory queue will be used

Global Flags:
  -m, --metadata string   path to a YML file with metadata describing the different features used on a tree or available on an input dataset (required)
  -v, --verbose
$
```

To grow a tree we will probably have to specify:
- an input or training dataset, with the `--input` or `-i` flag. This can be a CSV file, an SQLite3 file (with the .db) or a PostgreSQL or MongoDB connection URL. The default is a CSV dataset read from STDIN.
- the label feature the tree has to predict, with the `--label` or `-l`flag
- the path to the metadata YAML file describing the features in the dataset, with the `--metadata` or `-m`flag

The following optional flags can also be useful:
- `--output` or `-o` specifies where to store the resulting tree, in JSON format. It defaults to STDOUT. 
- `--prune` or `-p` defines the pruning strategy to apply while growing the tree: branches whose development does not help in improving predictions enough will be pruned, that is, their subbranches will be discarded. The following strategies are available:
  - `default`: the default one
  - `minimum-information-gain`: this strategy imposes a minimum value for the information gain obtained from the subbranching. This value can be specified appending :VALUE to the strategy, for example: `--prune minimum-information-gain:0.05`
  - `none`: this strategy disables pruning
- `--queue-backend` allows configuring the command to use a redis DB as backend. The value for this option must be a valid `redis://` URI to a key in a redis DB. The key will actually be used only as prefix for queue keys on the DB. For example, the value `redis://:mypasswd@redis.example.net:6379/1/my-queue` would configure the use of the `1` redis DB on `redis.example.net:6379`, authenticated by the password `mypasswd` and using `my-queue` as key prefix. If this option is not specified, an in-memory implementation is used instead.

If the input or training dataset is in a CSV file, the following optional flags are available:
- `--cpu-intensive` selects a dataset implementation that will keep in memory a single copy of the dataset's samples, at the cost of a longer time of processing.
- `--memory-intensive` selects a dataset implementation that will make copies of the samples of the training dataset for every subset it needs to build to grow the tree. This speeds up the processing time at the cost of a significant increase of memory use.

If the input or training dataset is in an SQLite3 database file, the flag `--max-db-conns` allows limiting the number of file handlers open to the given number. This is specially useful if running the tool with a considerable number of features and samples on an OS that imposes a low limit on the number of files a process can have open at a given time, such as Mac OS X.

For example, to grow a tree that predicts the Prediction feature, using the training dataset we generated before in the SQLite3 file train.db, our metadata.yml as metadata file and so that the output tree is written to a tree.json file we would run:
```Bash
botanic tree grow -l Prediction -m metadata.yml -i train.db -o tree.json
```


##### Test subcommand
The `botanic tree test` command takes a tree and a test dataset and provides information on how well the tree predicts the samples on the testing dataset.

We can see the flags available for it running it with the `--help` or `-h` flag:
```
$ botanic tree test --help
Test the performance of a tree against a test dataset

Usage:
  botanic tree test [flags]

Flags:
  -h, --help           help for test
  -i, --input string   path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)
  -t, --tree string    path to a file from which the tree to test will be read and parsed as JSON (required)

Global Flags:
  -m, --metadata string   path to a YML file with metadata describing the different features used on a tree or available on an input dataset (required)
  -v, --verbose
$
```

For example, to test the tree we grown on file tree.json with the testing dataset we split into our PostgreSQL database accessible at postgres://myuser:mypasswd@mypostgress.server.com/mydb we would run:
```
botanic tree test -i postgres://myuser:mypasswd@mypostgress.server.com/mydb -m metadata.yml -t tree.json
```

The output could be similar to:
```
0.969696 success rate, failed to make a prediction for 0 samples
```
The success rate indicates the rate of successful predictions over the number of samples in the training dataset, while the failures to make a prediction indicate the situation where the generated tree does not have data to make a prediction for a sample at all.

##### Predict subcommand
The `botanic tree predict` subcommand can be used to predict the value for the label feature of a sample using a generated tree. The subcommand does not expect to have all available data for the sample, but rather will interact with the user to gather the values for the features as they are needed to traverse the tree.

We can see the flags available for the subcommand running it with the `--help` or `-h` flag:
```
$ botanic tree predict --help
Use the loaded tree to predict the label feature value for a sample answering a reduced set of question about its features

Usage:
  botanic tree predict [flags]

Flags:
  -h, --help                     help for predict
  -t, --tree string              path to a file from which the tree to test will be read and parsed as JSON (required)
  -u, --undefined-value string   value to input to define a sample's value for a feature as undefined (default "?")

Global Flags:
  -m, --metadata string   path to a YML file with metadata describing the different features used on a tree or available on an input dataset (required)
  -v, --verbose
$
```

Again, most of the flags are self-explanatory, but the `--undefined-value` or `-u` flag deserves a special mention. A generated tree allows predicting a sample even when this has no available value for a feature that determines the subtree to go down to: at every level a subtree for the scenario where the value is undefined is developed. This flag allows specifying which answer to a feature should be interpreted by the subcommand as the undefined value. You should make sure the one you use does not match an available feature's value.

#### Version command
The `botanic version` command shows the version number for the botanic command:
```
$ botanic version
botanic v0.0.1
$
```

## State and roadmap

The project is curently unstable and APIs and tool commands may suffer some changes. 

These are the items planned for future development of the botanic tool and libraries:

- Implement multi-process distributed growing of a tree
  * Develop a redis backend for queues
- Develop an backed tree node store implementations:
  * etcd
  * redis
- Implement other DB adaptors for datasets:
  * MySQL