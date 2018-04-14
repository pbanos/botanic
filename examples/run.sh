#!/bin/bash
unset CDPATH
DIR="$1"

if [ -z "$DIR" ]; then
    echo Need a data directory to work on
    exit 1
fi

if [ ! -d "$DIR" ]; then
    echo "$DIR is not a directory"
    exit 1
fi

rm "$DIR"/train.{db,csv} "$DIR"/test.csv "$DIR/tree.json"

METADATA_FILE="$DIR"/metadata.yml
TRAINING_DATASET="$DIR"/train.csv
TESTING_DATASET="$DIR"/test.csv
TREE_FILE="$DIR"/tree.json

if [ -n "$BOTANIC_SQLITE3" ]; then
    TRAINING_DATASET="$DIR"/train.db
elif [ -n "$BOTANIC_POSTGRESQL" ]; then
    TRAINING_DATASET="$BOTANIC_POSTGRESQL"
elif [ -n "$BOTANIC_MONGODB" ]; then
    TRAINING_DATASET="$BOTANIC_MONGODB"
fi

if [ -n "$BOTANIC_QUEUE_BACKEND" ]; then
  QUEUE_BACKEND_OPTION="--queue-backend $BOTANIC_QUEUE_BACKEND"
fi

if [ -n "$BOTANIC_NODE_STORE" ]; then
  NODE_STORE_OPTION="--node-store $BOTANIC_NODE_STORE"
fi

if [ -n "$2" ]; then
    LABEL="$2"
else
    LABEL="Label"
fi

if [ -n "$3" ]; then
    BOTANIC_WORKERS="$3"
fi

if [ -n "$BOTANIC_WORKERS" ]; then
  CONCURRENCY_OPTION="--concurrency $BOTANIC_WORKERS"
fi

if [ -z "$BOTANIC_DATASET" ]; then
    BOTANIC_DATASET="$DIR/data.csv"
fi

echo "botanic dataset split -m \"$METADATA_FILE\" -i \"$BOTANIC_DATASET\" -o \"$TRAINING_DATASET\" -s \"$TESTING_DATASET\" -p 20" && \
    botanic dataset split -m "$METADATA_FILE" -i "$BOTANIC_DATASET" -o "$TRAINING_DATASET" -s "$TESTING_DATASET" -p 20 && \
    echo "time botanic tree grow -l \"$LABEL\" -m \"$METADATA_FILE\" -i \"$TRAINING_DATASET\" -o \"$TREE_FILE\" $QUEUE_BACKEND_OPTION $NODE_STORE_OPTION $CONCURRENCY_OPTION" && \
    time botanic tree grow -l "$LABEL" -m "$METADATA_FILE" -i "$TRAINING_DATASET" -o "$TREE_FILE" $QUEUE_BACKEND_OPTION $NODE_STORE_OPTION $CONCURRENCY_OPTION && \
    echo "botanic tree test -m \"$METADATA_FILE\" -i \"$TESTING_DATASET\" -t \"$TREE_FILE\"" && \
    botanic tree test -m "$METADATA_FILE" -i "$TESTING_DATASET" -t "$TREE_FILE" && \
    echo Done

