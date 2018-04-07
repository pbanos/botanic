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

if [ -n "$QUEUE_BACKEND" ]; then
  QUEUE_BACKEND_OPTION="--queue-backend $QUEUE_BACKEND"
fi

if [ -n "$2" ]; then
    LABEL="$2"
fi

if [ -z "$LABEL" ]; then
    LABEL="Label"
fi

if [ -z "$DATASET" ]; then
    DATASET="$DIR/data.csv"
fi

echo "botanic dataset split -m \"$METADATA_FILE\" -i \"$DATASET\" -o \"$TRAINING_DATASET\" -s \"$TESTING_DATASET\" -p 20" && \
    botanic dataset split -m "$METADATA_FILE" -i "$DATASET" -o "$TRAINING_DATASET" -s "$TESTING_DATASET" -p 20 && \
    echo "time botanic tree grow -l \"$LABEL\" -m \"$METADATA_FILE\" -i \"$TRAINING_DATASET\" -o \"$TREE_FILE\" $QUEUE_BACKEND_OPTION" && \
    time botanic tree grow -l "$LABEL" -m "$METADATA_FILE" -i "$TRAINING_DATASET" -o "$TREE_FILE" $QUEUE_BACKEND_OPTION && \
    echo "botanic tree test -m \"$METADATA_FILE\" -i \"$TESTING_DATASET\" -t \"$TREE_FILE\"" && \
    botanic tree test -m "$METADATA_FILE" -i "$TESTING_DATASET" -t "$TREE_FILE" && \
    echo Done

