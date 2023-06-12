#!/bin/bash

# Example usage:
# OUT_PATH=map-of-online-chess/parse/data/pgn/lichess_db_standard_rated_2013-02_cleaned.pgn
# IN_PATH=map-of-online-chess/parse/data/pgn/lichess_db_standard_rated_2013-02.pgn
# rewrite_pgn.sh $IN_PATH $OUT_PATH

input_file=$1
output_file=$2

# Initialize variables
delim="[Event "
buffer=""
new_line="true"

# Remove output file if it exists
if [ -f "$output_file" ]; then
    rm $output_file
fi

# Read the input file line by line
while IFS= read -r line
do
    # Check if the line starts with the delimiter
    if [[ $line == $delim* ]]; then
        # If this is not the first line, write buffer content to output file and add a new line
        if [[ $new_line == "false" ]]; then
            echo "$buffer" >> $output_file
        fi
        buffer=$line
        new_line="false"
    else
        # Concatenate the line to the buffer
        buffer=$buffer'###'$line
    fi
done < "$input_file"

# Write the last buffer content to the output file
echo "$buffer" >> $output_file