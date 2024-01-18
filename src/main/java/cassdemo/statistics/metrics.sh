#!/bin/bash

# Define your keyspace
KEYSPACE="srds"

# Output file
OUTPUT_FILE="metrics.txt"

# Function to calculate rate of change
calculate_rate() {
    current_value=$1
    previous_value=$2
    time_diff=$3

    # Avoid division by zero
    if [ "$time_diff" -eq 0 ]; then
        echo "N/A"
    else
        rate=$(awk "BEGIN { printf \"%.2f\", ($current_value - $previous_value) / $time_diff }")
        echo "$rate"
    fi
}

while true; do
    # Clear the terminal
    clear  # Use 'cls' on Windows

    # Get current date and time
    CURRENT_DATE=$(date "+%Y-%m-%d %H:%M:%S")

    # Run nodetool cfstats for the entire keyspace
    CFSTATS_OUTPUT=$(nodetool cfstats $KEYSPACE)

    # Extract specific data from cfstats output
    READ_LATENCY=$(echo "$CFSTATS_OUTPUT" | grep "Read Latency" | awk '{print $3}')
    WRITE_LATENCY=$(echo "$CFSTATS_OUTPUT" | grep "Write Latency" | awk '{print $3}')
    READ_COUNT=$(echo "$CFSTATS_OUTPUT" | grep "Read Count" | awk '{print $3}')
    WRITE_COUNT=$(echo "$CFSTATS_OUTPUT" | grep "Write Count" | awk '{print $3}')

    ROUNDED_READ_LATENCY=$(printf "%.4f" "$READ_LATENCY")
    ROUNDED_WRITE_LATENCY=$(printf "%.4f" "$WRITE_LATENCY")

    # Calculate rates
    CURRENT_TIMESTAMP=$(date +%s)
    READ_RATE=$(calculate_rate "$READ_COUNT" "$PREVIOUS_READ_COUNT" "$((CURRENT_TIMESTAMP - PREVIOUS_TIMESTAMP))")
    WRITE_RATE=$(calculate_rate "$WRITE_COUNT" "$PREVIOUS_WRITE_COUNT" "$((CURRENT_TIMESTAMP - PREVIOUS_TIMESTAMP))")

    # Print the extracted data
    echo "Read Latency: $ROUNDED_READ_LATENCY"
    echo "Write Latency: $ROUNDED_WRITE_LATENCY"
    echo "Read Count: $READ_COUNT (Rate: $READ_RATE /s)"
    echo "Write Count: $WRITE_COUNT (Rate: $WRITE_RATE /s)"
    echo ""

    # Write metrics to file
    echo "$CURRENT_DATE" >> "$OUTPUT_FILE"
    echo "Read Latency: $READ_LATENCY" >> "$OUTPUT_FILE"
    echo "Write Latency: $WRITE_LATENCY" >> "$OUTPUT_FILE"
    echo "Read Count: $READ_COUNT (Rate: $READ_RATE /s)" >> "$OUTPUT_FILE"
    echo "Write Count: $WRITE_COUNT (Rate: $WRITE_RATE /s)" >> "$OUTPUT_FILE"
    echo "---------------------" >> "$OUTPUT_FILE"

    # Update previous values
    PREVIOUS_READ_COUNT=$READ_COUNT
    PREVIOUS_WRITE_COUNT=$WRITE_COUNT
    PREVIOUS_TIMESTAMP=$CURRENT_TIMESTAMP

    # Sleep for 10 seconds before the next iteration
    sleep 10
done
