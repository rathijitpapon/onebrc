# One Billion Row Challenge

## Problem Statement

This is a coding challenge to process a large dataset of temperature measurements. The dataset is a text file with a line for each measurement, where each line contains a station name and a temperature value separated by a semicolon. The goal is to calculate the min, max, and mean temperature value of each station as fast as possible.

This problem is inspired by [1BRC](https://github.com/gunnarmorling/1brc)

## Constraints

- No external library dependencies may be used
- The computation must happen at application runtime, i.e. you cannot process the measurements file at build time and just bake the result into the binary
- Input value ranges are as follows:
  - Station name: non null UTF-8 string of min length 1 character and max length 100 bytes, containing neither ; nor \n characters. (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
  - Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
- There is a maximum of 10,000 unique station names
- Line endings in the file are \n characters on all platforms
- Implementations must not rely on specifics of a given data set, e.g. any valid station name as per the constraints above and any data distribution (number of measurements per station) must be supported
- The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive"

## Input Generation
```bash
git clone https://github.com/gunnarmorling/1brc.git
cd 1brc
./mvnw clean verify
./create_measurements.sh 1000000000

# This will generate a file measurements.txt with 1 billion measurements
# Copy the generated measurements.txt file to the root of this project
```

## Execution

```bash
cargo run --release

# To test with small data use the following line in `src/main.rs`
let file_path = "weather_stations.csv";
```

## Output

```bash
# Average Execution
Total lines: 1000000000
Total stations: 413
Elapsed time: 12.828243209s

# Device Info: M3 Pro, 11 Cores CPU, 18GB RAM
```
