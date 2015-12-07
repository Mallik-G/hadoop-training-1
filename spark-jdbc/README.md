# JDBC Example

This is the example uses the jdbc connectivity of SparkSQL to create and read from SQL tables.

## Preparation

You need to create an empty database in some MySQL server.

## Running

    ./run_export.sh --weather data/weather/20* --stations data/weather/ish-history.txt --connection jdbc:mysql://localhost/training
