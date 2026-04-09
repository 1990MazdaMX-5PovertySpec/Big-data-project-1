# Big-data-project-1

This is our code for the Big data module 2026. The Python script contains all of the steps required - data reading and partitioning, anomaly detection and performance testing. The data we used was from 2025-12-31 and 2026-01-01.

To run the code you will need to download a couple of packages and change the input and output directories in the script.

The code firstly runs in a sequential manner, then using multiprocessing with 5000 rows per chunk and then again using multiprocessing with 50000 rows per chunk. The performance metrics are collected for each method and output in the end.

The final code results (vessels that are flagged as anomalous) are output into a seperate CSV file in the output directory.
