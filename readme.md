# Mediavine Code Challenge

Solution for a code challenge for data engineer position at Mediavine

The `process_survey.py` script runs an analysis to get a few metrics out of the provided dataset. Even though the provided csv fits into memory comfortably, this solution only stores one row of data from it at a time, so it should be able to theoretically handle arbitrarily sized datasets. It gathers the information it needs and stores it in the `Aggregator` class, then runs a few post-processing steps to get the values requested and then prints to the terminal. This way it only stores what it needs in memory instead of loading the entire dataset and making multiple passes through it.

To run, just put the csv (not included in this repo) in the same directory as the script, and run `python process_survey.py` from the command line. I wrote it with python 3 in mind but it should work with python 2 as well.

As I mentioned, this solution is memory efficient, but if I were to encounter a problem like this "in the wild" I would definitely start with a solution like `pandas` which is highly optimized to be much faster than native python code. If the dataset were too large for memory I'd use a distributed solution like spark, and if it were not possible to know the size beforehand I would probaby implement a streaming solution using something like Kafka.
