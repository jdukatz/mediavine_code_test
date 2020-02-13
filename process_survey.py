"""
Coding test for Mediavine data engineer application.
Reads in a csv of survey data and calculates some statistics on it.
"""
from __future__ import print_function, division
import csv
from collections import Counter

SURVEY_DATA_FILE = 'survey_data.csv'

class Aggregator:
    """
    Stores functions to gather statistics on the given data set. If there was some new value you needed to calculate
    you could add a function to this class and then invoke it in your data pipeline.
    """

    def __init__(self):

        self.aggregators = {
            'total_salary': 0,
            'num_rows': 0,
            'num_a_names': 0,
            'total_a_gpa': 0,
            'profession_counts': Counter(),  # this is a risk for memory blow-up because the professions could all be unique; if you were worried about this based on the file size you could use a bloom filter instead!
            'married_ages': [],
        }

    def run_all_aggregators(self, row):
        self.agg_salary(row)
        self.agg_a_name_gpa(row)
        self.agg_profession_counts(row)
        self.agg_married_ages(row)

    def agg_salary(self, row):
        self.aggregators['total_salary'] += float(row[header_index_map['salary']])
        self.aggregators['num_rows'] += 1

    def agg_a_name_gpa(self, row):
        name = row[header_index_map['name']]
        if name.split()[-1].lower()[0] == 'a':
            self.aggregators['num_a_names'] += 1
            self.aggregators['total_a_gpa'] += float(row[header_index_map['gpa']])

    def agg_profession_counts(self, row):
        self.aggregators['profession_counts'][row[header_index_map['job']]] += 1

    def agg_married_ages(self, row):
        married = row[header_index_map['married']]
        if married == 'true':
            age = int(row[header_index_map['age']])
            self.aggregators['married_ages'].append(age)

agg = Aggregator()

with open(SURVEY_DATA_FILE) as f:
    survey_reader = csv.reader(f, delimiter=',')
    headers = next(survey_reader)  # manually read the header row
    header_index_map = {h: i for i, h in enumerate(headers)}  # map for header lookup later
    for row in survey_reader:
        agg.run_all_aggregators(row)

# post processing steps to get our metrics out
mean_salary = agg.aggregators['total_salary'] / agg.aggregators['num_rows']
avg_gpa = agg.aggregators['total_a_gpa'] / agg.aggregators['num_a_names']
most_common_job = agg.aggregators['profession_counts'].most_common(1)[0]
# so in the "real world" I would use at least statistics.median() here (or use built in stats tools
# in a solution like pandas, spark, or kafka which would be waaaaaay more optimized than native python code)
# but in the spirit of the coding challenge I'm just doing this manually to show you I know what a median is :)
sorted_ages = sorted(agg.aggregators['married_ages'])
if len(sorted_ages) % 2 == 0:
    median_low_idx = len(sorted_ages) // 2 - 1
    median_high_idx = len(sorted_ages) // 2 + 1
    median_age = sum(sorted_ages[median_low_idx:median_high_idx]) / 2
else:
    # odd number of married people, just take the middle index
    median_idx = len(sorted_ages) // 2
    median_age = sorted_ages[median_idx]

print('1. {}'.format(mean_salary))
print('2. {}'.format(avg_gpa))
print('3. {} ({} instances)'.format(most_common_job[0], most_common_job[1]))
print('4. {}'.format(median_age))
