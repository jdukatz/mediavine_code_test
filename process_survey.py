"""
Coding test for Mediavine data engineer application.
Reads in a csv of survey data and calculates some statistics on it.
"""
import csv
from collections import Counter

SURVEY_DATA_FILE = 'survey_data.csv'
survey_rows = []
with open(SURVEY_DATA_FILE) as f:
    survey_reader = csv.reader(f, delimiter=',')
    headers = next(survey_reader)  # manually read the header row
    header_index_map = {h: i for i, h in enumerate(headers)}  # map for header lookup later
    for row in survey_reader:
        survey_rows.append(row)

# total average salary
total_salary = 0
for row in survey_rows:
    total_salary += float(row[header_index_map['salary']])
mean_salary = total_salary / len(survey_rows)

# avg gpa for a last names
total_gpa = 0
num_a_names = 0
for row in survey_rows:
    name = row[header_index_map['name']]
    if name.split()[-1].lower()[0] == 'a':
        total_gpa += float(row[header_index_map['gpa']])
        num_a_names += 1
avg_gpa = total_gpa / num_a_names

# most popular profession
professions_by_counts = Counter()
for row in survey_rows:
    professions_by_counts[row[header_index_map['job']]] += 1
most_common_job = professions_by_counts.most_common(1)[0]

# median age
married_ages = []
for row in survey_rows:
    married = row[header_index_map['married']]
    if married == 'true':
        age = int(row[header_index_map['age']])
        married_ages.append(age)

# so in the "real world" I would use at least statistics.median() here (or use built in stats tools
# in a solution like pandas, spark, or kafka which would be waaaaaay more optimized than native python code)
# but in the spirit of the coding challenge I'm just doing this manually to show you I know what a median is :)
sorted_ages = sorted(married_ages)
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

