import pandas as pd
import os

# folder of parquet files
FOLDER_PATH = r"/Users/jack/Desktop/Daniel Kim/untitled folder 2"

# Initialize an empty list to accumulate data frames
data_frames = []

# Read all parquet files and filter on USA entries
for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(FOLDER_PATH, file_name)
        # read data, filter on only USA entries
        dataUSA = pd.read_parquet(file_path)
        dataUSA = dataUSA[dataUSA['country'] == 'United States']
        data_frames.append(dataUSA)

# Concatenate all data frames into one DataFrame
all_data = pd.concat(data_frames, ignore_index=True)

# Count how many times a record appears and remove people who didn't change jobs
# Now this accounts for people across different files since we concatenated everything
single_Entries = all_data['user_id'].value_counts()
peopleWhoChangedJobs = all_data[all_data['user_id'].isin(single_Entries[single_Entries > 1].index)]

# Convert start and end date columns to datetime
peopleWhoChangedJobs['startdate'] = pd.to_datetime(peopleWhoChangedJobs['startdate'])
peopleWhoChangedJobs['enddate'] = pd.to_datetime(peopleWhoChangedJobs['enddate'])

# Sort by user and startdate
peopleWhoChangedJobs.sort_values(by=['user_id', 'startdate'], inplace=True)

# Figure out the previous company and previous end date for each user
peopleWhoChangedJobs['prev_company'] = peopleWhoChangedJobs.groupby('user_id')['company_cleaned'].shift(1)
peopleWhoChangedJobs['prev_end_date'] = peopleWhoChangedJobs.groupby('user_id')['enddate'].shift(1)

# Check if there was a transition
transitions = peopleWhoChangedJobs[
    peopleWhoChangedJobs['company_cleaned'] != peopleWhoChangedJobs['prev_company']]

# Determine months
transitions['previous_end_month'] = transitions['prev_end_date'].dt.to_period('M')
transitions['start_month'] = transitions['startdate'].dt.to_period('M')

# Check if the transitions were in the same month
same_month_transitions = transitions[transitions['previous_end_month'] == transitions['start_month']]

# Group by 'previous_company', 'company', and 'previous_end_month' and count the number of transitions
monthlyLabourFlow = same_month_transitions.groupby(
    ['prev_company', 'company_cleaned', 'previous_end_month'],
    dropna=False
).size().reset_index(name='Labour Flow')

# Rename columns for clarity
monthlyLabourFlow.rename(columns={
    'prev_company': 'Former_Company',
    'company_cleaned': 'New_Company',
    'previous_end_month': 'Transition_month'
}, inplace=True)

# Save to Stata
monthlyLabourFlow['Transition_month'] = monthlyLabourFlow['Transition_month'].dt.to_timestamp()
monthlyLabourFlow.to_stata("monthly_labour_flow.dta")
