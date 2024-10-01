import pandas as pd
import os

# folder of parquet files
FOLDER_PATH = r"/Users/jack/Desktop/Daniel Kim/untitled folder 3"

# Initialize empty DataFrame to store the final monthly labour flow
monthlyLabourFlow = pd.DataFrame()

# Initialize an empty DataFrame to store data from all files
all_data = pd.DataFrame()

# Read and combine all files into one DataFrame
for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".csv"):
        file_path = os.path.join(FOLDER_PATH, file_name)
        dataUSA = pd.read_csv(file_path)

        # Filter on only USA entries
        dataUSA = dataUSA[dataUSA['country'] == 'United States']

        # Append data from the current file to the combined DataFrame
        all_data = pd.concat([all_data, dataUSA], ignore_index=True)

# Convert start and end date columns to datetime type
all_data['startdate'] = pd.to_datetime(all_data['startdate'])
all_data['enddate'] = pd.to_datetime(all_data['enddate'])

# Sort by user_id and startdate to capture job transitions
all_data.sort_values(by=['user_id', 'startdate'], inplace=True)

# Calculate previous company, previous RCID, and previous end date for each user across the combined dataset
all_data['prev_company'] = all_data.groupby('user_id')['company_cleaned'].shift(1)
all_data['prev_rcid'] = all_data.groupby('user_id')['rcid'].shift(1)
all_data['prev_end_date'] = all_data.groupby('user_id')['enddate'].shift(1)

# Detect overlapping jobs by checking if the current start date is before the previous end date
all_data['is_overlap'] = all_data['startdate'] < all_data['prev_end_date']

# Handle overlapping jobs (multiple jobs at the same time)
overlapping_jobs = all_data[all_data['is_overlap']]

# Separate handling for job transitions where there is no overlap
transitions = all_data[~all_data['is_overlap'] & (all_data['company_cleaned'] != all_data['prev_company'])]

# Process overlapping jobs
if not overlapping_jobs.empty:
    overlapping_jobs['previous_end_month'] = overlapping_jobs['prev_end_date'].dt.to_period('M')
    overlapping_jobs['start_month'] = overlapping_jobs['startdate'].dt.to_period('M')

    overlapping_flow = overlapping_jobs.groupby(['prev_company', 'company_cleaned', 'previous_end_month', 'prev_rcid', 'rcid'],
                                                dropna=False).size().reset_index(name='count')
    overlapping_flow = overlapping_flow.rename(columns={'prev_company': 'Former_Company', 'company_cleaned': 'New_Company',
                                                        'previous_end_month': 'Transition_month', 'prev_rcid': 'Former_RCID',
                                                        'rcid': 'New_RCID', 'count': 'Labour Flow'})
    monthlyLabourFlow = pd.concat([monthlyLabourFlow, overlapping_flow], ignore_index=True)

# Determine months for the non-overlapping transitions
transitions['previous_end_month'] = transitions['prev_end_date'].dt.to_period('M')
transitions['start_month'] = transitions['startdate'].dt.to_period('M')

# Filter transitions that happened within the same month
same_month_transitions = transitions[transitions['previous_end_month'] == transitions['start_month']]

# Group by 'prev_company', 'company_cleaned', 'previous_end_month', 'prev_rcid', and 'rcid'
monthly_flow = same_month_transitions.groupby(['prev_company', 'company_cleaned', 'previous_end_month', 'prev_rcid', 'rcid'],
                                              dropna=False).size().reset_index(name='count')
monthly_flow = monthly_flow.rename(columns={'prev_company': 'Former_Company', 'company_cleaned': 'New_Company',
                                            'previous_end_month': 'Transition_month', 'prev_rcid': 'Former_RCID',
                                            'rcid': 'New_RCID', 'count': 'Labour Flow'})

# Append to the global DataFrame, ensuring no duplicates
if monthlyLabourFlow.empty:
    monthlyLabourFlow = monthly_flow
else:
    for _, row in monthly_flow.iterrows():
        # Create a mask to check if the same row already exists
        existing_row_mask = (
            (monthlyLabourFlow['Former_Company'] == row['Former_Company']) &
            (monthlyLabourFlow['New_Company'] == row['New_Company']) &
            (monthlyLabourFlow['Transition_month'] == row['Transition_month']) &
            (monthlyLabourFlow['Former_RCID'] == row['Former_RCID']) &
            (monthlyLabourFlow['New_RCID'] == row['New_RCID'])
        )

        # Check if any row matches
        if monthlyLabourFlow[existing_row_mask].empty:
            # If no match, append the new row
            monthlyLabourFlow = pd.concat([monthlyLabourFlow, pd.DataFrame([row])], ignore_index=True)
        else:
            # If a match exists, update the 'Labour Flow' count by adding the new value
            monthlyLabourFlow.loc[existing_row_mask, 'Labour Flow'] += row['Labour Flow']

# Output to Excel
monthlyLabourFlow.to_excel("/Users/jack/Desktop/Daniel Kim/monthly_labour_test2.xlsx")
