import pandas as pd
import os

# folder of parquet files
FOLDER_PATH = r"/Users/jack/Library/CloudStorage/GoogleDrive-jckemo429@gmail.com/My Drive/academicduplicatetest"

# Initialize empty DataFrame to store the final monthly labour flow
monthlyLabourFlow = pd.DataFrame()

# Initialize a global Series to accumulate user_id counts across all files
global_user_counts = pd.Series(dtype='int')
firstPassCount = 0
secondPassCount = 0

# First pass: accumulate user_id counts across all files
for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(FOLDER_PATH, file_name)
        dataUSA = pd.read_parquet(file_path)
        firstPassCount += 1
        print(firstPassCount)

        # Filter on only USA entries
        dataUSA = dataUSA[dataUSA['country'] == 'United States']

        # Count user_id occurrences in this file and update the global count
        single_Entries = dataUSA['user_id'].value_counts()
        global_user_counts = global_user_counts.add(single_Entries, fill_value=0)

# Filter for users who appear more than once across all files
users_who_changed_jobs = global_user_counts[global_user_counts > 1].index

# Second pass: process the files again to capture the transitions for users who changed jobs
for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(FOLDER_PATH, file_name)
        dataUSA = pd.read_parquet(file_path)
        secondPassCount += 1
        print(secondPassCount)

        # Filter on only USA entries and users who changed jobs
        dataUSA = dataUSA[(dataUSA['country'] == 'United States') &
                          (dataUSA['user_id'].isin(users_who_changed_jobs))]

        # Convert start and end date columns to datetime type, and sort by user and startDate
        dataUSA['startdate'] = pd.to_datetime(dataUSA['startdate'])
        dataUSA['enddate'] = pd.to_datetime(dataUSA['enddate'])
        dataUSA.sort_values(by=['user_id', 'startdate'], inplace=True)

        # Calculate previous company, previous RCID, and previous end date for each user
        dataUSA['prev_company'] = dataUSA.groupby('user_id')['company_cleaned'].shift(1)
        dataUSA['prev_rcid'] = dataUSA.groupby('user_id')['rcid'].shift(1)
        dataUSA['prev_end_date'] = dataUSA.groupby('user_id')['enddate'].shift(1)

        # Detect overlapping jobs by checking if the current start date is before the previous end date
        dataUSA['is_overlap'] = dataUSA['startdate'] < dataUSA['prev_end_date']

        # Handle overlapping jobs (multiple jobs at the same time)
        overlapping_jobs = dataUSA[dataUSA['is_overlap']]

        # Separate handling for job transitions where there is no overlap
        transitions = dataUSA[~dataUSA['is_overlap'] & (dataUSA['company_cleaned'] != dataUSA['prev_company'])]

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
monthlyLabourFlow.to_excel("/Users/jack/Desktop/Daniel Kim/monthly_labour_flow_bulk_test.xlsx")
