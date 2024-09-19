import pandas as pd
import os

# folder of parquet files
FOLDER_PATH = r"/Users/jack/Desktop/Daniel Kim"

monthlyLabourFlow = pd.DataFrame()
firstFile = True

for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(FOLDER_PATH, file_name)
        # read data, filter on only USA entries and remove anyone who didn't leave their company
        dataUSA = pd.read_parquet(file_path)
        dataUSA = dataUSA[dataUSA['country'] == 'United States']

        # count how many times a record appears and then remove people who didn't change jobs
        single_Entries = dataUSA['user_id'].value_counts()
        peopleWhoChangedJobs = dataUSA[dataUSA['user_id'].isin(single_Entries[single_Entries > 1].index)]

        # convert start and date columns to datetime type, sort by user and startDate
        peopleWhoChangedJobs['startdate'] = pd.to_datetime(peopleWhoChangedJobs['startdate'])
        peopleWhoChangedJobs['enddate'] = pd.to_datetime(peopleWhoChangedJobs['enddate'])
        peopleWhoChangedJobs.sort_values(by=['user_id', 'startdate'])

        # figure out the previous company and previous end date for each user
        peopleWhoChangedJobs['prev_company'] = peopleWhoChangedJobs.groupby('user_id')['company_cleaned'].shift(1)
        peopleWhoChangedJobs['prev_end_date'] = peopleWhoChangedJobs.groupby('user_id')['enddate'].shift(1)

        # check if there was a transition
        transitions = peopleWhoChangedJobs[
        peopleWhoChangedJobs['company_cleaned'] != peopleWhoChangedJobs['prev_company']]

        # determine months
        transitions['previous_end_month'] = transitions['prev_end_date'].dt.to_period('M')
        transitions['start_month'] = transitions['startdate'].dt.to_period('M')

        # check if the transitions were in the same month
        same_month_transitions = transitions[transitions['previous_end_month'] == transitions['start_month']]

        # Group by 'previous_company', 'company', and 'previous_end_month' and count the number of transitions
        monthly_flow = same_month_transitions.groupby(['prev_company', 'company_cleaned', 'previous_end_month', 'rcid'],
                                                      dropna=False).size().reset_index(name='count')
        monthly_flow = monthly_flow.rename(columns={'prev_company': 'Former_Company', 'company_cleaned': 'New_Company',
                                                    'previous_end_month': 'Transition_month', 'count': 'Labour Flow'})

        if firstFile:
            monthlyLabourFlow = monthly_flow
            firstFile = False  # After processing the first file, set the flag to False
        else:
            # After the first file, check for duplicates before appending
            for _, row in monthly_flow.iterrows():
                # Create a mask to check if the same row already exists
                existing_row_mask = (
                        (monthlyLabourFlow['Former_Company'] == row['Former_Company']) &
                        (monthlyLabourFlow['New_Company'] == row['New_Company']) &
                        (monthlyLabourFlow['Transition_month'] == row['Transition_month'])
                )

                # Check if any row matches
                if monthlyLabourFlow[existing_row_mask].empty:
                    # If no match, append the new row
                    monthlyLabourFlow = pd.concat([monthlyLabourFlow, pd.DataFrame([row])], ignore_index=True)
                else:
                    # If a match exists, update the 'Labour Flow' count by adding the new value
                    monthlyLabourFlow.loc[existing_row_mask, 'Labour Flow'] += row['Labour Flow']

# output
monthlyLabourFlow['Transition_month'] = monthlyLabourFlow['Transition_month'].dt.to_timestamp()
monthlyLabourFlow.to_stata("monthly_labour_flow.dta")