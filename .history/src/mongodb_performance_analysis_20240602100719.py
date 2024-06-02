import pandas as pd

# Load the CSV file
df = pd.read_csv('/mnt/data/query_performance.csv')

# Map original query names to codes
query_codes = {
    "Patient with the most diverse microbiome": "Q1",
    "Given a microorganism, identify the samples in which it is present and its concentration": "Q2",
    "Patients who suffer from a certain disease and samples": "Q3",
    "Number of samples per type of sample": "Q4",
    "Number of times a microorganism appears in the same sample type": "Q5",
    "Patients who suffer from and have been diagnosed with a disease and have that disease microorganism": "Q6",
    "Find species of microorganism with different sequence registered": "Q7"
}

# Map index combinations to codes
index_codes = {
    "No Indices": "N0",
    "((samples.sample_id, 1),)": "I1",
    "((samples.microorganisms.microorganism_id, 1),)": "I2",
    "((disease, 1),)": "I3",
    "((samples.sample_type, 1),)": "I4",
    "((microorganism_id, 1),)": "I5",
    # Add other combinations as needed
}

# Rename queries
df['Unnamed: 0'] = df['Unnamed: 0'].map(query_codes)

# Rename columns
df = df.rename(columns=index_codes)

# Display the renamed dataframe
df.head()
 &#8203;:citation[【oaicite:0】]&#8203;
