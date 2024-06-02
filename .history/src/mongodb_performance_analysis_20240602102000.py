import pandas as pd

def encode_and_save_csv(input_file, output_file):
    # Load the CSV file
    df = pd.read_csv(input_file)
    
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
        "(('samples.sample_id', 1),)": "I1",
        "(('samples.microorganisms.microorganism_id', 1),)": "I2",
        "(('disease', 1),)": "I3",
        "(('samples.sample_type', 1),)": "I4",
        "(('microorganism_id', 1),)": "I5",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1))": "I1+I2",
        "(('samples.sample_id', 1), ('disease', 1))": "I1+I3",
        "(('samples.sample_id', 1), ('samples.sample_type', 1))": "I1+I4",
        "(('samples.sample_id', 1), ('microorganism_id', 1))": "I1+I5",
        "(('samples.microorganisms.microorganism_id', 1), ('disease', 1))": "I2+I3",
        "(('samples.microorganisms.microorganism_id', 1), ('samples.sample_type', 1))": "I2+I4",
        "(('samples.microorganisms.microorganism_id', 1), ('microorganism_id', 1))": "I2+I5",
        "(('disease', 1), ('samples.sample_type', 1))": "I3+I4",
        "(('disease', 1), ('microorganism_id', 1))": "I3+I5",
        "(('samples.sample_type', 1), ('microorganism_id', 1))": "I4+I5",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('disease', 1))": "I1+I2+I3",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('samples.sample_type', 1))": "I1+I2+I4",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('microorganism_id', 1))": "I1+I2+I5",
        "(('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('samples.sample_type', 1))": "I2+I3+I4",
        "(('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('microorganism_id', 1))": "I2+I3+I5",
        "(('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I3+I4+I5",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('samples.sample_type', 1))": "I1+I2+I3+I4",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('microorganism_id', 1))": "I1+I2+I3+I5",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I2+I4+I5",
        "(('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I2+I3+I4+I5",
        "(('samples.sample_id', 1), ('disease', 1), ('samples.sample_type', 1))": "I1+I3+I4",
        "(('samples.sample_id', 1), ('disease', 1), ('microorganism_id', 1))": "I1+I3+I5",
        "(('samples.sample_id', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I4+I5",
        "(('samples.microorganisms.microorganism_id', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I2+I4+I5",
        "(('samples.sample_id', 1), ('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I3+I4+I5"
    }

    # Rename queries
    df['Unnamed: 0'] = df['Unnamed: 0'].map(query_codes)

    # Rename columns
    df = df.rename(columns=index_codes)

    # Rename the first column to 'Query'
    df = df.rename(columns={'Unnamed: 0': 'Query'})

    # Save the renamed dataframe to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Encoded CSV saved to {output_file}")

def main():
    input_file = './query_optimization/query_performance.csv'
    output_file = './query_optimization/encoded_performance_mongodb.csv'
    encode_and_save_csv(input_file, output_file)

if __name__ == "__main__":
    main()

