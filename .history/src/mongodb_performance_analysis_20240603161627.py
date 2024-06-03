import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def visualize_query_performance(input_file, output_dir):
    # Load the encoded CSV file
    df = pd.read_csv(input_file)

    # Convert the dataframe from wide to long format for easier plotting
    df_long = pd.melt(df, id_vars=['Query'], var_name='Index_Combination', value_name='Execution_Time')

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set the style of the visualization
    sns.set(style="whitegrid")

    # Create a bar plot for each query showing the execution time for each index combination
    plt.figure(figsize=(15, 10))
    sns.barplot(x='Execution_Time', y='Index_Combination', hue='Query', data=df_long, palette='viridis')
    plt.title('Execution Time of Queries for Different Index Combinations')
    plt.xlabel('Execution Time (seconds)')
    plt.ylabel('Index Combination')
    plt.legend(title='Query', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.savefig(os.path.join(output_dir, 'barplot_execution_times.png'), bbox_inches='tight')
    plt.show()

    # Create a box plot for each query showing the distribution of execution times
    plt.figure(figsize=(15, 10))
    sns.boxplot(x='Execution_Time', y='Query', data=df_long, palette='viridis')
    plt.title('Distribution of Execution Times for Queries')
    plt.xlabel('Execution Time (seconds)')
    plt.ylabel('Query')
    plt.savefig(os.path.join(output_dir, 'boxplot_execution_times.png'), bbox_inches='tight')
    plt.show()

    # Create a heatmap to show the execution times for each query and index combination
    pivot_df = df_long.pivot(index='Query', columns='Index_Combination', values='Execution_Time')
    plt.figure(figsize=(15, 10))
    sns.heatmap(pivot_df, cmap='viridis', linewidths=.5, cbar_kws={'label': 'Execution Time (seconds)'})
    plt.title('Heatmap of Execution Times for Queries and Index Combinations')
    plt.xlabel('Index Combination')
    plt.ylabel('Query')
    plt.savefig(os.path.join(output_dir, 'heatmap_execution_times.png'), bbox_inches='tight')
    plt.show()


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
        "(('samples.sample_id', 1), ('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I3+I4+I5",
        "(('samples.sample_id', 1), ('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I3+I4+I5",
        "(('samples.sample_id', 1), ('samples.microorganisms.microorganism_id', 1), ('disease', 1), ('samples.sample_type', 1), ('microorganism_id', 1))": "I1+I2+I3+I4+I5"
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

def save_min_times(input_file: str, output_file: str):
    # Load the data
    data = pd.read_csv(input_file)
    
    # Initialize the result list
    results = []
    
    # Iterate over each row in the dataframe
    for index, row in data.iterrows():
        query = row['Query']
        # Extract all time columns except 'N0'
        times = row.drop(labels=['Query', 'N0']).values
        # Find the minimum non-zero time
        min_time = min(time for time in times if time > 0)
        # Find the corresponding indices
        indices = row.drop(labels=['Query', 'N0']).index[row.drop(labels=['Query', 'N0']).values == min_time].tolist()
        
        # Append the result
        results.append({
            'Query': query,
            'Min_time': min_time,
            'Indexes': ','.join(indices)
        })
    
    # Create a DataFrame from the results
    result_df = pd.DataFrame(results)
    
    # Save the results to the output CSV file
    result_df.to_csv(output_file, index=False)

def main():
    input_file = './query_optimization/mongodb/query_performance.csv'
    output_file = './query_optimization/mongodb/encoded_performance_mongodb.csv'
    #encode_and_save_csv(input_file, output_file)
    #visualize_query_performance(output_file, "./query_optimization/")
    save_min_times(input_file=output_file, output_file="'./query_optimization/mongodb/best_times_mongodb.csv'")

if __name__ == "__main__":
    main()

