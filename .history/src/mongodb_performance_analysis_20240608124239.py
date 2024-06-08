import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import pdist
from typing import Any

class MongoDBVisualizer:
    def __init__(self, csv_path: str, figure_path: str):
        self.csv_path = csv_path
        self.figure_path = figure_path
        self.data = self.read_csv()

    def read_csv(self) -> pd.DataFrame:
        """Reads the CSV file and returns a pandas DataFrame."""
        return pd.read_csv(self.csv_path)

    def normalize_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Normalizes the data to have zero mean and unit variance."""
        return (data - data.mean()) / data.std()

    def plot_clustered_heatmap(self) -> None:
        """Generates and saves a clustered heatmap from the DataFrame."""
        # Pivot the data to have queries as columns and indices as rows
        data_pivot = self.data.pivot(index='Indices', columns='variable', values='value')

        # Normalize the data
        data_normalized = self.normalize_data(data_pivot)

        # Compute the distance matrix and hierarchical clustering
        dist_matrix = pdist(data_normalized.T, metric='euclidean')
        linkage_matrix = linkage(dist_matrix, method='ward')

        # Plot clustered heatmap
        plt.figure(figsize=(20, 15))
        sns.clustermap(data_normalized, row_cluster=False, col_linkage=linkage_matrix, cmap='viridis')
        plt.savefig(f"{self.figure_path}clustered_heatmap.png")
        plt.close()

    def plot_boxplot(self) -> None:
        """Generates and saves a boxplot for query execution times with mean times in legend."""
        # Compute the mean execution time across all queries for each index
        self.data['MeanQueryTime'] = self.data[['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']].mean(axis=1)

        plt.figure(figsize=(15, 10))
        boxplot = sns.boxplot(x='Indices', y='MeanQueryTime', data=self.data)
        plt.ylabel("Mean Execution Time (ms)")
        plt.xlabel("Indices")
        plt.title("Distribution of Mean Query Execution Times by Indices")

        plt.savefig(f"{self.figure_path}boxplot.png")
        plt.close()

    def plot_clustered_heatmap_by_index(self) -> None:
        """Generates and saves a clustered heatmap for each index."""
        indices = self.data['Indices'].unique()
        for index in indices:
            data_subset = self.data[self.data['Indices'] == index]
            melted_data = pd.melt(data_subset, id_vars=['variable'], value_vars=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])
            data_pivot = melted_data.pivot(index='variable', columns='Indices', values='value')
            data_normalized = self.normalize_data(data_pivot)
            dist_matrix = pdist(data_normalized.T, metric='euclidean')
            linkage_matrix = linkage(dist_matrix, method='ward')

            plt.figure(figsize=(20, 15))
            sns.clustermap(data_normalized, row_cluster=False, col_linkage=linkage_matrix, cmap='viridis')
            plt.title(f"Clustered Heatmap for Index: {index}")
            plt.savefig(f"{self.figure_path}clustered_heatmap_{index}.png")
            plt.close()

    def plot_optimal_indices_heatmap(self, n: int = 10) -> None:
        """Finds and plots the optimal indices heatmap."""
        optimal_indices = {f'Q{i+1}': [] for i in range(7)}

        for i, query in enumerate(['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']):
            top_n_rows = self.data.nsmallest(n, query)
            for indices in top_n_rows['Indices']:
                optimal_indices[f'Q{i+1}'].extend(indices.split('+'))

        # Create a DataFrame for the heatmap
        index_set = sorted(set(idx for indices in optimal_indices.values() for idx in indices))
        heatmap_data = pd.DataFrame(0, index=index_set, columns=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])

        for query, indices in optimal_indices.items():
            for idx in indices:
                heatmap_data.at[idx, query] += 1

        heatmap_data['Total'] = heatmap_data.sum(axis=1)

        plt.figure(figsize=(15, 10))
        sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu')
        plt.title(f"Optimal Indices (Top {n} Executions per Query)")
        plt.savefig(f"{self.figure_path}optimal_indices_heatmap_top_{n}.png")
        plt.close()

    def plot_optimal_indices_heatmap_and_freq(self, n: int = 10) -> None:
        """Finds and plots the optimal indices heatmap and their frequencies."""
        optimal_indices = {f'Q{i+1}': [] for i in range(7)}

        for i, query in enumerate(['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']):
            top_n_rows = self.data.nsmallest(n, query)
            for indices in top_n_rows['Indices']:
                optimal_indices[f'Q{i+1}'].extend(indices.split('+'))

        # Create a DataFrame for the heatmap
        index_set = sorted(set(idx for indices in optimal_indices.values() for idx in indices))
        heatmap_data = pd.DataFrame(0, index=index_set, columns=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])

        for query, indices in optimal_indices.items():
            for idx in indices:
                heatmap_data.at[idx, query] += 1

        # Calculate the total frequency for the bar plot
        total_counts = heatmap_data.sum(axis=1)

        # Plotting
        fig, axes = plt.subplots(ncols=2, figsize=(20, 10), gridspec_kw={'width_ratios': [2, 1]})

        # Heatmap
        sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu', cbar=False, ax=axes[0])
        axes[0].set_title(f"Optimal Indexes (Top {n} Executions per Query)")

        # Bar plot
        sns.barplot(x=total_counts.values, y=total_counts.index, palette='viridis', ax=axes[1])
        axes[1].set_xlabel('Total Frequency')
        axes[1].set_ylabel('Indexes')
        axes[1].set_title('Total Frequency of Indices')

        plt.tight_layout()
        plt.savefig(f"{self.figure_path}optimal_indices_heatmap_top_{n}.png")
        plt.close()

if __name__ == "__main__":
    csv_path = "query_optimization/mongodb/encoded_performance_mongodb.csv"
    figure_path = "./query_optimization/mongodb/figures/"

    visualizer = MongoDBVisualizer(csv_path, figure_path)
    visualizer.plot_clustered_heatmap()
    visualizer.plot_boxplot()
    visualizer.plot_clustered_heatmap_by_index()
    visualizer.plot_optimal_indices_heatmap()
    visualizer.plot_optimal_indices_heatmap_and_freq()
    print(f"Visualizations saved to {figure_path}")
