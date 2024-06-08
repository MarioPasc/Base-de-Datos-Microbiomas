import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.cluster.hierarchy import linkage, dendrogram
from scipy.spatial.distance import pdist
from typing import Any

class CSVVisualizer:
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
        # Pivot the data to have queries as columns and engines/indices as rows
        data_pivot = self.data.pivot(index='Engine', columns='Indices', values=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])

        # Flatten the multi-level column index for easier processing
        data_pivot.columns = ['_'.join(map(str, col)).strip() for col in data_pivot.columns.values]

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

    def plot_violin_plot(self) -> None:
        """Generates and saves a violin plot for query execution times by engine."""
        plt.figure(figsize=(15, 10))
        melted_data = pd.melt(self.data, id_vars=['Engine'], value_vars=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])
        sns.violinplot(x='Engine', y='value', hue='variable', data=melted_data, split=True, inner="quart", palette="muted")
        plt.ylabel("Execution Time (ms)")
        plt.xlabel("Storage Engine")
        plt.title("Distribution of Query Execution Times by Storage Engine")
        plt.legend(title="Queries")
        plt.savefig(f"{self.figure_path}violin_plot.png")
        plt.close()

    def plot_clustered_heatmap_by_engine(self) -> None:
        """Generates and saves a clustered heatmap for each engine."""
        engines = self.data['Engine'].unique()
        for engine in engines:
            data_subset = self.data[self.data['Engine'] == engine]
            data_pivot = data_subset.pivot(index='Indices', columns='variable', values='value')
            data_normalized = self.normalize_data(data_pivot)
            dist_matrix = pdist(data_normalized.T, metric='euclidean')
            linkage_matrix = linkage(dist_matrix, method='ward')

            plt.figure(figsize=(20, 15))
            sns.clustermap(data_normalized, row_cluster=False, col_linkage=linkage_matrix, cmap='viridis')
            plt.title(f"Clustered Heatmap for Engine: {engine}")
            plt.savefig(f"{self.figure_path}clustered_heatmap_{engine}.png")
            plt.close()

    def plot_innodb_optimal_indices_heatmap(self) -> None:
        """Finds and plots the optimal indices heatmap for InnoDB engine."""
        innodb_data = self.data[self.data['Engine'] == 'InnoDB']
        optimal_indices = []

        for query in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']:
            min_time_row = innodb_data.loc[innodb_data[query].idxmin()]
            indices = min_time_row['Indices'].split('+')
            optimal_indices.append(indices)

        # Create a DataFrame for the heatmap
        index_set = sorted(set([idx for sublist in optimal_indices for idx in sublist]))
        heatmap_data = pd.DataFrame(0, index=index_set, columns=['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'])

        for i, indices in enumerate(optimal_indices):
            for idx in indices:
                heatmap_data.at[idx, f'Q{i+1}'] = 1

        heatmap_data['Total'] = heatmap_data.sum(axis=1)

        plt.figure(figsize=(15, 10))
        sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu')
        plt.title("Optimal Indices for InnoDB Engine")
        plt.savefig(f"{self.figure_path}optimal_indices_heatmap_innodb.png")
        plt.close()

if __name__ == "__main__":
    csv_path = "/mnt/data/mysql_encoded_optimization_results.csv"
    figure_path = "./query_optimization/mysql/figures/"

    visualizer = CSVVisualizer(csv_path, figure_path)
    visualizer.plot_clustered_heatmap()
    visualizer.plot_violin_plot()
    visualizer.plot_clustered_heatmap_by_engine()
    visualizer.plot_innodb_optimal_indices_heatmap()
    print(f"Visualizations saved to {figure_path}")
