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
        data_pivot.columns = ['_'.join(col).strip() for col in data_pivot.columns.values]

        # Normalize the data
        data_normalized = self.normalize_data(data_pivot)

        # Compute the distance matrix and hierarchical clustering
        dist_matrix = pdist(data_normalized.T, metric='euclidean')
        linkage_matrix = linkage(dist_matrix, method='ward')

        # Plot clustered heatmap
        plt.figure(figsize=(20, 15))
        sns.clustermap(data_normalized, row_cluster=False, col_linkage=linkage_matrix, cmap='viridis')
        plt.savefig(self.figure_path)
        plt.close()

if __name__ == "__main__":
    csv_path = "/mnt/data/mysql_encoded_optimization_results.csv"
    figure_path = "./query_optimization/mysql/figures/clustered_heatmap.png"

    visualizer = CSVVisualizer(csv_path, figure_path)
    visualizer.plot_clustered_heatmap()
    print(f"Clustered Heatmap saved to {figure_path}")
