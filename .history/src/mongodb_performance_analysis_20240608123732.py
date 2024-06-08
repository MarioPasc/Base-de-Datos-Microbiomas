import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import pdist

class PerformanceMongoDB:
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
        try:
            # Pivot the data so that queries are columns and indices are rows
            data_pivot = self.data.set_index('Query').T
            data_normalized = self.normalize_data(data_pivot)

            # Verify the dimensions of the normalized data
            print(f"Data dimensions: {data_normalized.shape}")

            # Compute the distance matrix and hierarchical clustering
            dist_matrix = pdist(data_normalized, metric='euclidean')
            linkage_matrix = linkage(dist_matrix, method='ward')

            # Plot clustered heatmap
            sns.clustermap(data_normalized, row_cluster=False, col_cluster=True, col_linkage=linkage_matrix, cmap='viridis')
            plt.title("Clustered Heatmap of Queries vs Indices")
            plt.savefig(f"{self.figure_path}clustered_heatmap.png")
            plt.close()
        except Exception as e:
            print(f"An error occurred: {e}")

    def plot_heatmap_minimum_indices(self) -> None:
        """Generates and saves a heatmap for the indices with minimum query times."""
        min_indices = []
        for query in self.data['Query'].unique():
            min_time_row = self.data[self.data['Query'] == query].drop(columns=['Query']).min()
            min_index = min_time_row.idxmin()
            indices = min_index.split('+')
            min_indices.append(indices)

        index_set = sorted(set(idx for sublist in min_indices for idx in sublist))
        heatmap_data = pd.DataFrame(0, index=index_set, columns=self.data['Query'].unique())

        for i, indices in enumerate(min_indices):
            for idx in indices:
                heatmap_data.at[idx, self.data['Query'].unique()[i]] = 1

        plt.figure(figsize=(15, 10))
        sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu')
        plt.title("Indices with Minimum Query Times")
        plt.savefig(f"{self.figure_path}heatmap_minimum_indices.png")
        plt.close()

    def plot_optimal_indices_heatmap(self, n: int = 10) -> None:
        """Finds and plots the optimal indices heatmap for MongoDB."""
        optimal_indices = {query: [] for query in self.data['Query'].unique()}

        for query in self.data['Query'].unique():
            top_n_rows = self.data[self.data['Query'] == query].nsmallest(n, query)
            for idx in top_n_rows.index:
                indices = self.data.loc[idx, 'Query'].split('+')
                optimal_indices[query].extend(indices)

        index_set = sorted(set(idx for indices in optimal_indices.values() for idx in indices))
        heatmap_data = pd.DataFrame(0, index=index_set, columns=self.data['Query'].unique())

        for query, indices in optimal_indices.items():
            for idx in indices:
                heatmap_data.at[idx, query] += 1

        total_counts = heatmap_data.sum(axis=1)

        fig, axes = plt.subplots(ncols=2, figsize=(20, 10), gridspec_kw={'width_ratios': [4, 1]})

        sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu', cbar=False, ax=axes[0])
        axes[0].set_title(f"Optimal Indices for MongoDB (Top {n} Executions per Query)")

        barplot = sns.barplot(x=total_counts.values, y=total_counts.index, palette='viridis', ax=axes[1])
        axes[1].set_xlabel('Total Frequency')
        axes[1].set_ylabel('Index')
        axes[1].set_title('Total Frequency of Indices')

        plt.tight_layout()
        plt.savefig(f"{self.figure_path}optimal_indices_heatmap_mongodb_top_{n}.png")
        plt.close()

if __name__ == "__main__":
    csv_path = "./query_optimization/mongodb/encoded_performance_mongodb.csv"
    figure_path = "./query_optimization/mongodb/figures/"

    visualizer = PerformanceMongoDB(csv_path, figure_path)
    visualizer.plot_clustered_heatmap()
    visualizer.plot_heatmap_minimum_indices()
    visualizer.plot_optimal_indices_heatmap(n=10)
    print(f"Plots saved to {figure_path}")
