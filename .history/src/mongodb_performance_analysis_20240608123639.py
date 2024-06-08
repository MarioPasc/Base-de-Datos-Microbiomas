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
        # Pivot the data so that queries are columns and indices are rows
        data_pivot = self.data.set_index('Query').T
        data_normalized = self.normalize_data(data_pivot)

        # Compute the distance matrix and hierarchical clustering
        dist_matrix = pdist(data_normalized, metric='euclidean')
        linkage_matrix = linkage(dist_matrix, method='ward')

        # Plot clustered heatmap
        sns.clustermap(data_normalized, row_cluster=False, col_cluster=True, col_linkage=linkage_matrix, cmap='viridis')
        plt.title("Clustered Heatmap of Queries vs Indices")
        plt.savefig(f"{self.figure_path}clustered_heatmap.png")
        plt.close()

if __name__ == "__main__":
    csv_path = "/mnt/data/encoded_performance_mongodb.csv"
    figure_path = "./"

    visualizer = PerformanceMongoDB(csv_path, figure_path)
    visualizer.plot_clustered_heatmap()
    print(f"Plot saved to {figure_path}")

Vamos a ejecutar este código para generar el heatmap y verificar si funciona correctamente con los datos proporcionados. ​

​

python
Always show details

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
        # Pivot the data so that queries are columns and indices are rows
        data_pivot = self.data.set_index('Query').T
        data_normalized = self.normalize_data(data_pivot)

        # Compute the distance matrix and hierarchical clustering
        dist_matrix = pdist(data_normalized, metric='euclidean')
        linkage_matrix = linkage(dist_matrix, method='ward')

        # Plot clustered heatmap
        sns.clustermap(data_normalized, row_cluster=False, col_cluster=True, col_linkage=linkage_matrix, cmap='viridis')
        plt.title("Clustered Heatmap of Queries vs Indices")
        plt.savefig(f"{self.figure_path}clustered_heatmap.png")
        plt.close()

# Create the visualizer and generate the plot
figure_path = "./"
visualizer = PerformanceMongoDB(csv_path, figure_path)
visualizer.plot_clustered_heatmap()

---------------------------------------------------------------------------
IndexError                                Traceback (most recent call last)
Cell In[2], line 39
     37 figure_path = "./"
     38 visualizer = PerformanceMongoDB(csv_path, figure_path)
---> 39 visualizer.plot_clustered_heatmap()

Cell In[2], line 31, in PerformanceMongoDB.plot_clustered_heatmap(self)
     28 linkage_matrix = linkage(dist_matrix, method='ward')
     30 # Plot clustered heatmap
---> 31 sns.clustermap(data_normalized, row_cluster=False, col_cluster=True, col_linkage=linkage_matrix, cmap='viridis')
     32 plt.title("Clustered Heatmap of Queries vs Indices")
     33 plt.savefig(f"{self.figure_path}clustered_heatmap.png")

File ~/.local/lib/python3.11/site-packages/seaborn/_decorators.py:46, in _deprecate_positional_args.<locals>.inner_f(*args, **kwargs)
     36     warnings.warn(
     37         "Pass the following variable{} as {}keyword arg{}: {}. "
     38         "From version 0.12, the only valid positional argument "
   (...)
     43         FutureWarning
     44     )
     45 kwargs.update({k: arg for k, arg in zip(sig.parameters, args)})
---> 46 return f(**kwargs)

File ~/.local/lib/python3.11/site-packages/seaborn/matrix.py:1406, in clustermap(data, pivot_kws, method, metric, z_score, standard_scale, figsize, cbar_kws, row_cluster, col_cluster, row_linkage, col_linkage, row_colors, col_colors, mask, dendrogram_ratio, colors_ratio, cbar_pos, tree_kws, **kwargs)
   1248 """
   1249 Plot a matrix dataset as a hierarchically-clustered heatmap.
   1250 
   (...)
   1398     >>> g = sns.clustermap(iris, z_score=0, cmap="vlag")
   1399 """
   1400 plotter = ClusterGrid(data, pivot_kws=pivot_kws, figsize=figsize,
   1401                       row_colors=row_colors, col_colors=col_colors,
   1402                       z_score=z_score, standard_scale=standard_scale,
   1403                       mask=mask, dendrogram_ratio=dendrogram_ratio,
   1404                       colors_ratio=colors_ratio, cbar_pos=cbar_pos)
-> 1406 return plotter.plot(metric=metric, method=method,
   1407                     colorbar_kws=cbar_kws,
   1408                     row_cluster=row_cluster, col_cluster=col_cluster,
   1409                     row_linkage=row_linkage, col_linkage=col_linkage,
   1410                     tree_kws=tree_kws, **kwargs)

File ~/.local/lib/python3.11/site-packages/seaborn/matrix.py:1232, in ClusterGrid.plot(self, metric, method, colorbar_kws, row_cluster, col_cluster, row_linkage, col_linkage, tree_kws, **kws)
   1229     yind = np.arange(self.data2d.shape[0])
   1231 self.plot_colors(xind, yind, **kws)
-> 1232 self.plot_matrix(colorbar_kws, xind, yind, **kws)
   1233 return self

File ~/.local/lib/python3.11/site-packages/seaborn/matrix.py:1151, in ClusterGrid.plot_matrix(self, colorbar_kws, xind, yind, **kws)
   1150 def plot_matrix(self, colorbar_kws, xind, yind, **kws):
-> 1151     self.data2d = self.data2d.iloc[yind, xind]
   1152     self.mask = self.mask.iloc[yind, xind]
   1154     # Try to reorganize specified tick labels, if provided

File ~/.local/lib/python3.11/site-packages/pandas/core/indexing.py:1067, in _LocationIndexer.__getitem__(self, key)
   1065     if self._is_scalar_access(key):
   1066         return self.obj._get_value(*key, takeable=self._takeable)
-> 1067     return self._getitem_tuple(key)
   1068 else:
   1069     # we by definition only have the 0th axis
   1070     axis = self.axis or 0

File ~/.local/lib/python3.11/site-packages/pandas/core/indexing.py:1563, in _iLocIndexer._getitem_tuple(self, tup)
   1561 def _getitem_tuple(self, tup: tuple):
-> 1563     tup = self._validate_tuple_indexer(tup)
   1564     with suppress(IndexingError):
   1565         return self._getitem_lowerdim(tup)

File ~/.local/lib/python3.11/site-packages/pandas/core/indexing.py:873, in _LocationIndexer._validate_tuple_indexer(self, key)
    871 for i, k in enumerate(key):
    872     try:
--> 873         self._validate_key(k, i)
    874     except ValueError as err:
    875         raise ValueError(
    876             "Location based indexing can only have "
    877             f"[{self._valid_types}] types"
    878         ) from err

File ~/.local/lib/python3.11/site-packages/pandas/core/indexing.py:1481, in _iLocIndexer._validate_key(self, key, axis)
   1479     # check that the key does not exceed the maximum size of the index
   1480     if len(arr) and (arr.max() >= len_axis or arr.min() < -len_axis):
-> 1481         raise IndexError("positional indexers are out-of-bounds")
   1482 else:
   1483     raise ValueError(f"Can only index by location with a [{self._valid_types}]")

IndexError: positional indexers are out-of-bounds

El error IndexError: positional indexers are out-of-bounds sugiere que hay un problema con los índices que se están utilizando al intentar crear el clustermap. Esto podría estar relacionado con la normalización de los datos o cómo se están pasando los índices al clustermap.

Para abordar este problema, vamos a:

    Asegurarnos de que los datos se normalicen correctamente.
    Verificar los índices utilizados para crear el clustermap.

A continuación, modificaremos la función para incluir pasos de verificación adicionales y manejo de errores. Aquí tienes el código actualizado:

python

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

    def plot_clustered_heatmap(self) -> None



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
