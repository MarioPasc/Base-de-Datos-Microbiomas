import matplotlib.pyplot as plt
import seaborn as sns
import os
import pandas as pd

class OverallAnalysis:
    
    def __init__(self, xml_path:str, 
                 mongo_path:str, 
                 sql_path:str,
                 best_mongo_path:str,
                 best_sql_path: str) -> None:
        self.xml_path = xml_path
        self.mongo_path = mongo_path
        self.sql_path = sql_path
        self.best_mongo_path = best_mongo_path
        self.best_sql_path = best_sql_path
        
    def generate_clustered_heatmap(csv_path, output_path):
        # Load the CSV file
        data = pd.read_csv(csv_path)

        # Convert the 'times' column from string to list of floats
        data['times'] = data['times'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        # Filter out rows with any zero times and remove the last 3 queries from the 'times' data
        filtered_data = data[data['times'].apply(lambda x: all(value != 0.0 for value in x))]
        filtered_data['times'] = filtered_data['times'].apply(lambda x: x[:7])

        # Separate data by engine type
        engines = filtered_data['engine'].unique()
        clustered_heatmaps = {}

        for engine in engines:
            engine_data = filtered_data[filtered_data['engine'] == engine]
            times_data = np.array(engine_data['times'].tolist())
            
            # Create and store the clustered heatmap for the current engine
            cluster_grid = sns.clustermap(times_data, cmap="viridis", figsize=(10, 8), yticklabels=False)
            cluster_grid.ax_heatmap.set_xticklabels(range(1, 8))
            cluster_grid.fig.suptitle(f'Clustered Heatmap of Execution Times for {engine}', y=1.02)
            clustered_heatmaps[engine] = cluster_grid
            
            # Save the figure
            output_file = f"{output_path}/clustered_heatmap_{engine}.png"
            cluster_grid.fig.savefig(output_file)

        # Show the plots
        for engine, grid in clustered_heatmaps.items():
            plt.figure(grid.fig.number)
            plt.show()
        
    
        