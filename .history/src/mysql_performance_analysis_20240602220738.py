import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# Función para codificar los índices
def encode_indices(indices_str, indices_list):
    # Extrae los números de índice de la cadena
    indices = indices_str.strip('()').replace("'", "").split(', ')
    
    # Mapea cada índice a su correspondiente notación "I"
    encoded_indices = []
    for index in indices:
        index = index.strip()
        if index and index in indices_list:
            index_num = indices_list.index(index) + 1
            encoded_indices.append(f"I{index_num}")
    
    # Une los índices codificados con un signo "+"
    return "+".join(encoded_indices)

# Función para codificar las queries
def encode_queries(queries_str, queries_list):
    # Extrae las queries de la cadena
    query_indices = [queries_list.index(query) + 1 for query in queries_str]
    
    # Mapea cada query a su correspondiente notación "Q"
    encoded_queries = [f"Q{query_index}" for query_index in query_indices]
    
    # Une las queries codificadas con un signo "+"
    return "+".join(encoded_queries)

def encode_csv(input_file, output_file):
    # Cargar el archivo CSV
    df = pd.read_csv(input_file)
    
    # Lista de índices de referencia
    indices_list = [
        "CREATE INDEX idx_patient_id ON patient(Patient_ID);",
        "CREATE INDEX idx_sample_patient_id ON sample(Patient_ID);",
        "CREATE INDEX idx_sample_id ON sample(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_sample_id ON sample_microorganism(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_microorganism_id ON sample_microorganism(Microorganism_ID);",
        "CREATE INDEX idx_sample_microorganism_qpcr ON sample_microorganism(qPCR);",
        "CREATE INDEX idx_patient_disease ON patient(Disease(255));",
        "CREATE INDEX idx_sample_date ON sample(Date);",
        "CREATE INDEX idx_sample_type ON sample(Sample_Type);",
        "CREATE INDEX idx_microorganism_species ON microorganism(Species(255));"
    ]
    
    # Lista de queries de referencia
    queries_list = [
        "SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms FROM patient p, sample s, sample_microorganism sm WHERE p.Patient_ID= s.Patient_ID AND s.Sample_ID= sm.Sample_ID GROUP BY p.Patient_ID ORDER BY Num_Microorganisms DESC LIMIT 10;",
        "SELECT sm.Sample_ID, sm.qPCR FROM sample_microorganism sm WHERE sm.Microorganism_ID = %s ORDER BY sm.qPCR DESC;",
        "SELECT p.Patient_ID, s.Sample_ID, s.Date, s.Body_Part, s.Sample_Type FROM patient p, sample s WHERE p.Patient_ID = s.Patient_ID AND p.Disease = %s ORDER BY s.Date;",
        "SELECT s.Sample_Type, COUNT(*) AS Sample_Count FROM sample s GROUP BY s.Sample_Type ORDER BY Sample_Count DESC;",
        "SELECT sm.Microorganism_ID, s.Sample_Type, COUNT(sm.Sample_ID) AS Sample_Count, AVG(sm.qPCR), STDDEV(sm.qPCR) FROM sample s, sample_microorganism sm WHERE s.Sample_ID= sm.Sample_ID GROUP BY s.Sample_Type, sm.Microorganism_ID ORDER BY sm.Microorganism_ID DESC;",
        "SELECT p1.Patient_ID, s1.Max_qPCR FROM (SELECT p.Patient_ID FROM patient p WHERE p.disease='Hepatitis B') p1, (SELECT s.Patient_ID, max(sm.qPCR) as Max_qPCR FROM sample s, microorganism m, sample_microorganism sm WHERE s.Sample_ID=sm.Sample_ID AND sm.Microorganism_ID=m.Microorganism_ID AND m.Species='Hepatitis B Virus' GROUP BY s.Patient_ID) s1 WHERE s1.Patient_ID=p1.Patient_ID;",
        "SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength FROM microorganism GROUP BY Species HAVING Count>1;",
        "SELECT * FROM patient;",
        "SELECT * FROM sample;",
        "SELECT * FROM sample_microorganism;"
    ]
    
    # Aplicar la función para codificar índices en cada fila
    df['indices'] = df['indices'].apply(lambda x: encode_indices(x, indices_list))
    
    # Aplicar la función para codificar queries en cada fila
    if 'queries' in df.columns:
        df['queries'] = df['queries'].apply(lambda x: encode_queries(x, queries_list))
    
    # Guardar el dataframe actualizado en un nuevo archivo CSV
    df.to_csv(output_file, index=False)

def create_and_save_boxplots(df, output_dir):
    # Crear el directorio de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Convertir la columna 'times' de string a lista de floats si no está ya convertida
    if isinstance(df['times'].iloc[0], str):
        df['times'] = df['times'].apply(eval)

    # Descomponer la columna 'times' en filas separadas para cada consulta
    df_exploded = df.explode('times').reset_index(drop=True)

    # Añadir un identificador de consulta basado en la posición en el vector 'times'
    df_exploded['query'] = df_exploded.groupby('engine').cumcount() % 10 + 1
    df_exploded['query'] = 'Q' + df_exploded['query'].astype(str)

    # Obtener la lista de queries únicas (máximo de 10)
    queries = df_exploded['query'].unique()[:10]

    for query in queries:
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='engine', y='times', data=df_exploded[df_exploded['query'] == query])
        plt.title(f'Execution Time for {query} by Engine')
        plt.xlabel('Engine')
        plt.ylabel('Execution Time (s)')
        plt.savefig(os.path.join(output_dir, f'{query}_execution_time_by_engine.png'))
        plt.close()    # Crear el directorio de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Convertir la columna 'times' de string a lista de floats
    df['times'] = df['times'].apply(eval)

    # Descomponer la columna 'times' en filas separadas para cada consulta
    df_exploded = df.explode('times').reset_index(drop=True)

    # Añadir un identificador de consulta
    df_exploded['query'] = df_exploded.groupby(['engine', 'indices']).cumcount() + 1
    df_exploded['query'] = 'Q' + df_exploded['query'].astype(str)

    # Obtener la lista de queries únicas
    queries = df_exploded['query'].unique()

    for query in queries:
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='engine', y='times', data=df_exploded[df_exploded['query'] == query])
        plt.title(f'Execution Time for {query} by Engine')
        plt.xlabel('Engine')
        plt.ylabel('Execution Time (s)')
        plt.savefig(os.path.join(output_dir, f'{query}_execution_time_by_engine.png'))
        plt.close()

def calculate_mean_times(df_exploded, unique_queries, unique_indices):
    mean_times = pd.DataFrame(index=unique_queries, columns=unique_indices, dtype=float)

    for query in unique_queries:
        for index in unique_indices:
            # Filtrar las filas donde el índice está presente (ya sea solo o en combinación)
            relevant_rows = df_exploded[df_exploded['indices'].str.contains(index)]
            # Filtrar las filas donde la query es la actual
            relevant_rows = relevant_rows[relevant_rows['query'] == query]
            # Calcular el tiempo medio
            mean_time = relevant_rows['times'].mean()
            mean_times.loc[query, index] = mean_time
    
    return mean_times

def create_and_save_heatmap(df, output_dir):
    # Crear el directorio de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Convertir la columna 'times' de string a lista de floats si no está ya convertida
    if isinstance(df['times'].iloc[0], str):
        df['times'] = df['times'].apply(eval)

    # Descomponer la columna 'times' en filas separadas para cada consulta
    df_exploded = df.explode('times').reset_index(drop=True)

    # Añadir un identificador de consulta basado en la posición en el vector 'times'
    df_exploded['query'] = df_exploded.groupby(['engine']).cumcount() % 10 + 1
    df_exploded['query'] = 'Q' + df_exploded['query'].astype(str)

    # Identificar índices no combinados y únicos
    unique_indices = sorted(set(i for sublist in df_exploded['indices'].str.split('+') for i in sublist))
    unique_indices = [index for index in unique_indices if not '+' in index]  # Filtrar los no combinados
    
    # Identificar queries únicas
    unique_queries = sorted(df_exploded['query'].unique())

    # Calcular los tiempos medios
    mean_times = calculate_mean_times(df_exploded, unique_queries, unique_indices)

    # Crear el mapa de calor
    plt.figure(figsize=(12, 8))
    sns.heatmap(mean_times, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title('Mean Execution Time for Queries by Non-Combined Indices')
    plt.xlabel('Index')
    plt.ylabel('Query')
    plt.savefig(os.path.join(output_dir, 'mean_execution_time_heatmap.png'))
    plt.close()

# Ejecutar la función principal
if __name__ == "__main__":
    # Especificar los archivos de entrada y salida
    input_file = './query_optimization/mysql/mysql_optimization_results.csv'
    output_file = './query_optimization/mysql/mysql_optimization_results_encoded.csv'
    # encode_csv(input_file, output_file)
    
    df = pd.read_csv(output_file)
    
    create_and_save_boxplots(
        df,
        "./query_optimization/mysql/boxplots/"
    )
