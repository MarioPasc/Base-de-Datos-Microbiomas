import pandas as pd

def create_index_mapping(indices_list):
    """Crea un mapeo de índices a identificadores únicos."""
    return {index: f"I{idx+1}" for idx, index in enumerate(indices_list)}

def encode_indices(indices, index_mapping):
    """Codifica una lista de índices utilizando el mapeo proporcionado."""
    if pd.isna(indices) or indices == "[]":
        return ""
    encoded_indices = [index_mapping[index.strip()] for index in indices.strip("()").split(',') if index.strip()]
    return "+".join(encoded_indices)

def encode_indices_in_dataframe(df, index_mapping):
    """Codifica las combinaciones de índices en un DataFrame."""
    df['encoded_indices'] = df['indices'].apply(lambda x: encode_indices(x, index_mapping))
    return df

if __name__ == "__main__":
    # Cargar los datos del archivo CSV
    input_file = '/mnt/data/mysql_optimization_results.csv'
    output_file = '/mnt/data/mysql_optimization_results_encoded.csv'
    df = pd.read_csv(input_file)

    # Obtener la lista de índices únicos del DataFrame original
    indices_list = sorted(set(index for indices in df['indices'].dropna() for index in indices.strip("()").split(',')))
    
    # Crear el mapeo de índices a identificadores únicos
    index_mapping = create_index_mapping(indices_list)
    
    # Codificar las combinaciones de índices en el DataFrame
    df_encoded = encode_indices_in_dataframe(df, index_mapping)
    
    # Guardar el DataFrame resultante en un nuevo archivo CSV
    df_encoded.to_csv(output_file, index=False)
    
    print("Codificación completada y datos guardados en:", output_file)
