import pandas as pd

# Función para codificar los índices
def encode_indices(indices_str, indices_list):
    # Extrae los números de índice de la cadena
    indices = indices_str.strip('()').replace("'", "").split(', ')
    
    # Mapea cada índice a su correspondiente notación "I"
    encoded_indices = []
    for index in indices:
        if index:
            index_num = indices_list.index(index) + 1
            encoded_indices.append(f"I{index_num}")
    
    # Une los índices codificados con un signo "+"
    return "+".join(encoded_indices)

def main(input_file, output_file):
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
    
    # Aplicar la función para codificar índices en cada fila
    df['encoded_indices'] = df['indices'].apply(lambda x: encode_indices(x, indices_list))
    
    # Guardar el dataframe actualizado en un nuevo archivo CSV
    df.to_csv(output_file, index=False)

# Especificar los archivos de entrada y salida
input_file = './query_optimization/mysql_optimization_results.csv'
output_file = './query_optimization/mysql_optimization_results_encoded.csv'

# Ejecutar la función principal
if __name__ == "__main__":
    main(input_file, output_file)
