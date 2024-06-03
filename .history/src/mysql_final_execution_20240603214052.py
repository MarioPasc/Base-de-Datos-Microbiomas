from mysql_queries import Queries
import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt

# Función para añadir una nueva fila al DataFrame
def add_row(df, query, time):
    new_row = pd.DataFrame({'Query': [query], 'Time': [time]})
    df = pd.concat([df, new_row], ignore_index=True)
    return df

def tiempo_query(n, query, n_query, df):
    tiempos = []
    for _ in range(n):
        t = query 
        tiempos += [t * 1000]  # Convertir a ms
    return add_row(df, f"Q{n_query}", np.mean(tiempos))

def main(n=50):
    password = "bdbiO"
    bd = "microbiomeDB"
    
    connection = Queries(password, bd)

    # Crear un DataFrame vacío con columnas 'Query' y 'Time'
    df = pd.DataFrame(columns=['Query', 'Time'])

    df = tiempo_query(n, connection.query1(), 1, df)

    microorganismos = ["MIC-17098-ZUZ", "MIC-21174-VEX", "MIC-21714-ZMU", "MIC-21822-ASC"]
    tiempos = []
    for _ in range(n):
        microorganismo = random.choice(microorganismos)
        t = connection.query2(microorganismo)
        tiempos += [t * 1000]  # Convertir a ms
    df = add_row(df, "Q2", np.mean(tiempos))

    diseases = ["Herpes simplex", "Tuberculosis", "Respiratory infections", "Tetanus"]
    tiempos = []
    for _ in range(n):
        disease = random.choice(diseases)
        t = connection.query3(disease)    
        tiempos += [t * 1000]  # Convertir a ms
    df = add_row(df, "Q3", np.mean(tiempos))

    df = tiempo_query(n, connection.query4(), 4, df)
    df = tiempo_query(n, connection.query5(), 5, df)
    df = tiempo_query(n, connection.query6(), 6, df)
    df = tiempo_query(n, connection.query7(), 7, df)

    return df

if __name__ == "__main__":
    df = main()
    df.to_csv('./query_optimization/mysql/mysql_final_results.csv', index=False)
    
    # Generación del gráfico de barras
    plt.figure(figsize=(10, 6))
    plt.bar(df["Query"], df["Time"]/1000, color='skyblue')
    plt.title('Execution Time of Each Query')
    plt.xlabel('Query')
    plt.ylabel('Time (s)')
    plt.grid(True)
    plt.savefig("query_optimization/mysql/query_times_mysql.png")
    plt.show()
