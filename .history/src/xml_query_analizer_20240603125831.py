import pandas as pd
import matplotlib.pyplot as plt

# Datos del archivo CSV proporcionado
data = pd.read_csv(".\query_optimization\XML\ExecutionTimeXml.csv")

# Crear un DataFrame con los datos
df = pd.DataFrame(data)

# Generación del gráfico de barras
plt.figure(figsize=(10, 6))
plt.bar(df['Query'], df['Time (ms)'], color='skyblue')
plt.title('Execution Time of Each Query')
plt.xlabel('Query')
plt.ylabel('Time (ms)')
plt.grid(True)
plt.show()
