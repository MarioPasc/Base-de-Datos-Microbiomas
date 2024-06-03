import pandas as pd
import matplotlib.pyplot as plt

# Datos del archivo CSV proporcionado
data = {
    'Query': ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7'],
    'Time (ms)': [18.079280853271484, 1.4070367813110352, 1.5658807754516602, 
                  2.0580291748046875, 2.0020008087158203, 5.9986114501953125, 
                  4.001379013061523]
}

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
