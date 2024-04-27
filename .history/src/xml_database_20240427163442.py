from lxml import etree
import mysql.connector as mysql 
import datetime
def connect(password, database):
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=password,
                    database=database)
            return  connection
        except mysql.Error as err:
            print(f"Error: {err}")

# Función para generar el XML
def generate_xml(password, database):
    """
    Genera un archivo XML a partir de los datos obtenidos de la base de datos MySQL.
    """
    # Crea el elemento raíz
    root = etree.Element('microbiome')
    
    # Ejecutar consulta SQL para obtener los datos de los pacientes
    connection = connect(password, database)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patient")
    pacientes = cursor.fetchall()

    for paciente in pacientes:
        # Crear elemento por cada paciente
        patient_elem = etree.SubElement(root, 'patient')
        etree.SubElement(patient_elem, 'Patient_ID').text = str(paciente[0])
        etree.SubElement(patient_elem, 'Age').text = str(paciente[1])
        etree.SubElement(patient_elem, 'Birth_Type').text = paciente[2]
        etree.SubElement(patient_elem, 'Location').text = paciente[3]
        etree.SubElement(patient_elem, 'Lifestyle').text = paciente[4]
        etree.SubElement(patient_elem, 'Disease').text = paciente[5]
        etree.SubElement(patient_elem, 'Sex').text = paciente[6]
        
        # Asumimos que hay otra tabla para muestras relacionadas con cada paciente
        cursor.execute(f"SELECT * FROM sample WHERE Patient_ID = '{paciente[0]}'")
        muestras = cursor.fetchall()
        samples_elem = etree.SubElement(patient_elem, 'sample_list')
        
        for muestra in muestras:
            sample_elem = etree.SubElement(samples_elem, 'sample')
            etree.SubElement(sample_elem, 'Sample_ID').text = str(muestra[0])
            # Skip [1] for patient id
            fecha = muestra[2]
            etree.SubElement(sample_elem, 'Date').text = fecha.strftime("%Y-%m-%d")
            etree.SubElement(sample_elem, 'Body_Part').text = muestra[3]
            etree.SubElement(sample_elem, 'Sample_Type').text = muestra[4]

            # Asumimos que hay una tabla para microorganismos en cada muestra
            cursor.execute(f"SELECT m.Microorganism_ID, m.FASTA, m.Kingdom, m.Species, m.Seq_length, sm.qPCR FROM microorganism m, sample_microorganism sm WHERE sm.Microorganism_ID=m.Microorganism_ID  AND sm.Sample_ID = '{muestra[0]}'")
            micros = cursor.fetchall()
            micros_elem = etree.SubElement(sample_elem, 'lista_microorganismo')
            
            for micro in micros:
                print(micro)
                micro_elem = etree.SubElement(micros_elem, 'microorganism')
                etree.SubElement(micro_elem, 'Microorganism_ID').text = str(micro[0])
                etree.SubElement(micro_elem, 'Species').text = micro[3]
                etree.SubElement(micro_elem, 'Kingdom').text = micro[2]
                etree.SubElement(micro_elem, 'FASTA').text = micro[1]
                etree.SubElement(micro_elem, 'Seq_length').text = micro[4]
                etree.SubElement(micro_elem, 'qPCR').text = micro[5]

    # Convertir el árbol XML en una cadena y guardarla en un archivo
    tree = etree.ElementTree(root)
    tree.write('microbiome.xml', pretty_print=True, xml_declaration=True, encoding='UTF-8')



generate_xml(password="bdbiO", database="microbiome_db")
