from lxml import etree
import mysql.connector as mysql 
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
    print(pacientes)
    print(type(pacientes))
    for paciente in pacientes:
        # Crear elemento por cada paciente
        patient_elem = etree.SubElement(root, 'patient')
        etree.SubElement(patient_elem, 'Patient_ID').text = str(paciente['Patient_ID'])
        etree.SubElement(patient_elem, 'Age').text = str(paciente['Age'])
        etree.SubElement(patient_elem, 'Birth_Type').text = paciente['Birth_Type']
        etree.SubElement(patient_elem, 'Location').text = paciente['Location']
        etree.SubElement(patient_elem, 'Lifestyle').text = paciente['Lifestyle']
        etree.SubElement(patient_elem, 'Disease').text = paciente['Disease']
        etree.SubElement(patient_elem, 'Sex').text = paciente['Sex']
        
        # Asumimos que hay otra tabla para muestras relacionadas con cada paciente
        cursor.execute(f"SELECT * FROM sample WHERE paciente_id = {paciente['Patient_ID']}")
        muestras = cursor.fetchall()
        samples_elem = etree.SubElement(patient_elem, 'sample_list')
        
        for muestra in muestras:
            sample_elem = etree.SubElement(samples_elem, 'sample')
            etree.SubElement(sample_elem, 'Sample_ID').text = str(muestra['Sample_ID'])
            etree.SubElement(sample_elem, 'Date').text = muestra['Date']
            etree.SubElement(sample_elem, 'Body_Part').text = muestra['Body_Part']
            etree.SubElement(sample_elem, 'Sample_Type').text = muestra['Sample_Type']

            # Asumimos que hay una tabla para microorganismos en cada muestra
            cursor.execute(f"SELECT m.Microorganism_ID, m.FASTA, m.Kingdom, m.Species, m.Seq_length, sm.qPCR FROM microorganism m, sample_microorganism sm WHERE sm.Microorganism_ID=m.Microorganism_ID  AND sm.Sample_ID = {muestra['ID']}")
            micros = cursor.fetchall()
            micros_elem = etree.SubElement(sample_elem, 'lista_microorganismo')
            
            for micro in micros:
                micro_elem = etree.SubElement(micros_elem, 'microorganism')
                etree.SubElement(micro_elem, 'Microorganism_ID').text = str(micro['Microorganism_ID'])
                etree.SubElement(micro_elem, 'Species').text = micro['Species']
                etree.SubElement(micro_elem, 'Kingdom').text = micro['Kingdom']
                etree.SubElement(micro_elem, 'FASTA').text = micro['FASTA']
                etree.SubElement(micro_elem, 'Seq_length').text = micro['Seq_length']
                etree.SubElement(micro_elem, 'qPCR').text = micro['qPCR']

    # Convertir el árbol XML en una cadena y guardarla en un archivo
    tree = etree.ElementTree(root)
    tree.write('microbiome.xml', pretty_print=True, xml_declaration=True, encoding='UTF-8')



generate_xml(password="bdbiO", database="microbiome_db")
