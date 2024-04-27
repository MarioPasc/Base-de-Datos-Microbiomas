from lxml import etree
import mysql.connector as mysql 
def connection(password, database):
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
    connection = connection(password, database)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patient")
    pacientes = cursor.fetchall()
    
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
        cursor.execute(f"SELECT * FROM muestras WHERE paciente_id = {paciente['ID']}")
        muestras = cursor.fetchall()
        samples_elem = etree.SubElement(patient_elem, 'sample_list')
        
        for muestra in muestras:
            sample_elem = etree.SubElement(samples_elem, 'sample')
            etree.SubElement(sample_elem, 'ID').text = str(muestra['ID'])
            etree.SubElement(sample_elem, 'DATE').text = muestra['DATE']
            etree.SubElement(sample_elem, 'BODY_PART').text = muestra['BODY_PART']
            etree.SubElement(sample_elem, 'SAMPLE_TYPE').text = muestra['SAMPLE_TYPE']

            # Asumimos que hay una tabla para microorganismos en cada muestra
            cursor.execute(f"SELECT * FROM microorganism WHERE muestra_id = {muestra['ID']}")
            micros = cursor.fetchall()
            micros_elem = etree.SubElement(sample_elem, 'lista_microorganismo')
            
            for micro in micros:
                micro_elem = etree.SubElement(micros_elem, 'microorganismo')
                etree.SubElement(micro_elem, 'ID').text = str(micro['ID'])
                etree.SubElement(micro_elem, 'SPECIES').text = micro['SPECIES']
                etree.SubElement(micro_elem, 'KINGDOM').text = micro['KINGDOM']
                etree.SubElement(micro_elem, 'FASTA').text = micro['FASTA']
                etree.SubElement(micro_elem, 'SEQ').text = micro['SEQ']
                etree.SubElement(micro_elem, 'QPCR').text = micro['QPCR']

    # Convertir el árbol XML en una cadena y guardarla en un archivo
    tree = etree.ElementTree(root)
    tree.write('microbiome.xml', pretty_print=True, xml_declaration=True, encoding='UTF-8')



generate_xml(password="bdbiO", database="microbiome_db")
