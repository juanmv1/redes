import pandas as pd
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import networkx as nx
import folium
import matplotlib.pyplot as plt
import sys
from data_imp_exp import download



PATH_DIN_IMPORTACIONES = os.path.join(os.path.dirname(__file__), 'data','DIN.xlsx')
DIN_SHEET_NAME = 'DIN'

PATH_DUS_EXPORTACIONES = os.path.join(os.path.dirname(__file__), 'data','DUS.xlsx')
DUS_SHEET_NAME = 'DUS'

DATA_DIR = os.path.join(os.path.dirname(__file__), 'downloads')

geolocator = Nominatim(user_agent="puertos_geo")

def listar_archivos_txt():
    """
    Lista todos los archivos .txt en el directorio de descargas.
    """
    archivos_txt = [f for f in os.listdir(DATA_DIR) if f.endswith('.txt')]
    return archivos_txt

def obtener_importaciones_desde_archivo(ano, mes, file_name):
    """
    Carga un archivo de importaciones .txt basado en el nombre del archivo.
    """
    din_data = get_din_columns()  # Obtener las columnas definidas para importaciones
    path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(path, sep=";", encoding="utf-8", names=din_data['CAMPO'], low_memory=True)
    return df

def obtener_exportaciones_desde_archivo(ano, mes, file_name):
    """
    Carga un archivo de exportaciones .txt basado en el nombre del archivo.
    """
    dus_data = get_dus_columns()  # Obtener las columnas definidas para exportaciones
    path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(path, sep=";", encoding="utf-8", names=dus_data['CAMPO'], low_memory=True)
    return df

def obtener_datos_archivo(file_name):
    """
    Obtiene los datos del archivo .txt (importaciones o exportaciones) basado en el nombre del archivo.
    Detecta automáticamente si el archivo es de importaciones o exportaciones y extrae mes y año.
    """
    # Determinar si es importación o exportación
    if 'Importaciones' in file_name:
        tipo = 'importaciones'
    elif 'Exportaciones' in file_name:
        tipo = 'exportaciones'
    else:
        raise ValueError(f"Tipo de archivo desconocido para {file_name}")
    
    # Intentar extraer el mes y el año del nombre del archivo
    partes = file_name.replace('.txt', '').split(' ')
    
    try:
        # Asumimos que el nombre está en formato: "Exportaciones mes año"
        mes = partes[1].lower()  # El mes está en la segunda parte
        ano = partes[2]  # El año está en la tercera parte
    except IndexError:
        # Si el formato del nombre no es el esperado, mostrar un mensaje y saltar el archivo
        print(f"El archivo {file_name} no tiene el formato esperado.")
        return None

    print(f"Procesando archivo: {file_name} (Tipo: {tipo}, Mes: {mes}, Año: {ano})")
    
    # Cargar el archivo dependiendo del tipo
    if tipo == 'importaciones':
        return obtener_importaciones_desde_archivo(ano, mes, file_name)
    elif tipo == 'exportaciones':
        return obtener_exportaciones_desde_archivo(ano, mes, file_name)

    
def procesar_archivos_txt():
    """
    Procesa todos los archivos .txt en el directorio de descargas, detectando automáticamente
    si son importaciones o exportaciones, y extrayendo los datos del mes y año.
    """
    archivos_txt = listar_archivos_txt()
    datos_completos = []
    
    for file_name in archivos_txt:
        try:
            datos = obtener_datos_archivo(file_name)
            datos_completos.append(datos)
        except Exception as e:
            print(f"Error procesando archivo {file_name}: {e}")
    
    # Concatenar todos los datos en un solo DataFrame
    if datos_completos:
        df_total = pd.concat(datos_completos, ignore_index=True)
        return df_total
    else:
        print("No se encontraron archivos .txt válidos.")
        return None


def obtener_datos(ano: str, mes: str, type: str):
    '''
    Obtiene los datos del archivo data.
    El archivo contiene los datos que pueden ser de exportaciones o importaciones.
    '''
    if type not in ['exportaciones', 'importaciones']:
        raise ValueError('Tipo de datos incorrecto')
    # Obtener los archivos que se encuentran en el directorio data
    files = [f.lower() for f in os.listdir(DATA_DIR) if os.path.isfile(os.path.join(DATA_DIR, f))]
    # Obtener los datos del archivo solicitado en base al año y mes
    for file in files:
        if ano in file and mes in file and type in file and '.txt' in file:
            file_path = os.path.join(DATA_DIR, file)
            return file_path  # Devolver la ruta del archivo
    raise FileNotFoundError(f"No se encontró el archivo para {ano}, {mes}, {type}")

def get_din_columns():
    return pd.read_excel(PATH_DIN_IMPORTACIONES, sheet_name=DIN_SHEET_NAME, skiprows=[1,134,135], usecols=[0,1,4,5,6], names=['CAMPO','GLOSA','TIPO','LARGO','PRECISION'], dtype=str)

def get_dus_columns():
    return pd.read_excel(PATH_DUS_EXPORTACIONES, sheet_name=DUS_SHEET_NAME, skiprows=[0],nrows=84, usecols=[0,1,3,4,5], names=['CAMPO','GLOSA','TIPO','LARGO','PRECISION'], dtype=str)

def obtener_importaciones(ano, mes):
    din_data = get_din_columns()
    path = obtener_datos(ano, mes, 'importaciones')
    df = pd.read_csv(path, sep=";", encoding="utf-8", names=din_data['CAMPO'], low_memory=True)
    return df

def obtener_exportaciones(ano, mes):
    path = obtener_datos(ano, mes, 'exportaciones')
    dus_data = get_dus_columns()
    # dus_data = dus_data[~duplicated]
    df = pd.read_csv(path, sep=";", encoding="utf-8", names=dus_data['CAMPO'], low_memory=True)
    return df

PATH_TABLA_CODIGOS = os.path.join(os.path.dirname(__file__), 'data','tablas_de_codigos.xlsx')
PUERTOS_SHEET_NAME = 'Puertos'
def obtener_coordenadas(nombre_puerto, pais):
    for _ in range(3):  # Intentar hasta 3 veces
        try:
            location = geolocator.geocode(f"{nombre_puerto}, {pais}")
            if location:
                return location.latitude, location.longitude
            else:
                print(f"Coordenadas no encontradas para {nombre_puerto}, {pais}")
                return None, None
        except GeocoderTimedOut:
            print(f"Tiempo de espera agotado al buscar {nombre_puerto}, {pais}. Reintentando...")
            time.sleep(1)
    print(f"No se pudo obtener coordenadas para {nombre_puerto}, {pais} después de varios intentos.")
    return None, None


def geolocalizar_puertos(df):
    """ Función que busca y completa las coordenadas (latitud y longitud) faltantes en el DataFrame """
    for index, row in df.iterrows():
        if pd.isna(row['LATITUD']) or pd.isna(row['LONGITUD']):
            # Intentar primero con TIPO_PUERTO + NOMBRE_PUERTO
            query_1 = f"{row['TIPO_PUERTO']} {row['NOMBRE_PUERTO']}"
            latitude, longitude = obtener_coordenadas(query_1, row['PAIS'])
            
            # Si falla, intentar solo con NOMBRE_PUERTO
            if not latitude or not longitude:
                query_2 = row['NOMBRE_PUERTO']
                latitude, longitude = obtener_coordenadas(query_2, row['PAIS'])

            # Si se obtienen coordenadas, se actualizan en el DataFrame
            if latitude and longitude:
                df.at[index, 'LATITUD'] = latitude
                df.at[index, 'LONGITUD'] = longitude
                print(f"Coordenadas encontradas para {query_1}: ({latitude}, {longitude})")
            else:
                print(f"Coordenadas no encontradas para {query_1}")
    
    return df

def completar_coordenadas_puertos():
    """
    Función que toma el DataFrame con información de puertos y completa
    las coordenadas faltantes (latitud y longitud) para todos los puertos.
    """
    # Leer el archivo Excel con todos los puertos
    df_puertos = pd.read_excel(PATH_TABLA_CODIGOS, sheet_name=PUERTOS_SHEET_NAME, dtype=str)

    # Asegurar que las columnas de coordenadas sean numéricas para evitar problemas con valores NaN
    df_puertos['LATITUD'] = pd.to_numeric(df_puertos['LATITUD'], errors='coerce')
    df_puertos['LONGITUD'] = pd.to_numeric(df_puertos['LONGITUD'], errors='coerce')

    # Geolocalizar puertos si faltan coordenadas
    df_puertos = geolocalizar_puertos(df_puertos)

    # Guardar las coordenadas actualizadas en el archivo Excel
    df_puertos.to_excel(PATH_TABLA_CODIGOS, sheet_name=PUERTOS_SHEET_NAME, index=False)

def buscar_puertos_por_codigo(df_codigos, completar_coordenadas=True):
    """
    Función que toma un DataFrame con códigos de puertos y devuelve la información
    asociada a esos puertos desde el DataFrame de puertos del Excel, además de completar
    las coordenadas faltantes (latitud y longitud) si es necesario.
    """
    if completar_coordenadas:
        # Completar coordenadas faltantes
        completar_coordenadas_puertos()
    # Leer el archivo Excel con los puertos
    df_puertos = pd.read_excel(PATH_TABLA_CODIGOS, sheet_name=PUERTOS_SHEET_NAME, dtype=str)
    

    # Asegurar que las columnas de coordenadas sean numéricas para evitar problemas con valores NaN
    df_puertos['LATITUD'] = pd.to_numeric(df_puertos['LATITUD'], errors='coerce')
    df_puertos['LONGITUD'] = pd.to_numeric(df_puertos['LONGITUD'], errors='coerce')

    # Limpiar los códigos de puerto y buscar los solicitados
    df_puertos['COD_PUERTO'] = df_puertos['COD_PUERTO'].str.strip()
    df_codigos = df_codigos.astype(str).str.strip()
    resultado = df_puertos[df_puertos['COD_PUERTO'].isin(df_codigos)]
    return resultado

def visualizar_puertos_en_mapa(df_puertos):
    """
    Función que toma un DataFrame con latitudes y longitudes y genera un mapa interactivo
    con los puertos.
    
    Parámetros:
    - df_puertos: DataFrame que contiene las columnas 'LATITUD', 'LONGITUD', 'NOMBRE_PUERTO', y 'PAIS'.
    
    Retorna:
    - Un mapa interactivo de Folium.
    """
    # Crear un mapa centrado en las coordenadas promedio (si quieres centrarlo en una región específica puedes cambiarlo)
    centro_lat = df_puertos['LATITUD'].mean()
    centro_lon = df_puertos['LONGITUD'].mean()
    
    # Inicializar el mapa de Folium centrado
    mapa = folium.Map(location=[centro_lat, centro_lon], zoom_start=4)
    
    # Agregar marcadores para cada puerto
    for index, row in df_puertos.iterrows():
        if not pd.isna(row['LATITUD']) and not pd.isna(row['LONGITUD']):
            popup_text = f"{row['NOMBRE_PUERTO']} - {row['PAIS']}"
            folium.Marker(location=[row['LATITUD'], row['LONGITUD']],
                          popup=popup_text).add_to(mapa)
    
    # Guardar el mapa en un archivo HTML o mostrarlo directamente
    mapa.save('mapa_puertos.html')
    return mapa

def obtener_rutas():
    """
    Obtener todas las rutas de los archivos de importaciones y exportaciones disponibles.
    """
    df_total = procesar_archivos_txt()
    if df_total is not None:
        # Extraer columnas relevantes para importaciones y exportaciones
        rutas = df_total[['PTO_EMB', 'PTO_DESEM']].dropna()
        return rutas
    else:
        return pd.DataFrame()  # Retorna un DataFrame vacío si no hay datos

def construir_grafo(rutas_totales):
    G = nx.Graph()
    
    # Añadir nodos (puertos)
    puertos = set(rutas_totales['PTO_EMB']).union(set(rutas_totales['PTO_DESEM']))
    G.add_nodes_from(puertos)
    
    # Añadir aristas entre puertos con al menos un viaje
    for _, row in rutas_totales.iterrows():
        puerto_origen = row['PTO_EMB']
        puerto_destino = row['PTO_DESEM']
        if G.has_edge(puerto_origen, puerto_destino):
            continue 
        else:
            G.add_edge(puerto_origen, puerto_destino)
    
    return G
def agregar_atributos_puertos(G):
    df_puertos = pd.read_excel(PATH_TABLA_CODIGOS, sheet_name=PUERTOS_SHEET_NAME, dtype=str)
    
    # Asegurar que las columnas de coordenadas sean numéricas
    df_puertos['LATITUD'] = pd.to_numeric(df_puertos['LATITUD'], errors='coerce')
    df_puertos['LONGITUD'] = pd.to_numeric(df_puertos['LONGITUD'], errors='coerce')
    
    # Crear un diccionario de atributos
    atributos = {}
    for _, row in df_puertos.iterrows():
        codigo_puerto = int(row['COD_PUERTO'].strip())
        atributos[codigo_puerto] = {
            'NOMBRE_PUERTO': row['NOMBRE_PUERTO'],
            'PAIS': row['PAIS'],
            'LATITUD': row['LATITUD'],
            'LONGITUD': row['LONGITUD']
        }
    
    # Asignar atributos a los nodos
    # print(atributos)
    nx.set_node_attributes(G, atributos)
    
    return G
def inhabilitar_puerto(G, codigo_puerto):
    """
    Función que inhabilita un puerto en el grafo eliminando el nodo correspondiente y sus aristas.
    """
    if G.has_node(codigo_puerto):
        G.remove_node(codigo_puerto)
        print(f"El puerto {codigo_puerto} ha sido inhabilitado en el grafo.")
    else:
        print(f"El puerto {codigo_puerto} no existe en el grafo.")
def obtener_vecinos_puerto(G, codigo_puerto):
    """
    Función que obtiene los puertos vecinos (adyacentes) al puerto dado.
    """
    if G.has_node(codigo_puerto):
        vecinos = list(G.neighbors(codigo_puerto))
        vecinos = [int(v) for v in vecinos]
        return vecinos
    else:
        print(f"El puerto {codigo_puerto} no existe en el grafo.")
        return []
import math

def calcular_distancia_geografica(lat1, lon1, lat2, lon2):
    """
    Calcula la distancia en kilómetros entre dos puntos geográficos utilizando la fórmula del haversine.
    """
    R = 6371  # Radio de la Tierra en kilómetros
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2) ** 2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distancia = R * c
    return distancia
def encontrar_puertos_cercanos(G, codigo_puerto_inhabilitado, num_puertos=5):
    """
    Encuentra los puertos más cercanos al puerto inhabilitado.
    
    Parámetros:
    - G: Grafo de NetworkX.
    - codigo_puerto_inhabilitado: Código del puerto que está inhabilitado.
    - num_puertos: Número de puertos cercanos a sugerir.
    
    Retorna:
    - Lista de códigos de puertos cercanos ordenados por distancia.
    """
    if codigo_puerto_inhabilitado not in G.nodes(data=False):
        # Si el puerto inhabilitado no está en el grafo (porque ya fue eliminado), lo agregamos temporalmente
        # para obtener sus coordenadas
        temp_node = True
        G_temp = G.copy()
        agregar_atributos_puertos(G_temp)
        lat1 = G_temp.nodes[codigo_puerto_inhabilitado].get('LATITUD')
        lon1 = G_temp.nodes[codigo_puerto_inhabilitado].get('LONGITUD')
    else:
        temp_node = False
        lat1 = G.nodes[codigo_puerto_inhabilitado].get('LATITUD')
        lon1 = G.nodes[codigo_puerto_inhabilitado].get('LONGITUD')
    
    if pd.isna(lat1) or pd.isna(lon1):
        print(f"No se tienen coordenadas para el puerto {codigo_puerto_inhabilitado}.")
        return []
    
    distancias = []
    for node in G.nodes():
        if node != codigo_puerto_inhabilitado:
            lat2 = G.nodes[node].get('LATITUD')
            lon2 = G.nodes[node].get('LONGITUD')
            if pd.notna(lat2) and pd.notna(lon2):
                distancia = calcular_distancia_geografica(lat1, lon1, lat2, lon2)
                distancias.append((node, distancia))
    
    # Ordenar por distancia
    distancias.sort(key=lambda x: x[1])
    puertos_cercanos = [codigo for codigo, _ in distancias[:num_puertos]]
    return puertos_cercanos


def visualizar_grafo(G):
    import matplotlib.pyplot as plt
    
    # Preparar los datos de posición
    pos = {}
    for node in G.nodes():
        lat = G.nodes[node].get('LATITUD')
        lon = G.nodes[node].get('LONGITUD')
        if pd.notna(lat) and pd.notna(lon):
            pos[node] = (lon, lat)  
        else:
            pos[node] = (0, 0) 
    
    plt.figure(figsize=(15, 10))
    
    # Dibujar nodos y aristas
    nx.draw_networkx_edges(G, pos=pos, edge_color='gray', alpha=0.5, width=1)
    nx.draw_networkx_nodes(G, pos=pos, node_size=50, node_color='red', alpha=0.8)
    
    # Añadir etiquetas
    labels = {}
    for node in G.nodes():
        nombre_puerto = G.nodes[node].get('NOMBRE_PUERTO', '')
        labels[node] = f"{nombre_puerto} ({node})"
    nx.draw_networkx_labels(G, pos=pos, labels=labels, font_size=8)
    
    plt.title("Grafo de Puertos y Rutas Históricas")
    plt.xlabel("Longitud")
    plt.ylabel("Latitud")
    plt.show()
def visualizar_grafo_folium(G):
    import folium
    from folium.plugins import MarkerCluster
    
    centro_lat = -35.6751  
    centro_lon = -71.5430  
    m = folium.Map(location=[centro_lat, centro_lon], zoom_start=5)
    
    # Crear un grupo de marcadores para los puertos
    marker_cluster = MarkerCluster().add_to(m)
    
    # Añadir nodos (puertos) al mapa
    for node in G.nodes():
        lat = G.nodes[node].get('LATITUD')
        lon = G.nodes[node].get('LONGITUD')
        nombre_puerto = G.nodes[node].get('NOMBRE_PUERTO', '')
        codigo_puerto = node
        if pd.notna(lat) and pd.notna(lon):
            folium.Marker(
                location=[lat, lon],
                popup=f"{nombre_puerto} ({codigo_puerto})",
                icon=folium.Icon(color='blue', icon='anchor', prefix='fa')
            ).add_to(marker_cluster)
    
    # Añadir aristas (rutas) al mapa
    for edge in G.edges():
        node1, node2 = edge
        lat1 = G.nodes[node1].get('LATITUD')
        lon1 = G.nodes[node1].get('LONGITUD')
        lat2 = G.nodes[node2].get('LATITUD')
        lon2 = G.nodes[node2].get('LONGITUD')
        if pd.notna(lat1) and pd.notna(lon1) and pd.notna(lat2) and pd.notna(lon2):
            folium.PolyLine(
                locations=[(lat1, lon1), (lat2, lon2)],
                color='red',
                weight=2,
                opacity=0.8
            ).add_to(m)
    
    m.save('grafo_puertos.html')
    print("El mapa interactivo ha sido guardado como 'grafo_puertos.html'.")


def visualizar_rutas_alternativas(G, puerto_origen, puertos_alternativos):
    import folium
    from folium.plugins import MarkerCluster
    
    centro_lat = -35.6751
    centro_lon = -71.5430
    m = folium.Map(location=[centro_lat, centro_lon], zoom_start=5)
    marker_cluster = MarkerCluster().add_to(m)
    
    # Añadir puertos al mapa
    for node in G.nodes():
        lat = G.nodes[node].get('LATITUD')
        lon = G.nodes[node].get('LONGITUD')
        nombre_puerto = G.nodes[node].get('NOMBRE_PUERTO', '')
        codigo_puerto = node
        if pd.notna(lat) and pd.notna(lon):
            if codigo_puerto == puerto_origen:
                color = 'green'
            elif codigo_puerto in [pa[0] for pa in puertos_alternativos]:
                color = 'orange'
            else:
                color = 'blue'
            folium.Marker(
                location=[lat, lon],
                popup=f"{nombre_puerto} ({codigo_puerto})",
                icon=folium.Icon(color=color, icon='anchor', prefix='fa')
            ).add_to(marker_cluster)

    
    for puerto_alternativo, camino in puertos_alternativos:
        coordenadas_camino = []
        for codigo_puerto in camino:
            lat = G.nodes[codigo_puerto].get('LATITUD')
            lon = G.nodes[codigo_puerto].get('LONGITUD')
            if pd.notna(lat) and pd.notna(lon):
                coordenadas_camino.append((lat, lon))
        if coordenadas_camino:
            folium.PolyLine(
                locations=coordenadas_camino,
                color='green',
                weight=3,
                opacity=0.8
            ).add_to(m)
    
    m.save('rutas_alternativas.html')
    print("El mapa interactivo con las rutas alternativas ha sido guardado como 'rutas_alternativas.html'.")

if __name__ == '__main__':
    # Descarga de archivos de importaciones y exportaciones
    download()
    
    #  Obtener puerto de origen y destino por terminal
    puerto_origen = int(sys.argv[1])
    puerto_destino_inhabilitado = int(sys.argv[2])
    # Obtener rutas históricas
    rutas_totales = obtener_rutas()
    print(f"Total de rutas encontradas: {len(rutas_totales)}")
    print(buscar_puertos_por_codigo(rutas_totales['PTO_EMB'], True))
    # Construir el grafo
    G = construir_grafo(rutas_totales)
    print(f"Número de puertos (nodos) en el grafo: {G.number_of_nodes()}")
    print(f"Número de rutas (aristas) en el grafo: {G.number_of_edges()}")
    
    # Agregar atributos de puertos
    G = agregar_atributos_puertos(G)
    
    # Verificar que los puertos de origen y destino existen en el grafo
    if puerto_origen not in G.nodes():
        print(f"El puerto de origen {puerto_origen} no existe en el grafo.")
    if puerto_destino_inhabilitado not in G.nodes():
        print(f"El puerto de destino {puerto_destino_inhabilitado} no existe en el grafo.")
    # Inhabilitar el puerto de destino en el grafo
    inhabilitar_puerto(G, puerto_destino_inhabilitado)
    
    # Obtner vecinos del puerto de origen
    vecinos_origen = obtener_vecinos_puerto(G, puerto_origen)
    print(f"Puertos conectados al puerto de origen {puerto_origen}: {vecinos_origen}")
    
    # Excluir el puerto inhabilitado si está en los vecinos
    if puerto_destino_inhabilitado in vecinos_origen:
        vecinos_origen.remove(puerto_destino_inhabilitado)
    
    # Los puertos vecinos son los puertos alternativos
    puertos_alternativos = []
    for puerto_alternativo in vecinos_origen:
        # Como son vecinos directos, la ruta es simplemente [puerto_origen, puerto_alternativo]
        camino = [puerto_origen, puerto_alternativo]
        puertos_alternativos.append((puerto_alternativo, camino))
    
    # Mostrar puertos alternativos y rutas
    # if puertos_alternativos:
    #     print("Puertos alternativos y rutas desde el puerto de origen:")
    #     for puerto, camino in puertos_alternativos:
    #         nombre_puerto = G.nodes[puerto].get('NOMBRE_PUERTO', '')
    #         print(f"Puerto alternativo: {nombre_puerto} ({puerto})")
    #         print(f"Ruta: {camino}")
    # else:
    #     print("No se encontraron puertos alternativos accesibles desde el puerto de origen.")
    
    if puertos_alternativos:
        # Visualizar las rutas alternativas
        visualizar_rutas_alternativas(G, puerto_origen, puertos_alternativos)
    visualizar_grafo_folium(G)



distances = [
    ("QUINTERO", "VALPARAISO", 30, 921, 905),  # QUINTERO (921) y VALPARAISO (905)
    ("VALPARAISO", "SAN ANTONIO", 15, 905, 906),  # VALPARAISO (905) y SAN ANTONIO (906)
    ("SAN ANTONIO", "TALCAHUANO", 135, 906, 907),  # SAN ANTONIO (906) y TALCAHUANO (907)
    ("TALCAHUANO", "S. VICENTE", 7, 907, 908),  # TALCAHUANO (907) y SAN VICENTE (908)
    ("S. VICENTE", "CORONEL", 117, 908, 926),  # SAN VICENTE (908) y CORONEL (926) - CORREGIDO
    ("S. VICENTE", "P. MONTI", 79, 908, 910),  # SAN VICENTE (908) y PUERTO MONTT (910)
    ("CORONEL", "MOCHA", 318, 926, None),  # CORONEL (926) - CORREGIDO
    ("MOCHA", "S. VICENTE", 318, None, 908),  # MOCHA sin referencia y SAN VICENTE (908)
    ("GUAFO", "DARWIN", 206, None, None),  # No hay referencias para GUAFO y DARWIN
    ("GUAFO", "RAPER", 81, None, None),  # No hay referencias para GUAFO y RAPER
    ("GUAFO", "CORONEL", 98, None, 926),  # GUAFO y CORONEL (926) - CORREGIDO
    ("DARWIN", "CHACABUCO", 37, None, 911),  # CHACABUCO (911)
    ("DARWIN", "MITAHUES", 15, None, None),  # No hay referencia para MITAHUES
    ("DARWIN", "PULLUCHE", 37, None, None),  # No hay referencia para PULLUCHE
    ("MITAHUES", "CHACABUCO", 42, None, 911),  # CHACABUCO (911)
    ("CHACABUCO", "VIA OCEANO", 456, 911, None),  # CHACABUCO (911)
    ("PULLUCHE", "INCHEMO", 7, None, None),  # No hay referencia para INCHEMO
    ("INCHEMO", "SAN PEDRO", 11, None, None),  # No hay referencia para SAN PEDRO
    ("SAN PEDRO", "CAMPANA", 34, None, None),  # No hay referencia para CAMPANA
    ("SAN PEDRO", "LADRILLEROS", 11, None, None),  # No hay referencia para LADRILLEROS
    ("LADRILLEROS", "PICHTON", 51, None, None),  # No hay referencia para PICHTON
    ("A INGLESA", "PIO XI", 19, None, None),  # No hay referencia para A INGLESA ni PIO XI
    ("PULLUCHE", "AUCHILO", 41, None, None),  # No hay referencia para AUCHILO
    ("AUCHILO", "NINUALAC", 37, None, None),  # No hay referencia para NINUALAC
    ("NINUALAC", "LAITE", 78, None, None),  # No hay referencia para LAITE
    ("LAITE", "TUAP", 41, None, None),  # No hay referencia para TUAP
    ("TUAP", "LOCOS", 153, None, None),  # No hay referencia para LOCOS
    ("LOCOS", "MITAHUES", 42, None, None),  # No hay referencia para MITAHUES
    ("NATALES", "BRINKLEY", 28, 936, None),  # NATALES (936)
    ("BRINKLEY", "SKUA", 16, None, None),  # No hay referencia para SKUA
    ("RAPER", "GUAFO", 81, None, None),  # No hay referencia para RAPER ni GUAFO
    ("PULLUCHE", "DARWIN", 37, None, None),  # No hay referencia para DARWIN ni PULLUCHE
    ("ROEPKE", "ENTRADA", 42, None, None),  # No hay referencia para ENTRADA
    ("ENTRADA", "CHACABUCO", 54, None, 911),  # CHACABUCO (911)
    ("NELSON-CASTRO-ESTEBAN", "CONCEPCIÓN", 73, None, 907),  # CONCEPCIÓN (907)
    ("CONCEPCIÓN", "PORPOISE", 157, 907, None),  # CONCEPCIÓN (907)
    ("PORPOISE", "HDO.", 21, None, None),  # No hay referencia para PORPOISE ni HDO.
    ("HDO.", "TRUENO", 7, None, None),  # No hay referencia para TRUENO
    ("TRUENO", "PICHTON", 51, None, None),  # No hay referencia para PICHTON
    ("PICHTON", "SAN PEDRO", 11, None, None),  # No hay referencia para SAN PEDRO
    ("EVANGELISTAS", "POSESION", 181, None, None),  # No hay referencia para POSESION
    ("POSESION", "RUPERT", 68, None, None),  # No hay referencia para RUPERT
    ("RUPERT", "COOPER KEY", 30, None, None),  # No hay referencia para COOPER KEY
    ("COOPER KEY", "FALGATE", 31, None, None),  # No hay referencia para FALGATE
    ("FALGATE", "SHOAL", 51, None, None),  # No hay referencia para SHOAL
    ("SHOAL", "FAIRWAY", 47, None, None),  # No hay referencia para FAIRWAY
    ("FAIRWAY", "NELSON-CASTRO-ESTEBAN", 73, None, None),  # No hay referencia para NELSON-CASTRO-ESTEBAN
    ("COOPER KEY", "MORRION", 31, None, None),  # No hay referencia para MORRION
    ("MORRION", "CROSSTIDE", 11, None, None),  # No hay referencia para CROSSTIDE
    ("CROSSTIDE", "CO. NEGRO", 36, None, None),  # No hay referencia para CO. NEGRO
    ("CO. NEGRO", "MAGDALENA", 50, None, None),  # No hay referencia para MAGDALENA
    ("MAGDALENA", "DELGADA", 21, None, None),  # No hay referencia para DELGADA
    ("DELGADA", "POSESION", 68, None, None),  # No hay referencia para POSESION
    ("POSESION", "DUNGENESS", 30, None, None),  # No hay referencia para DUNGENESS
    ("DUNGENESS", "RUPERT", 68, None, None),  # No hay referencia para DUNGENESS ni RUPERT
    ("POSESION", "C. DE HORNOS", 34, None, None),  # No hay referencia para CABO DE HORNOS
    ("C. DE HORNOS", "WILLIAMS", 30, None, 943),  # CABO DE HORNOS sin referencia y PUERTO WILLIAMS (943)
    ("WILLIAMS", "REPARO", 34, 943, None),  # PUERTO WILLIAMS (943)
    ("REPARO", "USHUAIA", 68, None, None),  # No hay referencia para USHUAIA
    ("USHUAIA", "ANXIOUS", 79, None, None),  # No hay referencia para ANXIOUS
    ("ANXIOUS", "FORWARD", 51, None, None),  # No hay referencia para FORWARD
    ("FORWARD", "CO. NEGRO", 66, None, None),  # No hay referencia para CO. NEGRO
    ("USHUAIA", "PERÓN", 79, None, None)  # No hay referencia para PERÓN
]
