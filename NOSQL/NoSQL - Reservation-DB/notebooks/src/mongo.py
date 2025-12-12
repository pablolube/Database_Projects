from db_connections import client
from src.utils import lectura_csv
from pprint import pprint
import pandas as pd
import ast

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#                                                             ALTA Y CARGA MONGO
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def coleccion_existe(db, nombre_coleccion):
    """
    Verifica si una colección existe en la base de datos.

    Parámetros:
    - db: objeto pymongo.database.Database
    - nombre_coleccion: nombre de la colección a verificar (string)

    Retorna:
    - True si existe, False si no
    """
    return nombre_coleccion in db.list_collection_names()  

def obtener_coleccion(nombre_base, nombre_coleccion):
    """
    Devuelve el objeto Collection.

    Parámetros:
    - nombre_base: nombre de la base de datos de MongoDB
    - nombre_coleccion: nombre de la colección 
    """
    db = client[nombre_base]
    if not coleccion_existe(db, nombre_coleccion):
        raise KeyError(
            f"La colección '{nombre_coleccion}' no existe en la base '{nombre_base}'.")
    return db[nombre_coleccion]

def crear_coleccion(nombre_base, nombre_coleccion, recrear=False):
    """
    Crea una colección dentro de la base de datos especificada.

    Parametros:
        nombre_base:nombre de la base de datos dentro de mongoDB
        nombre_coleccion: nombre de la colección a crear
        recrear: si es True, borra la coleccion. 
    Retorna:
        coleccion creada
    """
    db = client[nombre_base]

    if coleccion_existe(db, nombre_coleccion):
        if recrear:
            db.drop_collection(nombre_coleccion)
        else:
            return db[nombre_coleccion]

    coleccion = db.create_collection(nombre_coleccion)
    return coleccion

def insertar_muchos_coleccion(nombre_base, nombre_coleccion, datos, ordenado=False):
    """
    Inserta varios documentos en una colección.

    Parametros:
        nombre_base: nombre de la base de datos dentro de mongoDB
        nombre_coleccion: nombre de la colección a ingresar los datos.
        datos: diccionario con las datos que se quieren incluir.
        ordenado: Si es True, se ingresan los datos ordenados.
    """
    if datos is None:
        raise ValueError("El parámetro 'datos' no puede ser None.")
        
    lista_datos = list(datos)
    if not lista_datos:
        print("⚠️ El DataFrame está vacío, no se insertaron datos.")
        return None

    db = client[nombre_base]
    coleccion = db[nombre_coleccion]

    try:
        resultado = coleccion.insert_many(lista_datos, ordered=ordenado)
        print(f"✅ Se insertaron {len(resultado.inserted_ids)} documentos en '{coleccion.name}'.")
        return resultado
        
    except Exception as e:
        print(f"❌ Error inesperado: {type(e).__name__} - {e}")

def insertar_en_mongo(nombre_base, nombre_coleccion, df):
    """
    Crea e inserta datos en una colección de MongoDB.

    Parametros:
        nombre_base: nombre de la base de datos dentro de mongoDB
        nombre_coleccion: nombre de la colección a ingresar los datos.
        df: dataFrame con los datos a insertar
    """
    crear_coleccion(nombre_base, nombre_coleccion, recrear=True)
    
    if nombre_coleccion == "hoteles":
        #Se convierten los servicios en una lista para que no se guarde como string
        df["servicios"] = df["servicios"].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x)

    filas = df.to_dict(orient="records")

    # Si es reservas, se filtran solo las que tienen estado
    if nombre_coleccion == "reservas":
        df_filtrado = df[df["estado"].notna() & (df["estado"].astype(str).str.strip() != "")]
        insertar_muchos_coleccion(nombre_base, nombre_coleccion, df_filtrado.to_dict("records"))
    else:
        insertar_muchos_coleccion(nombre_base, nombre_coleccion, filas)

    print(f"✅ Colección {nombre_coleccion} creada e insertada en MongoDB.")

def cargar_df_a_coleccion(df, nombre_base, nombre_coleccion,ordenado=False):
    """
    Carga un DataFrame en una colección de MongoDB.
    Devuelve el InsertManyResult o None si no se insertó nada.

    Parametros:
        df: DataFrame con los datos.
        nombre_base: nombre de la base de datos dentro de mongoDB
        nombre_coleccion: nombre de la colección a guardar
        ordenado (opcional): Si es True, lo inserta ordenado.
    """ 
    if df is None or df.empty:
        return None

    # Convertir a diccionario
    datos = df.to_dict(orient="records")

    return insertar_muchos_coleccion(nombre_base, nombre_coleccion, datos, ordenado)


#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#                                                             CONSULTAS
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def obtener_cursor(nombre_base, nombre_coleccion, limite=None, filtro=None, proyeccion=None):
    """
    Retorna un cursor para la consulta.

    Parametros:
        nombre_base: nombre de la base de datos dentro de mongoDB
        nombre_colección: colección de la cual se quieren obtener datos
        limite (opcional): limite de registros que desean obtener.
        filtro (opcional): filtro para la consulta a la base.
        proyeccion (opcional): campos a proyectar
    Retorna
        cursor: datos de la consulta a la colección
    """
    db = client[nombre_base]
    if not coleccion_existe(db, nombre_coleccion):
        raise KeyError(
            f"La colección '{nombre_coleccion}' no existe en la base '{nombre_base}'.")

    coleccion = db[nombre_coleccion]
    cursor = coleccion.find(filter=filtro or {}, projection=proyeccion)
    if limite is not None:
        cursor = cursor.limit(limite)
    return cursor   

def contar_documentos(nombre_base, nombre_coleccion):
    """
    Devuelve la cantidad de documentos de una coleccion

    Parametros:
        nombre_base: nombre de la base de datos dentro de mongoDB
        nombre_colección: colección de la cual se quieren obtener datos
    """
    db = client[nombre_base]
    coleccion = db[nombre_coleccion]
    
    if not coleccion_existe(db, nombre_coleccion):
        raise KeyError(
            f"La colección '{nombre_coleccion}' no existe en la base '{nombre_base}'.")
    
    return coleccion.count_documents({})

def contador(nombre_base, coleccion, agrupacion=None, campo_calculo="cantidad", filtrar=None):
    """
    Realiza un conteo (u operación genérica) de documentos en MongoDB con opción de agrupar y filtrar.

    Parámetros:
        nombre_base: str, nombre de la base de datos
        coleccion: str, nombre de la colección
        agrupacion: str o None, campo por el que agrupar. Si None, no agrupa
        campo_calculo: str, nombre del campo de salida para el conteo
        filtrar: dict o None, filtro tipo {"campo": valor} para usar en $match
    
    Retorna:
        Lista de diccionarios con el resultado de la agregación
    """

    db = client[nombre_base]
    coll = db[coleccion]

    pipeline = []

    # Si hay filtro, agregamos $match
    if filtrar:
        pipeline.append({"$match": filtrar})

    # Si se indica agrupación, agregamos $group
    if agrupacion:
        pipeline.append({
            "$group": {
                "_id": f"${agrupacion}",
                campo_calculo: {"$sum": 1}
            }
        })
    else:
        # Si no hay agrupación, solo contamos todos los documentos que cumplen el filtro
        pipeline.append({
            "$group": {
                "_id": None,
                campo_calculo: {"$sum": 1}
            }
        })

    resultado = list(coll.aggregate(pipeline))
    return resultado

