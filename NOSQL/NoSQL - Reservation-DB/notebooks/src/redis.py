from db_connections import db_redis as r
import random, json

def borrar_reservas_temporales():
    """
    Borra todas las reservas temporales y devuelve la cantidad de claves eliminadas.
    """
    claves = list(r.scan_iter("reserva_temp:*"))
    
    if not claves:
        return None
    
    r.delete(*claves)
    
    return len(claves)

def carga_masiva_reservas_temporales(df, ttl=3600):
    """
    Carga masivamente en Redis las reservas temporales

    Parametros:
        df: DataFrame con los datos
        ttl: tiempo de expiración de la clave.
    """
    if df is None or df.empty:
        return None

    borrar_reservas_temporales()    
    filas = df.to_dict(orient="records")
    
    for fila in filas:
        clave = f"reserva_temp:{fila['reserva_id']}"
        r.hset(clave, mapping={
                "usuario_id": fila["usuario_id"],
                "destino_id": fila["destino_id"],
                "fecha_reserva": fila["fecha_reserva"],
                "precio_total": fila["precio_total"]
        })
        r.expire(clave, ttl)

    return len(filas)

def guardar_usuarios_conectados(df, cantidad=10):
    """
    Elige aleatoriamente `cantidad` usuarios del DataFrame
    y los guarda en Redis como conectados.

    Parametros:
        df: Dataframe con los datos
        cantidad: cantidad de usuario a almacenar. Por defecto 10
    """  
    if df is None or df.empty:
        return None

    # Elige aleatoriamente sin repetir
    seleccionados = df.sample(n=min(cantidad, len(df)), random_state=42)
    filas = seleccionados.to_dict(orient="records")

    for fila in filas:
        r.set(f"usuario:{fila['usuario_id']}:sesion", "activa", ex=3600) #Expira en 1hs

    return len(filas)

def generar_clave_cache(tipo, parametros):
    """
    Genera una clave única para búsquedas cacheadas.

    Parametros:
        tipo: 'destinos', 'hoteles', 'actividades'
        parametros: diccionario con filtros
    """
    partes = [f"{k}:{v}" for k, v in sorted(parametros.items())]
    return f"busqueda:{tipo}:" + "|".join(partes)

def obtener_cache(tipo, parametros):
    """
    Obtiene de redis la busqueda cacheada.

    Parametros:
        tipo: 'destinos', 'hoteles', 'actividades'
        parametros: diccionario con filtros
    """
    clave = generar_clave_cache(tipo, parametros)
    resultado = r.get(clave)
    if resultado:
        return json.loads(resultado)
    return None

def guardar_en_cache(tipo, parametros, resultado, ttl=600):
    """
    Guarda en cache los datos pasados por parametros

    Parametros:
        tipo: 'destinos', 'hoteles', 'actividades'
        parametros: diccionario con filtros
        resultado: lista con los resultados de la búsqueda a guardar.
        ttl: tiempo de expiración de la busqueda
    """
    clave = generar_clave_cache(tipo, parametros)
    try:
        return r.set(clave, json.dumps(resultado), ex=ttl)
    except Exception:
        return False

def insertar_en_redis(nombre_coleccion, df):
    """
    Guarda datos en Redis (solo usuarios y reservas temporales).

    Parametros:
        nombre_colección: nombre de la colección en MongoDB 
        df: DataFrame con los datos a guardar.
    """
    
    if nombre_coleccion == "usuarios":
        try:
            conectados = guardar_usuarios_conectados(df, 15)
            print(f"✅ Se registran {conectados} usuarios conectados en Redis.")
        except Exception as e:
            print(f"⚠️ Error al guardar usuarios conectados en Redis: {e}")

    elif nombre_coleccion == "reservas":
        df_reservas_temporales = df[df["estado"].isna()]
        try:
            resultado = carga_masiva_reservas_temporales(df_reservas_temporales)
            print(f"✅ Se insertaron {resultado} reservas temporales en Redis.")
        except Exception as e:
            print(f"⚠️ Error al cargar reservas temporales en Redis: {e}")





