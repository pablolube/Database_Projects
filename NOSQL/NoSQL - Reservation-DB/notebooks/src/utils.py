import pandas as pd
from pathlib import Path
import random
from faker import Faker
import os
from collections import defaultdict
from itertools import combinations


def lectura_csv(ruta):
    """
    Lee un CSV desde la ruta dada y devuelve un DataFrame, o None si no existe.
    """
    if ruta.exists():
        df = pd.read_csv(ruta)
        return df
    else:
        print("⚠️ No se encontró el archivo en:", ruta)
        return None

def procesar_csv(nombre_archivo):
    """
    Lee un CSV desde la carpeta 'fuentes' y devuelve un DataFrame si tiene datos.
    """
    ruta = Path("fuentes") / nombre_archivo
    df = lectura_csv(ruta)
    if df is None or df.empty:
        print(f"⚠️ Archivo {nombre_archivo} vacío o no encontrado.")
        return None
    return df

def generar_csv_datos_ficticios():
    """
    Genera datasets de Actividades, Destinos, Hoteles, Reservas, Usuarios_relaciones y Usuarios con datos ficticios.
    """
    # -----------------------------
    # Configuración del faker
    # -----------------------------
    fake = Faker('es_ES')
    Faker.seed(42)
    random.seed(42)
    
    # -----------------------------
    # Datos de input
    # -----------------------------
    provincias_arg = [
        "Buenos Aires",
        "Ciudad Autónoma de Buenos Aires",
        "Catamarca",
        "Chaco",
        "Chubut",
        "Córdoba",
        "Corrientes",
        "Entre Ríos",
        "Formosa",
        "Jujuy",
        "La Pampa",
        "La Rioja",
        "Mendoza",
        "Misiones",
        "Neuquén",
        "Río Negro",
        "Salta",
        "San Juan",
        "San Luis",
        "Santa Cruz",
        "Santa Fe",
        "Santiago del Estero",
        "Tierra del Fuego",
        "Tucumán"
    ]
    ciudades_arg = {
        "Buenos Aires": ["La Plata", "Mar del Plata"],
        "Ciudad Autónoma de Buenos Aires": ["CABA"],
        "Catamarca": ["San Fernando"],
        "Chaco": ["Resistencia"],
        "Chubut": ["Puerto Madryn"],
        "Córdoba": ["Córdoba", "Villa Carlos Paz", "Río Cuarto"],
        "Corrientes": ["Corrientes"],
        "Entre Ríos": ["Paraná", "Concordia", "Gualeguaychú"],
        "Formosa": ["Formosa"],
        "Jujuy": ["San Salvador de Jujuy"],
        "La Pampa": ["Santa Rosa"],
        "La Rioja": ["La Rioja", "Chilecito",],
        "Mendoza": ["Mendoza", "San Rafael"],
        "Misiones": ["Posadas", "Iguazú"],
        "Neuquén": ["Neuquén", "San Martín de los Andes"],
        "Río Negro": ["Bariloche", "Viedma"],
        "Salta": ["Salta"],
        "San Juan": ["San Juan"],
        "San Luis": ["San Luis", "Merlo"],
        "Santa Cruz": ["Río Gallegos", "El Calafate"],
        "Santa Fe": ["Rosario", "Santa Fe"],
        "Santiago del Estero": ["Santiago del Estero"],
        "Tierra del Fuego": ["Ushuaia"],
        "Tucumán": ["San Miguel de Tucumán"]
    }
    tipos_destino = ["Cultural", "Playa", "Montaña", "Aventura", "Relax"]
    nombre_actividad = [
        "Visita guiada a ciudad",
        "Tour gastronómico local",
        "Caminata por parque",
        "Paseo en bicicleta",
        "Clase de yoga o meditación",
        "Excursión a sitio turístico cercano",
        "Recorrido cultural o histórico",
        "Actividad de bienestar en spa",
        "Clase de cocina típica",
        "Participación en evento o festival local"
    ]
    tipos_actividad = ["aventura", "cultural",
                       "gastronómica", "relax", "deportiva"]
    servicios_posibles = ["wifi", "spa", "pileta",
                          "desayuno", "gimnasio", "restaurant"]
    estados_reserva = ["Confirmada", "Pagada", "Pendiente", "Cancelada", ""]
    
    # ----------------------------
    # Parametros del generador
    # -----------------------------
    n_usuarios = 60
    n_hoteles = 5
    n_actividades = 3
    n_reservas = 200
    
    # -----------------------------
    # Generar Usuarios
    # -----------------------------
    usuarios = []
    for i in range(1, n_usuarios+1):
        usuarios.append({
            "usuario_id": i,
            "nombre": fake.first_name(),
            "apellido": fake.last_name(),
            "email": fake.unique.email(),
            "telefono": fake.phone_number()
        })
    usuario_ids = [u["usuario_id"] for u in usuarios]
    
    # -----------------------------
    # Generar Destinos
    # -----------------------------
    destinos = []
    destino_id = 1
    
    for provincia in provincias_arg:
        for ciudad in ciudades_arg[provincia]:
            destinos.append({
                "destino_id": destino_id,
                "provincia": provincia,
                "ciudad": ciudad,
                "pais": "Argentina",
                "tipo": random.choice(tipos_destino),
                "precio_promedio": random.randint(50000, 200000)
            })
            destino_id += 1
    
    # -----------------------------
    # Generar Hoteles
    # -----------------------------
    hoteles = []
    hotel_id = 1
    for destino in destinos:
        n_hoteles_ciudad = random.randint(1, n_hoteles)
        for _ in range(n_hoteles_ciudad):
            hoteles.append({
                "hotel_id": hotel_id,
                "nombre": f"{fake.company()} Hotel",
                "ciudad": destino["ciudad"],
                "provincia": destino["provincia"],
                "precio": random.randint(80000, 300000),
                "calificacion": random.randint(1, 5),
                "servicios": random.sample(servicios_posibles, random.randint(2, 4))
            })
            hotel_id += 1
    
    # -----------------------------
    # Generar Actividades
    # -----------------------------
    actividades = []
    actividad_id = 1
    n_actividades_ciudad = 3
    
    for destino in destinos:
        for _ in range(n_actividades_ciudad):
            actividades.append({
                "actividad_id": actividad_id,
                "nombre": random.choice(nombre_actividad),
                "tipo": random.choice(tipos_actividad),
                "ciudad": destino["ciudad"],
                "provincia": destino["provincia"],
                "precio": random.randint(20000, 80000)
            })
            actividad_id += 1
    
    # -----------------------------
    # Generar Reservas
    # -----------------------------
    reservas = []
    reserva_id = 1
    
    # Índice auxiliar: ciudad → destino_id y precio
    destino_por_ciudad = {
        d["ciudad"]: {"id": d["destino_id"], "precio": d["precio_promedio"]}
        for d in destinos
    }
    
    # Diccionario para controlar cuántas reservas tiene cada usuario
    reservas_usuario = {u["usuario_id"]: [] for u in usuarios}
    
    #  Cada hotel tiene entre 30 y 50 reservas
    for hotel in hoteles:
        ciudad_hotel = hotel["ciudad"]
        destino_info = destino_por_ciudad[ciudad_hotel]
        destino_id = destino_info["id"]
        destino_precio = destino_info["precio"]
    
        # Número aleatorio de reservas por hotel
        n_reservas_hotel = random.randint(5, 30)
    
        for _ in range(n_reservas_hotel):
            usuario_id = random.choice(usuario_ids)
            reservas.append({
                "reserva_id": reserva_id,
                "usuario_id": usuario_id,
                "destino_id": destino_id,
                "hotel_id": hotel["hotel_id"],
                "fecha_reserva": fake.date_between(start_date="-1y", end_date="+6m").isoformat(),
                "estado": random.choice(estados_reserva),
                # pequeña variación
                "precio_total": destino_precio + random.randint(-10000, 10000)
            })
            reservas_usuario[usuario_id].append(destino_id)
            reserva_id += 1
    
    #  Asegurar que cada usuario tenga al menos 5 reservas en distintas ciudades
    ciudades_disponibles = list(destino_por_ciudad.keys())
    
    for usuario_id, destinos_usuario in reservas_usuario.items():
        ciudades_reservadas = {d for d in destinos_usuario}
        faltan = 5 - len(ciudades_reservadas)
        if faltan > 0:
            nuevas_ciudades = random.sample(
                [c for c in ciudades_disponibles if destino_por_ciudad[c]
                    ["id"] not in ciudades_reservadas],
                faltan
            )
            for ciudad in nuevas_ciudades:
                destino_info = destino_por_ciudad[ciudad]
                reservas.append({
                    "reserva_id": reserva_id,
                    "usuario_id": usuario_id,
                    "destino_id": destino_info["id"],
                    "hotel_id": random.choice([h["hotel_id"] for h in hoteles if h["ciudad"] == ciudad]),
                    "fecha_reserva": fake.date_between(start_date="-1y", end_date="+6m").isoformat(),
                    "estado": random.choice(estados_reserva),
                    "precio_total": destino_info["precio"] + random.randint(-10000, 10000)
                })
                reserva_id += 1
    
    #print(f"✅ Total de reservas generadas: {len(reservas)}")
    
    
    usuario_ids = list(range(1, n_usuarios+1))
    
    # Máximos de relaciones
    max_amigos = 8
    max_familiares = 2
    
    # -----------------------------
    # Generar relaciones
    # -----------------------------
    
    # Generar todos los pares posibles y mezclarlos
    pares_posibles = list(combinations(usuario_ids, 2))
    random.shuffle(pares_posibles)
    
    # Diccionarios para contar relaciones
    amigos_count = defaultdict(int)
    familiares_count = defaultdict(int)
    
    # Lista de relaciones
    relaciones = []
    
    for usuario1, usuario2 in pares_posibles:
        if len(relaciones) >= 300:
            break
    
        # Elegir tipo de relación
        tipo_relacion = random.choice(["AMIGO_DE", "FAMILIAR_DE"])
    
        # Validar límites
        if tipo_relacion == "AMIGO_DE":
            if amigos_count[usuario1] >= max_amigos or amigos_count[usuario2] >= max_amigos:
                continue
            amigos_count[usuario1] += 1
            amigos_count[usuario2] += 1
        else:
            if familiares_count[usuario1] >= max_familiares or familiares_count[usuario2] >= max_familiares:
                continue
            familiares_count[usuario1] += 1
            familiares_count[usuario2] += 1
    
        # Agregar relación
        relaciones.append({
            "usuario1": usuario1,
            "usuario2": usuario2,
            "tipo": tipo_relacion
        })
    
    # -----------------------------
    # Guardar CSV
    # -----------------------------
    carpeta_destino = Path("fuentes")
    os.makedirs(carpeta_destino, exist_ok=True)
    
    pd.DataFrame(usuarios).to_csv(
        f"{carpeta_destino}/usuarios.csv", index=False, encoding="utf-8")
    pd.DataFrame(destinos).to_csv(
        f"{carpeta_destino}/destinos.csv", index=False, encoding="utf-8")
    pd.DataFrame(hoteles).to_csv(
        f"{carpeta_destino}/hoteles.csv", index=False, encoding="utf-8")
    pd.DataFrame(actividades).to_csv(
        f"{carpeta_destino}/actividades.csv", index=False, encoding="utf-8")
    pd.DataFrame(reservas).to_csv(
        f"{carpeta_destino}/reservas.csv", index=False, encoding="utf-8")
    pd.DataFrame(relaciones).to_csv(
        f"{carpeta_destino}/usuarios_relaciones.csv", index=False, encoding="utf-8")
    #print(f"Se generaron {len(relaciones)} relaciones para {len(usuario_ids)} usuarios.")
    print(f"✅ Archivos generados correctamente en {carpeta_destino}")