import requests
import json
import sqlite3
import pandas as pd  # Importa la biblioteca pandas
import os

class ingesta:
    def __init__(self):
        self.ruta_estatica = os.path.join("src", "ibgd", "static")
        self.db = os.path.join(self.ruta_estatica, "db", "disney.db")
        self.csv = os.path.join(self.ruta_estatica, "csv", "disney.csv")
        self.auditoria = os.path.join(self.ruta_estatica, "auditoria", "auditoria.txt")

        os.makedirs(os.path.dirname(self.db), exist_ok=True)
        os.makedirs(os.path.dirname(self.csv), exist_ok=True)
        os.makedirs(os.path.dirname(self.auditoria), exist_ok=True)

        try:
            os.makedirs(os.path.dirname(self.db), exist_ok=True)  # Crea la carpeta si no existe
            print("Carpeta creada correctamente.")
        except PermissionError:
            print("Error: No tienes permisos para crear la carpeta.")

    def obtener_datos_api(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            datos = response.json()
            return datos.get("data", [])
        except requests.exceptions.RequestException as error:
            print(error)
            return []
    
    def crear_base_datos(self):
        conexion = sqlite3.connect(self.db)
        cursor = conexion.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS personajes (
            id INTEGER PRIMARY KEY,
            name TEXT,
            films TEXT,
            tvShows TEXT
        )
        """)
        conexion.commit()
        conexion.close()

    def insertar_datos(self, personajes):
        conexion = sqlite3.connect(self.db)
        cursor = conexion.cursor()
        registros_insertados = 0
        registros_existentes = 0

        for personaje in personajes:
            if not isinstance(personaje, dict):
                continue 
            personaje_id = personaje.get("_id")
            if not personaje_id:
                continue
            try:
                cursor.execute("""
                INSERT INTO personajes (id, name, films, tvShows)
                VALUES (?, ?, ?, ?)
                """, (
                    int(personaje_id),
                    personaje.get("name", ""),
                    ", ".join(personaje.get("films", [])),
                    ", ".join(personaje.get("tvShows", []))
                ))
                registros_insertados += 1
            except sqlite3.IntegrityError:
                print(f"El ID {personaje_id} ya existe en la base de datos.")
                registros_existentes += 1

        conexion.commit()
        conexion.close()
        return registros_insertados, registros_existentes

    def generar_csv(self):
        conexion = sqlite3.connect(self.db) 
        df = pd.read_sql_query("SELECT * FROM personajes", conexion)
        df.to_csv(self.csv, index=False)
        conexion.close()
        return len(df)

    def generar_auditoria(self, total_api, total_insertados, total_existentes, total_csv):
        with open(self.auditoria, "w") as f:
            f.write(f"Total registros obtenidos del API: {total_api}\n")
            f.write(f"Total registros insertados en SQLite: {total_insertados}\n")
            f.write(f"Total registros ya existentes en SQLite: {total_existentes}\n")
            f.write(f"Total registros en CSV: {total_csv}\n")
            if total_api == total_insertados + total_existentes:
                f.write("Los datos fueron almacenados correctamente.\n")
            else:
                f.write("Hubo diferencias en la cantidad de registros almacenados.\n")
    

ingesta = ingesta()
url = "https://api.disneyapi.dev/character"
datos = ingesta.obtener_datos_api(url=url)
print(json.dumps(datos[:3], indent=2)) 

ingesta.crear_base_datos()
registros_insertados, registros_existentes = ingesta.insertar_datos(personajes=datos)
total_csv = ingesta.generar_csv()
ingesta.generar_auditoria(len(datos), registros_insertados, registros_existentes, total_csv)