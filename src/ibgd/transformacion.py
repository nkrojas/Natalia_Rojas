import requests
import json
import pandas as pd

class Transformacion:
    
    def dataset1(self):
        df1 = pd.read_csv("src/ibgd/static/csv/disney.csv")
        print("Se cargó df1 desde disney.csv")
        return df1
    
    def dataset2(self):
        url = "https://api.disneyapi.dev/character"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json().get("data", [])
            df2 = pd.DataFrame([{ 
                "id": item.get("_id", ""),
                "createdAt": item.get("createdAt", ""),
                "updatedAt": item.get("updatedAt", ""),
                "url": item.get("url", "")
            } for item in data])
            print("Se cargó df2 desde la API de Disney")
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos de la API: {e}")
            df2 = pd.DataFrame()
        return df2

    def join_datasets(self, df1, df2):
        df3 = pd.merge(df1, df2, on="id", how="inner")
        df3.to_csv("src/ibgd/static/csv/enriched_disney.csv", index=False)
        print("Se generó el dataset enriquecido en enriched_disney.csv")
        return df3
    
    def auditoria(self, df1, df2, df3): 
        num_registros_base = len(df1)
        num_registros_api = len(df2)
        num_registros_enriquecido = len(df3)
        registros_coincidentes = len(df3)
        diferencias_detectadas = num_registros_base - registros_coincidentes
        
        with open("src/ibgd/static/auditoria/auditoriaETL.txt", "w") as f:
            f.write(f"Columnas en dataset 1: {', '.join(df1.columns)}\n")
            f.write(f"Registros en dataset 1: {num_registros_base}\n")
            f.write(f"Columnas en dataset 2: {', '.join(df2.columns)}\n")
            f.write(f"Registros en dataset 2: {num_registros_api}\n")
            f.write(f"Total de columnas en el dataset enriquecido: {', '.join(df3.columns)} ({len(df3.columns)})\n")
            f.write(f"Total de registros en el dataset enriquecido: {num_registros_enriquecido}\n")
            f.write(f"Registros coincidentes en el cruce: {registros_coincidentes}\n")
            f.write(f"Diferencias detectadas: {diferencias_detectadas}\n")
        
        print("Se realizó la auditoría")
        return True

    def ejecucion(self):
        df1 = self.dataset1()
        df2 = self.dataset2()
        df3 = self.join_datasets(df1, df2) 
        if self.auditoria(df1, df2, df3):
            print("Actividad 3 exitosa")       
        pass

trx = Transformacion()
trx.ejecucion()
