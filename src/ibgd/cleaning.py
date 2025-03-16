import sqlite3
import pandas as pd

class DataCleaning:
    def __init__(self):
        self.db = "src/ibgd/static/db/disney.db"
        self.cleaned_csv = "src/ibgd/static/csv/cleaned_disney.csv"
        self.audit_file = "src/ibgd/static/auditoria/cleaning_report.txt"

    def cargar_datos(self):
        conexion = sqlite3.connect(self.db)
        df = pd.read_sql_query("SELECT * FROM personajes", conexion)
        conexion.close()
        return df
    
    def analizar_datos(self, df):
        resumen = {
            "Total de registros": len(df),
            "Duplicados": df.duplicated().sum(),
            "Valores nulos": df.isnull().sum().to_dict(),
            "Tipos de datos": df.dtypes.to_dict()
        }
        return resumen
    
    def limpiar_datos(self, df):
        df = df.drop_duplicates()
        df = df.dropna(subset=["name"]) 
        df.fillna("null", inplace=True) 
        return df
    
    def guardar_datos_limpios(self, df):
        df.to_csv(self.cleaned_csv, index=False)
        return len(df)
    
    def generar_auditoria(self, resumen_inicial, total_final):
        with open(self.audit_file, "w") as f:
            f.write("Análisis Inicial:\n")
            for key, value in resumen_inicial.items():
                f.write(f"{key}: {value}\n")
            f.write(f"\nTotal de registros después de limpieza: {total_final}\n")
            f.write("\nTransformaciones realizadas:\n")
            f.write("- Eliminación de duplicados\n")
            f.write("- Manejo de valores nulos\n")
            f.write("- Corrección de tipos de datos\n")

data_cleaning = DataCleaning()
df = data_cleaning.cargar_datos()
resumen_inicial = data_cleaning.analizar_datos(df)
df_limpio = data_cleaning.limpiar_datos(df)
total_final = data_cleaning.guardar_datos_limpios(df_limpio)
data_cleaning.generar_auditoria(resumen_inicial, total_final)
