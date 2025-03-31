## Proyecto Big Data

Este repositorio contiene dos Scripts donde el primero se encarga de obtienen los datos sobre los personajes de disney los cuales son almacenados en una base de datos SQLite.
El segundo Script realiza la liempieza de datos eliminando duplicados, manejando valores nulos y generando un informe de auditoria donde informa las tranformaciones realizadas.

## Instalación y Configuración del repositorio

Paso 1: 
En la consola Git bash ejecutar la siguiente línea de código
git clone https://github.com/nkrojas/Natalia_Rojas.git

Paso 2: 
Crear entorno virtual, en el terminal ejecutar las dos líneas de código
python -m venv venv
venv\Scripts\activate

Paso 3:
Instalar dependencias, en el terminar ejecutar la siguiente línea de código
pip instal -e .

Paso 4:
Ejecución de Scripts, en el terminal ejecutar las dos líneas de código
python src/ibgd/ingesta.py
python src/ibgd/cleaning.py
python src/ibgd/transformacion.py