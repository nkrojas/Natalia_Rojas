import requests
import json

def obtener_datos_api(url=""):
    url = url
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print("Error en la petición: ", error)
        return {}

url = "https://api.disneyapi.dev/character"
datos = obtener_datos_api(url=url)

if len(datos)>0:
    print(json.dumps(datos,indent=4))

else:
    print("No se obtuvieron datos") 