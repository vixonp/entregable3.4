from elasticsearch import Elasticsearch
import pandas as pd
import os


def conectar_es():
    return Elasticsearch("http://localhost:9200")

def cargar_excel(indice, ruta, columnas=None):
    if not os.path.exists(ruta):
        print(f"❌ Archivo no encontrado: {ruta}")
        return
    df = pd.read_excel(ruta)
    if columnas:
        df.columns = columnas
    es = conectar_es()
    if not es.ping():
        print("❌ No se pudo conectar a Elasticsearch.")
        return
    for _, fila in df.iterrows():
        es.index(index=indice, document=fila.to_dict())
    print(f"✅ Datos cargados al índice: {indice}")

def cargar_datos():
    cargar_excel("tipo_calle", "data/output_tipo_calle.xlsx", ["tipo", "calle", "cantidad"])
    cargar_excel("por_hora", "data/output_por_hora.xlsx", ["hora", "cantidad"])
    cargar_cache()

def cargar_cache():
    cargar_excel("cache_metrics", "resultados_cache.xlsx")



if __name__ == "__main__":
    cargar_datos()
