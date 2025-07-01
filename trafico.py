import numpy as np
import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from db import ejecutar_query
import redis

cache = redis.Redis(host='localhost', port=6379, db=0)
TTL = 3600  # segundos


def consultar_evento(id_evento):
    clave = f"evento:{id_evento}"
    if cache.exists(clave):
        cache.incr(f"{clave}:hits", 1)
        return cache.get(clave).decode(), "HIT"
    else:
        df = ejecutar_query("SELECT * FROM eventos WHERE id = %s", (id_evento,))
        if not df.empty:
            valor = df.to_json()
            cache.setex(clave, TTL, valor)
            return valor, "MISS"
        return None, "MISS"

def obtener_eventos_unicos(n):
    df = ejecutar_query("SELECT id FROM eventos ORDER BY RAND() LIMIT %s", (n,))
    return df["id"].tolist()

def simular_distribucion(eventos, tipo, tasa, repeticiones):
    if tipo == "poisson":
        esperas = np.random.exponential(1 / tasa, repeticiones)
    else:
        esperas = np.random.exponential(tasa, repeticiones)

    historial = []
    efectividad = []
    hits = 0

    for i in range(repeticiones):
        id_evento = eventos[i % len(eventos)]
        resultado, estado = consultar_evento(id_evento)
        if estado == "HIT":
            hits += 1
        efectividad.append(hits / (i + 1))
        historial.append({
            "consulta": i + 1,
            "id": id_evento,
            "resultado": estado,
            "momento": datetime.now()
        })
        print(f"[{i+1}/{repeticiones}] Evento ID {id_evento}: {estado}")
        time.sleep(esperas[i])

    return pd.DataFrame(historial), efectividad

def simular_cache_y_exportar():
    print("\nSimulación de tráfico iniciada.")
    tipo = input("¿Tipo de distribución? [poisson/exponencial/ambas]: ").strip().lower()
    tasa = float(input("Tasa promedio (λ): "))
    repeticiones = int(input("Número total de consultas: "))
    eventos = obtener_eventos_unicos(10)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if tipo in ["poisson", "exponencial"]:
        df, curva = simular_distribucion(eventos, tipo, tasa, repeticiones)

        resumen = df["resultado"].value_counts()
        resumen.plot(kind="bar", color=["green", "red"])
        plt.title("Resumen de HITs y MISSes")
        plt.xlabel("Estado")
        plt.ylabel("Cantidad")
        plt.grid(axis="y", linestyle="--")
        nombre_img = f"resumen_cache_{timestamp}.png"
        plt.tight_layout()
        plt.savefig(nombre_img)
        plt.show()

        plt.figure()
        plt.plot(curva)
        plt.title("Curva de efectividad acumulada del caché")
        plt.xlabel("Consulta")
        plt.ylabel("Efectividad acumulada")
        plt.grid(True)
        nombre_img2 = f"curva_cache_{timestamp}.png"
        plt.tight_layout()
        plt.savefig(nombre_img2)
        plt.show()

        archivo = f"resultados_cache_{timestamp}.xlsx"
        df.to_excel(archivo, index=False)

        print(f"\nResultados exportados en: {archivo}")
        print(f"Gráfico resumen: {nombre_img}")
        print(f"Curva de efectividad: {nombre_img2}")

    elif tipo == "ambas":
        print("Simulando distribución Poisson...")
        df_poisson, curva_poisson = simular_distribucion(eventos, "poisson", tasa, repeticiones)

        print("Simulando distribución Exponencial...")
        df_expon, curva_expon = simular_distribucion(eventos, "exponencial", tasa, repeticiones)

        resumen_df = pd.DataFrame([
            {"Distribución": "Poisson", "Total": repeticiones,
             "Hits": sum(df_poisson["resultado"] == "HIT"),
             "Misses": sum(df_poisson["resultado"] == "MISS"),
             "Efectividad (%)": round(100 * sum(df_poisson["resultado"] == "HIT") / repeticiones, 2)},
            {"Distribución": "Exponencial", "Total": repeticiones,
             "Hits": sum(df_expon["resultado"] == "HIT"),
             "Misses": sum(df_expon["resultado"] == "MISS"),
             "Efectividad (%)": round(100 * sum(df_expon["resultado"] == "HIT") / repeticiones, 2)}
        ])

        archivo = f"resultados_cache_{timestamp}.xlsx"
        with pd.ExcelWriter(archivo) as writer:
            resumen_df.to_excel(writer, sheet_name="Resumen", index=False)
            df_poisson.to_excel(writer, sheet_name="Poisson", index=False)
            df_expon.to_excel(writer, sheet_name="Exponencial", index=False)

        plt.figure(figsize=(10, 5))
        plt.plot(curva_poisson, label="Poisson")
        plt.plot(curva_expon, label="Exponencial")
        plt.title("Curva de Efectividad del Caché")
        plt.xlabel("Consulta N°")
        plt.ylabel("Efectividad acumulada")
        plt.legend()
        plt.grid(True)
        nombre_img = f"curva_efectividad_comparada_{timestamp}.png"
        plt.tight_layout()
        plt.savefig(nombre_img)
        plt.show()

        print(f"\nResultados exportados en: {archivo}")
        print(f"Curva comparativa guardada como: {nombre_img}")

    else:
        print("Tipo de distribución no válido. Usa 'poisson', 'exponencial' o 'ambas'.")
