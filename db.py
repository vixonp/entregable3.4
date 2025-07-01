import mysql.connector
import pandas as pd
import subprocess
from datetime import datetime

config = {
    "host": "localhost",
    "user": "usuario",
    "password": "pass123",
    "database": "eventosdb",
    "port": 3306
}

def conectar_mysql():
    return mysql.connector.connect(**config)

def ejecutar_query(sql, params=None):
    conn = conectar_mysql()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

def ejecutar_modificacion(sql, params=None):
    conn = conectar_mysql()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    conn.close()

def exportar_xlsx():
    df = ejecutar_query("SELECT * FROM eventos")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"eventos_mysql_export_{timestamp}.xlsx"
    df.to_excel(nombre_archivo, index=False)
    return nombre_archivo

def controlar_docker(accion="up"):
    comando = ["docker", "compose", accion, "-d"] if accion == "up" else ["docker", "compose", "down"]
    resultado = subprocess.run(comando, capture_output=True, text=True)
    return resultado.stdout if resultado.returncode == 0 else resultado.stderr

def eliminar_tabla_completa():
    conn = conectar_mysql()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tablas = [tabla[0] for tabla in cursor.fetchall()]
    if not tablas:
        print(" No hay tablas en la base de datos.")
        conn.close()
        return

    print("\n Tablas disponibles:")
    for idx, tabla in enumerate(tablas, 1):
        print(f"{idx}. {tabla}")

    try:
        eleccion = int(input("\nElige el número de la tabla que deseas eliminar: ").strip())
        if 1 <= eleccion <= len(tablas):
            tabla_a_eliminar = tablas[eleccion - 1]
            confirm = input(f"¿Seguro que quieres eliminar toda la tabla '{tabla_a_eliminar}'? [s/N]: ").strip().lower()
            if confirm == "s":
                cursor.execute(f"DROP TABLE `{tabla_a_eliminar}`")
                conn.commit()
                print(f" Tabla '{tabla_a_eliminar}' eliminada correctamente.")
            else:
                print(" Cancelado.")
        else:
            print("Número fuera de rango.")
    except ValueError:
        print("Entrada no válida.")
    conn.close()

def crear_tabla_eventos():
    conn = conectar_mysql()
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES LIKE 'eventos'")
    existe = cursor.fetchone()

    if not existe:
        cursor.execute("""
        CREATE TABLE eventos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tipo VARCHAR(255),
            descripcion TEXT,
            lat DOUBLE,
            lon DOUBLE,
            fecha_extraccion DATETIME,
            cuadrante VARCHAR(255),
            calle VARCHAR(255)
        );
        """)
        print(" Tabla 'eventos' creada correctamente.")
    else:
        cursor.execute("SHOW COLUMNS FROM eventos LIKE 'calle'")
        columna = cursor.fetchone()
        if not columna:
            cursor.execute("ALTER TABLE eventos ADD COLUMN calle VARCHAR(255)")
            print("Columna 'calle' añadida a la tabla 'eventos'.")
        else:
            print("Tabla 'eventos' ya existe y contiene la columna 'calle'.")

    conn.commit()
    cursor.close()
    conn.close()

def importar_csv_eventos():
    try:
        df = pd.read_csv("data/eventos.csv")
        columnas_necesarias = {"tipo", "descripcion", "lat", "lon", "fecha_extraccion", "cuadrante", "calle"}
        if not columnas_necesarias.issubset(set(df.columns)):
            print("El archivo CSV no tiene todas las columnas necesarias.")
            return

        conn = conectar_mysql()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO eventos (tipo, descripcion, lat, lon, fecha_extraccion, cuadrante, calle)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row["tipo"], row["descripcion"], row["lat"], row["lon"],
                row["fecha_extraccion"], row["cuadrante"], row["calle"]
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Se importaron {len(df)} eventos desde 'eventos.csv'.")
    except Exception as e:
        print("Error al importar CSV:", e)

def menu_mysql():
    print("MENÚ DE CONSULTAS Y GESTIÓN DE TABLA 'eventos'")
    print("1. Total de eventos")
    print("2. Eventos por tipo")
    print("3. Eventos por cuadrante")
    print("4. Eventos por fecha")
    print("5. Ver tabla completa (limit 50)")
    print("6. Ver eventos por tipo específico")
    print("7. Eliminar TODOS los eventos")
    print("8. Eliminar eventos por tipo")
    print("9. Eliminar eventos por cuadrante")
    print("10. Exportar a Excel")
    print("11. Iniciar Docker Compose")
    print("12. Detener Docker Compose")
    print("13. Eliminar tabla completa (elegir tabla)")
    print("14. Crear tabla eventos (verificar estructura)")
    print("15. Importar eventos desde eventos.csv")
    print("0. Salir")

    opcion = input("Elige una opción [0-15]: ").strip()

    if opcion == "1":
        df = ejecutar_query("SELECT COUNT(*) AS total_eventos FROM eventos")
        print(df)
    elif opcion == "2":
        df = ejecutar_query("""
            SELECT tipo, COUNT(*) AS cantidad FROM eventos GROUP BY tipo ORDER BY cantidad DESC
        """)
        print(df)
    elif opcion == "3":
        df = ejecutar_query("""
            SELECT cuadrante, COUNT(*) AS cantidad FROM eventos GROUP BY cuadrante ORDER BY cuadrante
        """)
        print(df)
    elif opcion == "4":
        df = ejecutar_query("""
            SELECT DATE(fecha_extraccion) AS fecha, COUNT(*) AS cantidad FROM eventos GROUP BY DATE(fecha_extraccion)
        """)
        print(df)
    elif opcion == "5":
        df = ejecutar_query("SELECT * FROM eventos LIMIT 50")
        print(df)
    elif opcion == "6":
        tipo = input("Tipo de evento a consultar: ").strip()
        df = ejecutar_query("SELECT * FROM eventos WHERE tipo = %s", (tipo,))
        print(df)
    elif opcion == "7":
        confirm = input("¿Eliminar TODOS los eventos? [s/N]: ").strip().lower()
        if confirm == "s":
            ejecutar_modificacion("DELETE FROM eventos")
            print(" Todos eliminados.")
    elif opcion == "8":
        tipo = input("Tipo a eliminar: ").strip()
        ejecutar_modificacion("DELETE FROM eventos WHERE tipo = %s", (tipo,))
        print(" Eliminados.")
    elif opcion == "9":
        cuadrante = input("Cuadrante a eliminar: ").strip()
        ejecutar_modificacion("DELETE FROM eventos WHERE cuadrante = %s", (cuadrante,))
        print(" Eliminados.")
    elif opcion == "10":
        archivo = exportar_xlsx()
        print(f"Exportado como: {archivo}")
    elif opcion == "11":
        print(controlar_docker("up"))
    elif opcion == "12":
        print(controlar_docker("down"))
    elif opcion == "13":
        eliminar_tabla_completa()
    elif opcion == "14":
        crear_tabla_eventos()
    elif opcion == "15":
        importar_csv_eventos()
    elif opcion == "0":
        print("Saliendo del menú.")
    else:
        print("Opción no válida.")
