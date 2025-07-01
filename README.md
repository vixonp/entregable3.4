# Proyecto Sistemas Distribuidos - Entrega 3: Integración Elastic/Kibana
**Autores**: Lucas Vicuña, Vicente Silva

**Profesor**: Nicolás Hidalgo

---

## Descripción General

Esta segunda entrega expande el sistema distribuido de análisis de eventos de tráfico mediante el uso de **Apache Pig** como herramienta de procesamiento paralelo. Se incorporan nuevos análisis agrupando eventos por **tipo y calle** y por **hora de ocurrencia**, con visualizaciones generadas automáticamente.

## Tecnologías utilizadas

- **Python 3.8+**
- **Apache Pig** (procesamiento paralelo)
- **Apache Hadoop** (HDFS + YARN)
- **Docker** (contenedores)
- **MySQL** (base de datos)
- **Redis** (caché)
- **Selenium** (web scraping)
- **Pandas** (manipulación de datos)
- **Matplotlib** (visualizaciones)
- **elasticSearch (visualización)

## Instalación y ejecución

1. **Clonar el repositorio:**
   ```bash
   git clone <url-del-repositorio>
   cd entregable3.4
   ```

2. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Levantar servicios con Docker:**
   ```bash
   docker-compose up -d
   ```

Se levantarán:
- **MySQL** (puerto 3306)
- **Redis** (puerto 6379)
- **Hadoop + Pig** (contenedor personalizado)

4. **Ejecutar el sistema:**
   ```bash
   python main.py
   ```

Desde el menú podrás:
- Extraer datos de Waze
- Consultar base de datos
- Simular tráfico y evaluar caché
- Ejecutar procesamiento Pig
- Ver resultados y gráficos

## Procesamiento con Apache Pig

Se ejecutan automáticamente dos scripts Pig:

1. **Agrupación por tipo y calle** (`test.pig`)
2. **Agrupación por hora del día** (`test_hora.pig`)

Los gráficos resultantes son:
- `distribucion_tipos_evento.png`
- `top_eventos_por_tipo.png`
- `eventos_por_hora.png`
- `evolucion_eventos_por_hora.png`

## Estructura del Repositorio

```
entregable3.4/
├── main.py                     # Menú principal del sistema
├── scraper.py                  # Web scraping de Waze
├── db.py                       # Conexión y consultas a MySQL
├── trafico.py                  # Simulación de tráfico y caché
├── funciones.py                # Gráficos y visualización
├── pig_processor.py            # Ejecución de scripts Pig
├── data/
│   ├── eventos.csv / eventos.xlsx
│   ├── output_tipo_calle.xlsx
│   ├── output_por_hora.xlsx
├── informe/
│   └── main.tex                # Informe en LaTeX
├── docker-compose.yml
├── Dockerfile.pig
├── README.md
└── requirements.txt
```

## Resultados de la Entrega 2

| Agrupación     | Archivo Excel                  | Imagen generada                   |
|----------------|--------------------------------|-----------------------------------|
| Tipo + Calle   | `output_tipo_calle.xlsx`       | `distribucion_tipos_evento.png`   |
| Hora del día   | `output_por_hora.xlsx`         | `evolucion_eventos_por_hora.png`  |

> Ambos análisis permiten visualizar tendencias de tráfico en distintos contextos.

## Enlaces

- [Apache Pig Documentation](https://pig.apache.org/docs/latest/)
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/current/)
- [Docker Documentation](https://docs.docker.com/)

## Notas

- Asegúrate de tener Docker instalado y corriendo
- Los contenedores pueden tardar varios minutos en iniciarse completamente
- Verifica que los puertos 3306 y 6379 estén disponibles
