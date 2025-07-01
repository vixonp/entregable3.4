import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Lista de dashboards personalizados con título y URL completa
DASHBOARDS = [
    ("Top eventos por tipo", "http://localhost:5601/app/dashboards#/view/1ef18850-54fd-11f0-93c1-67094a36688c"),
    ("Eventos por hora", "http://localhost:5601/app/dashboards#/view/48ab9ce0-5501-11f0-93c1-67094a36688c"),
    ("Comparativa Hits vs Misses", "http://localhost:5601/app/dashboards#/view/e6480db0-5503-11f0-93c1-67094a36688c"),
]

def ver_dashboards_kibana():
    print("\nDashboards disponibles:")
    for i, (nombre, _) in enumerate(DASHBOARDS, 1):
        print(f"{i}. {nombre}")

    try:
        seleccion = int(input("Selecciona un dashboard para abrir [número]: "))
        if 1 <= seleccion <= len(DASHBOARDS):
            nombre, url = DASHBOARDS[seleccion - 1]
            print(f"\nAbriendo: {nombre}")

            options = Options()
            options.add_argument("--start-maximized")
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(10)  # espera que se cargue Kibana

            input("Presiona ENTER para cerrar el navegador...")
            driver.quit()
        else:
            print("Número fuera de rango.")
    except ValueError:
        print("Entrada inválida.")
