import multiprocessing
import subprocess


def ejecutar_script(script):
    subprocess.run(script, shell=True)


# Lista de scripts a ejecutar en paralelo
scripts = ["FASE1_1_ALISTAMIENTO_ICMP_P2.py",
           "FASE1_1_ALISTAMIENTO_FGN_INACTIVOS_P2.py",
           "FASE1_1_ALISTAMIENTO_CNMH_SE_P2.py"]

# Crear un grupo de procesos
with multiprocessing.Pool() as pool:
    # Ejecutar los scripts en paralelo
    pool.map(ejecutar_script, scripts)

import multiprocessing as mp
print("Number of processors: ", mp.cpu_count())