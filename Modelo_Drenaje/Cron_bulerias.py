import os
import numpy as np

# Generar lluvia operacional (pronostico y sin pronostico), escribir para swmm

os.system('python /media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/P_operacional.py')

print '---------------------------------------'
print '1. Lluvia operacional escrita para SWMM'
print '---------------------------------------'

# Crear proyecto y escribir archivos .ini (pronostico y sin pronostico)
                                                                                                                                   
os.system('python /media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/escribir_ini.py')
                                                                                                                                   
print '----------------------------------------------' 
print '2. Proyectos creados - archivos .ini generados'
print '----------------------------------------------' 
