
# coding: utf-8


import MySQLdb
import matplotlib
import matplotlib.pyplot as plt
import datetime
import numpy as np
import pandas as pd
import pylab as pl
from matplotlib import cm


# ## Consulta de lluvia radar


from wmf import wmf
cu = wmf.SimuBasin(rute='/media/nicolas/Home/nicolas/01_SIATA/nc_cuencas/Cuenca_AMVA_Barbosa_C.nc')
flag = 0


# Leer centroides de las subcuencas

from osgeo import ogr

ruta_puntos = '/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/centroides_poligonosWGS.shp'

dr = ogr.Open(ruta_puntos)
l = dr.GetLayer()
layerDef = l.GetLayerDefn()
ptos = []

for i in range(l.GetFeatureCount()):
    f = l.GetFeature(i)
    g = f.GetGeometryRef()
    pt = [g.GetX(),g.GetY()]
    ptos.append(pt)

ID_sub = []
for j in range(l.GetFeatureCount()):
    f = l.GetFeature(j)
    ID_sub.append(str(f.GetFieldAsString(2)))


# Funcion para leer lluvia radar

def Read_radar(ruta,fi,ff):
    serie = [] ; date = [] ; acum = np.zeros((cu.ncells))
    for i in ruta:
        date_file = datetime.datetime(int(i.split('/')[6][0:4]), int(i.split('/')[6][4:6]), int(i.split('/')[6][6:8]),
                                       int(i.split('/')[6][8:10]), int(i.split('/')[6][10:12]))

        if (date_file >= fi) and (date_file <= ff):
            g = netCDF4.Dataset(i)
            RadProp = [g.ncols, g.nrows, g.xll, g.yll, g.dx, g.dx]                        
            rvec = cu.Transform_Map2Basin(g.variables['Rain'][:].T / (12*1000), RadProp)
            serie.append(rvec) ; date.append(date_file)
            acum = acum + rvec
            g.close()
    
    return serie, date, acum



# Informacion radar para cada evento

import glob
import netCDF4
import cPickle

umb_R = 5

#datel = datetime.datetime.now()
datel = datetime.datetime(2017,4,8,18,30)
FH1 =  datel - datetime.timedelta(hours = 4) +  datetime.timedelta(hours=5) #Fecha inicial en UTC
FH2 =  datel + datetime.timedelta(hours=5) #Fecha inicial en UTC

mm1 = FH1.month ; mm2 = FH2.month

if mm1 < 10:
    mm1 = '0'+str(mm1)
mm1 = str(mm1)
if mm2 < 10:
    mm2 = '0'+str(mm2)
mm2 = str(mm2)

yy1 = str(FH1.year)+str(mm1)
yy2 = str(FH2.year)+str(mm2)

if yy1 != yy2: #El periodo abarca meses diferentes
    listado1 = glob.glob('/media/nicolas/Home/nicolas/101_RadarClass/'+yy1+'*')
    listado2 = glob.glob('/media/nicolas/Home/nicolas/101_RadarClass/'+yy2+'*')
    listado1.extend(listado2)
    ff = [datetime.datetime(int(i.split('/')[6][0:4]), int(i.split('/')[6][4:6]), int(i.split('/')[6][6:8]),
                            int(i.split('/')[6][8:10]), int(i.split('/')[6][10:12])) for i in listado1]
    cc = sorted(range(len(ff)), key=lambda k: ff[k],reverse=False) #fechas de atras hacia adelante
    z = Read_radar(np.array(listado1)[cc],FH1,FH2)

if yy1 == yy2: #El periodo esta en el mismo mes
    listado = glob.glob('/media/nicolas/Home/nicolas/101_RadarClass/'+yy1+'*')
    ff = [datetime.datetime(int(i.split('/')[6][0:4]), int(i.split('/')[6][4:6]), int(i.split('/')[6][6:8]), 
        int(i.split('/')[6][8:10]), int(i.split('/')[6][10:12])) for i in listado]
    cc = sorted(range(len(ff)), key=lambda k: ff[k],reverse=False) #fechas de atras hacia adelante
    z = Read_radar(np.array(listado)[cc],FH1,FH2)


# Extraer serie para los centroides de las subcuencas (aferentes a los nodos)
mapa = z[2]
P_zona = []

for co in range(len(ptos)):
    xy = ptos[co]
    var_cel = wmf.cu.basin_extract_var_by_point(cu.structure,mapa,xy,3,1,cu.ncells)
    P_zona.append(var_cel[0])

acum_R = max(P_zona)
por_datos = len(z[1])/49.

# Evaluar si el evento cumple para ser con radar

if (max(P_zona) > umb_R) and (len(z[1])/49. > 0.6):
    print FH2

    # Serie de fechas con fecuencia exacta de 10 minutos
    fec_dd = FH1 - datetime.timedelta(hours=5)
    fec_ee = FH2 - datetime.timedelta(hours=5)
    fec_freq = [] 
    fec_freq.append(fec_dd)

    while fec_dd < fec_ee:
        fec_freq.append(fec_dd + datetime.timedelta(minutes=10))
        fec_dd = fec_dd + datetime.timedelta(minutes=10)
    fec_freq = fec_freq[0:-1]

    # Unificar fechas en la resolucion 10 min
    fecha = []
    for f in z[1]:
        fecha.append(f-datetime.timedelta(hours=5))

    pos_lluvia = []
    for j in fec_freq:
        aa = np.where((np.array(fecha) >= j-datetime.timedelta(minutes=5)) & (np.array(fecha) <= j+datetime.timedelta(minutes=5)))[0]
        try:
            pos_lluvia.append(aa[0])
        except:
            pos_lluvia.append(999)

    serie = z[0]
    file_salida1 = [] ; P_freq = [] ; id_sub1 = [] ; fecc1 = []

    for co in range(len(ptos)):
        xy = ptos[co]
        for ma in range(len(fec_freq)):
            if pos_lluvia[ma] != 999:
                mapa1 = serie[pos_lluvia[ma]]
                var_cel = wmf.cu.basin_extract_var_by_point(cu.structure,mapa1,xy,3,1,cu.ncells)
                file_salida1.append([ID_sub[co],fec_freq[ma],var_cel[0]])
                P_freq.append(var_cel[0]) ; id_sub1.append(ID_sub[co]) ; fecc1.append(fec_freq[ma])
            if pos_lluvia[ma] == 999:
                file_salida1.append([ID_sub[co],fec_freq[ma],0])
                P_freq.append(0) ; id_sub1.append(ID_sub[co]) ; fecc1.append(fec_freq[ma])

    anno = fec_dd.year ; mes = fec_dd.month ; dia = fec_dd.day
    anno_str = str(anno) ; mes_str = str(mes) ; dia_str = str(dia)
    if mes < 10:
        mes_str = '0'+str(mes)
    if dia < 10:
        dia_str = '0'+str(dia)

    fef_str = anno_str+'-'+mes_str+'-'+dia_str

    flag = 1

    print '----------------------------'
    print FH2
    print 'Consulta de lluvia con radar'
    print '----------------------------'

if (max(P_zona) < umb_R) or (len(z[1])/49. < 0.6):
    print '------------------------'
    print FH2
    print 'Evento pequeño con radar'
    print '------------------------'


# ## Consulta de lluvia pluviometros

n=116
p=(54,28)
ctec=['2014-03-06 15:55','2014-03-04 14:31']
dt_ctec = []

for x in ctec:
    dt_ctec.append(datetime.datetime(int(x[0:4]),int(x[5:7]),int(x[8:10]),int(x[11:13]),int(x[14:16])))

# open database connection
host      ='192.168.1.74'
user      ='usrCalidad'
passw     ='aF05wnXC;'
bd_nombre ='siata'

Estaciones="SELECT Codigo,Nombreestacion, offsetN, red  FROM estaciones WHERE codigo=("+str(n)+")"
db = MySQLdb.connect(host, user,passw,bd_nombre)
db_cursor = db.cursor()
db_cursor.execute(Estaciones)
Cod = db_cursor.fetchall()


# Funcion para consultar precipitacion en los pluviometros

def consultaP(fecha,rezago):
    
    cl = []
    P = []
    dat = []

    date_ini=fecha-datetime.timedelta(minutes=rezago)
    date_fin=fecha
    datos_p = "SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P1/1000,P2/1000, calidad FROM datos WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"+" UNION SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P,P, calidad FROM tramas WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"                                                                                                                                                                                                                                                                                                                                                                                                                                                             

    db = MySQLdb.connect(host, user,passw,bd_nombre)
    db_cursor = db.cursor()
    db_cursor.execute(datos_p)
    data = db_cursor.fetchall()

    for dato in data:
        if dato[7] == 1:
            P.append((dato[5]+dato[6])/2)
            cl.append(dato[4])
            hora = int(str(dato[3]).split(':')[0])
            minu = int(str(dato[3]).split(':')[1])
            da = datetime.datetime(dato[0].year,dato[0].month,dato[0].day,hora,minu)
            dat.append(da)
        if dato[7] == 1511:
            P.append(dato[6])
            cl.append(dato[4])
            hora = int(str(dato[3]).split(':')[0])
            minu = int(str(dato[3]).split(':')[1])
            da = datetime.datetime(dato[0].year,dato[0].month,dato[0].day,hora,minu)
            dat.append(da)
        if dato[7] == 1512:
            P.append(dato[5])
            cl.append(dato[4])
            hora = int(str(dato[3]).split(':')[0])
            minu = int(str(dato[3]).split(':')[1])
            da = datetime.datetime(dato[0].year,dato[0].month,dato[0].day,hora,minu)
            dat.append(da)

    return cl,P,date_ini,date_fin,dat



# Si en la consulta de radar no hay evento procede con consulta de pluvios

#-------------Hallar pesos de cada subcuenca para IDW--------------
pto_est = np.array([[-75.597, 6.235], [-75.605, 6.248]]) # [54, 28]
x_ptos = [] ; y_ptos = []
for j in ptos:
	x_ptos.append(j[0])
	y_ptos.append(j[1])
ws = []
for est in range(len(p)):
	dist = (((x_ptos-pto_est[est][0])*100000)**2 + ((y_ptos-pto_est[est][1])*100000)**2)**(1./2)
	ws.append(1./(dist**2))
#------------------------------------------------------------------


if flag == 0:

    umb_P = 5

    file_salida2 = [] ; id_sub2 = [] ; fecc2 = []

    # Serie de lluvia con fecuencia exacta de 10 minutos
    fec_dd = FH2 - datetime.timedelta(hours=4) - datetime.timedelta(hours=5) # Devolver a hora local
    fec_ee = FH2 - datetime.timedelta(hours=5) # Devolver a hora local
    fec_freq = []
    fec_freq.append(fec_dd)

    while fec_dd < fec_ee:
        fec_freq.append(fec_dd + datetime.timedelta(minutes=10))
        fec_dd = fec_dd + datetime.timedelta(minutes=10)

    a = consultaP(fec_ee,240)
    P_freq = np.zeros((len(fec_freq),len(p)))

    for ff in range(len(fec_freq)):
        fff = fec_freq[ff]
        f1 = fff - datetime.timedelta(minutes=5)
        f2 = fff + datetime.timedelta(minutes=5)
        for est in range(len(p)):
            hh = np.where((np.array(a[4]) >= f1) & (np.array(a[4]) <= f2) & (np.array(a[0]) == p[est]))[0]
            P_freq[ff][est] = np.sum(np.array(a[1])[hh])

    max1 = np.sum(P_freq[:,0]) # Estacion 1 en frecuencia 10 min 
    max2 = np.sum(P_freq[:,1]) # Estacion 2 en frecuencia 10 min

    if max(max1,max2) > umb_P:
        print fec_ee

        for w in range(len(ID_sub)): # Estimar lluvia en cada subcuenca utilizando IDW
            P_sub_freq = (P_freq[:,0]*ws[0][w] + P_freq[:,1]*ws[1][w])/(ws[0][w]+ws[1][w])

            for g in range(len(fec_freq)):
                file_salida2.append([ID_sub[w],fec_freq[g],P_sub_freq[g]])
                id_sub2.append(ID_sub[w]) ; fecc2.append(fec_freq[g])

        anno = FH2.year ; mes = FH2.month ; dia = FH2.day
        anno_str = str(anno) ; mes_str = str(mes) ; dia_str = str(dia)
        if mes < 10:
            mes_str = '0'+str(mes)
        if dia < 10:
            dia_str = '0'+str(dia)

        fef_str = anno_str+'-'+mes_str+'-'+dia_str
        flag = 2
	print '-----------------------------------'
	print FH2
	print 'Consulta de lluvia con pluviometros'
	print '-----------------------------------'

    if max(max1,max2) < umb_P:
	print '-------------------------------'
        print FH2
        print 'Evento pequeño con pluviometros'
	print '-------------------------------'    


import pickle

# Test lectura archivo de pronostico

f = open('/media/nicolas/Home/Jupyter/Esneider/Lluvia_operacional/Salidas/_cast_alto.rain','r')
cast_alto = pickle.load(f)
f.close()

f = open('/media/nicolas/Home/Jupyter/Esneider/Lluvia_operacional/Salidas/_cast_normal.rain','r')
cast_normal = pickle.load(f)
f.close()

f = open('/media/nicolas/Home/Jupyter/Esneider/Lluvia_operacional/Salidas/_cast_bajo.rain','r')
cast_bajo = pickle.load(f)
f.close()


est_bulerias = ['54','28']
print 'Fecha generacion: '+str(cast_alto.index[0])
registros_P = {}

for i in est_bulerias:
    
    registros_P[i] = {}
    registros_P[i]['ca'] = cast_alto[i].values
    registros_P[i]['cn'] = cast_normal[i].values
    registros_P[i]['cb'] = cast_bajo[i].values


# ## Unir series registradas y pronosticadas de pluviometro

if flag != 0:
    
    fe_pro = cast_alto['54'].index
    fe_pro_date = [k.to_pydatetime() for k in fe_pro]

    # Serie de lluvia con fecuencia exacta de 10 minutos
    fec_dd = fec_freq[-1::][0]
    fec_ee = fec_dd + datetime.timedelta(hours=1) #fe_pro[-1::][0]
    fec_freq1 = []

    while fec_dd < fec_ee:
        fec_freq1.append(fec_dd + datetime.timedelta(minutes=10))
        fec_dd = fec_dd + datetime.timedelta(minutes=10)

    P_freq1 = np.zeros((len(fec_freq1),len(est_bulerias)))

    for ff in range(len(fec_freq1)):
        fff = fec_freq1[ff]
        f1 = fff - datetime.timedelta(minutes=5)
        f2 = fff + datetime.timedelta(minutes=5)
        for est in range(len(est_bulerias)):

            pe_pro = registros_P[est_bulerias[est]]['ca']
            hh = np.where((np.array(fe_pro_date) >= f1) & (np.array(fe_pro_date) <= f2))[0]
            P_freq1[ff][est] = np.sum(np.array(pe_pro)[hh])
    
    # Estimar lluvia en cada subcuenca utilizando IDW
    file_pronostico = [] ; id_sub3 = []
    for w in range(len(ID_sub)): 
        P_sub_freq = (P_freq1[:,0]*ws[0][w] + P_freq1[:,1]*ws[1][w])/(ws[0][w]+ws[1][w])

        for g in range(len(fec_freq1)):
            file_pronostico.append([ID_sub[w],fec_freq1[g],P_sub_freq[g]])
            id_sub3.append(ID_sub[w])
    
    fecha_corte = FH2 - datetime.timedelta(hours=5) - datetime.timedelta(hours=3.01)
    
    if flag == 1:
	file_final = []
	print '----------------'
        print 'Evento con radar'
	print '----------------'

        file_base = file_salida1
        file_pronostico
        
        for k in ID_sub:
            bb = np.where((np.array(id_sub1) == k) & (np.array(fecc1) >= fecha_corte))[0]
            cc = np.where(np.array(id_sub3) == k)[0]

	    for b in bb:
	    	file_final.append(file_base[b])

	    for c in cc:
	    	file_final.append(file_pronostico[c])
    
	f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm_cpp.bin','w')
	cPickle.dump(file_final,f)
	f.close()    

    if flag == 2:
	file_final = []
	print '----------------------'
        print 'Evento con pluviometro'
	print '----------------------'

        file_base = file_salida2
        
        for k in ID_sub:
            bb = np.where((np.array(id_sub2) == k) & (np.array(fecc2) >= fecha_corte))[0]
            cc = np.where(np.array(id_sub3) == k)[0]
            
            for b in bb:
                file_final.append(file_base[b])
            
            for c in cc:
                file_final.append(file_pronostico[c])

    	f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm_cpp.bin','w')
    	cPickle.dump(file_final,f)
    	f.close()


# ## Serie en tiempo real (sin pronostico)

import os

# Decidir entre Radar y Pluviometros

# flag = 1 : Radar flag = 2 :Pluvio

if flag == 1: 
    file_final = file_salida1
    
if flag == 2:
    file_final = file_salida2
    
if flag == 0:
    os.system('rm /media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm*.bin')
    print '--------------------'
    print 'Sin evento de lluvia'
    

if flag != 0:
    f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm_sp.bin','w')
    cPickle.dump(file_final,f)
    f.close()

print '------------------'
print 'flag = '+str(flag)


# Definir acumulados para modelo estadistico

print 'Definiendo modelo estadistico'

# Cargar reglas de pronostico con acumulado
f = open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/reglas_saturacion.bin','r') 
reglas_pro = pickle.load(f) 
f.close()

if flag == 1:
    acum_tiempo_real = [np.percentile(P_zona,50), np.max(P_zona)] # Radar
    acum_pronostico = [np.sum(P_freq1[:,0]), np.sum(P_freq1[:,1])] # Pronosticado pluvio
    acum_pro1hora = acum_tiempo_real + np.mean(acum_pronostico)

    print 'Evento con radar'
    print '------------------------'
    print 'Acumulado en tiempo real'
    print acum_tiempo_real
    print 'Acumulado pronosticado'
    print acum_pro1hora

if flag == 2:
    acum_tiempo_real = [np.sum(P_freq[:,0]), np.sum(P_freq[:,1])] # Pluvios
    acum_pronostico = [np.sum(P_freq1[:,0]), np.sum(P_freq1[:,1])] # Pronosticado pluvio
    acum_pro1hora = acum_tiempo_real + np.array(acum_pronostico)

    print 'Evento con pluviometros'
    print '------------------------'
    print 'Acumulado en tiempo real'
    acum_tr = max(acum_tiempo_real)
    print acum_tr
    print 'Acumulado pronosticado'
    acum_p1h = max(acum_pro1hora)
    print acum_p1h

if flag != 0:
    # Decidir a que grupo pertenece
    g1_inf = np.percentile(reglas_pro['G1']['acum'],10) ; g1_sup = np.percentile(reglas_pro['G1']['acum'],90)
    g2_inf = np.percentile(reglas_pro['G2']['acum'],10) ; g2_sup = np.percentile(reglas_pro['G2']['acum'],90)
    g3_inf = np.percentile(reglas_pro['G3']['acum'],10) ; g3_sup = np.percentile(reglas_pro['G3']['acum'],90)

    # Evaluar tiempo real
    if (acum_tr >= g1_inf) and (acum_tr <= g1_sup):
        gru_tr = 'Grupo1' ; dif = abs(reglas_pro['G1']['acum'] - acum_tr)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G1']['mat'] ; satur_tr = mat[dd]

    if (acum_tr >= g2_inf) and (acum_tr <= g2_sup):
        gru_tr = 'Grupo2' ; dif = abs(reglas_pro['G2']['acum'] - acum_tr)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G2']['mat'] ; satur_tr = mat[dd]

    if (acum_tr >= g3_inf) and (acum_tr <= g3_sup):
        gru_tr = 'Grupo3' ; dif = abs(reglas_pro['G3']['acum'] - acum_tr)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G3']['mat'] ; satur_tr = mat[dd]

    if (acum_tr < g3_inf):
        gru_tr = 'Grupo3' ; dif = abs(reglas_pro['G3']['acum'] - acum_tr)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G3']['mat'] ; satur_tr = mat[dd]

    if (acum_tr > g1_sup):
        gru_tr = 'Grupo1' ; dif = abs(reglas_pro['G1']['acum'] - acum_tr)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G1']['mat'] ; satur_tr = mat[dd]

    # Evaluar pronostico 1 hora
    if (acum_p1h >= g1_inf) and (acum_p1h <= g1_sup):
    	gru_p1 = 'Grupo1' ; dif = abs(reglas_pro['G1']['acum'] - acum_p1h)
	dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G1']['mat'] ; satur_p1 = mat[dd]

    if (acum_p1h >= g2_inf) and (acum_p1h <= g2_sup):
        gru_p1 = 'Grupo2' ; dif = abs(reglas_pro['G2']['acum'] - acum_p1h)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G2']['mat'] ; satur_p1 = mat[dd]

    if (acum_p1h >= g3_inf) and (acum_p1h <= g3_sup):
        gru_p1 = 'Grupo3' ; dif = abs(reglas_pro['G3']['acum'] - acum_p1h)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G3']['mat'] ; satur_p1 = mat[dd]

    if (acum_p1h < g3_inf):
        gru_p1 = 'Grupo3' ; dif = abs(reglas_pro['G3']['acum'] - acum_p1h)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G3']['mat'] ; satur_p1 = mat[dd]

    if (acum_p1h > g1_sup):
        gru_p1 = 'Grupo1' ; dif = abs(reglas_pro['G1']['acum'] - acum_p1h)
        dd = np.where(np.array(dif) == np.min(dif))[0]
        mat = reglas_pro['G1']['mat'] ; satur_p1 = mat[dd] 

if flag == 0:
    satur_tr = np.zeros((len(reglas_pro['G1']['ejex'])))
    satur_p1 = np.zeros((len(reglas_pro['G1']['ejex'])))

print '-------Saturacion tiempo real---------------------'
print satur_tr
print '-------Saturacion pronostico pluvio---------------'
print satur_p1
print '--------------------------------------------------'
