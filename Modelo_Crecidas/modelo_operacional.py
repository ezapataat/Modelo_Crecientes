import MySQLdb 
import matplotlib
matplotlib.use('Agg')
matplotlib.use ('template')
import matplotlib.pyplot as plt 
import datetime 
import numpy as np 
import pandas as pd 
import pylab as pl 
import sys, getopt
import time
import os
import cPickle
from matplotlib.dates import DateFormatter


#-------------------------------------------------------------
#  1. Modelo estadistico (en base a acumualdos de lluvia)
#-------------------------------------------------------------
#print vvv

nivel = [92,106,108,115,96,101,116,134,182,109,155,161,166,169,152,104,90]
estaciones = [106,179,91,94,93,99,140]
precipitacion = [(1,8,29),(33,58,57,171),(18,3,25),(52,64,15),(20,48),(5,56,26,49),(54,9,17,29),(89,48,14,121),(5,46,129,44,42),(64,65,34),(37,14,21,51),(38,62,36),(8,29,1),(57,58,171),(9,29,17),(15,56),(3,43,18,25)]
rezagos = [120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120] 
offset = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] 
tipos = ['Ni','Ni','Ni','Ni','Ni','pr','pr','pr','Ni','pr','pr','ni','pr','pr','pr','pr','pr']
lim_pos = [(0,200,141,151),(0,180,161,171),(50,320,276,291),(0,200,171,181),(0,140,111,121),(0,300,271,281),(30,240,211,221),(30,240,211,221),(50,300,276,291),(20,160,140,150),(20,160,125,135),(0,80,62,67),(80,200,167,173),(0,100,85,90),(0,100,85,90),(0,100,85,90),(0,150,85,90)] # (yinf, ysup, pos leyenda1, pos leyenda2)
umbral_lluvia = 6

host ='192.168.1.74'
user ='usrCalidad'
passw ='aF05wnXC;'# Consultar niveles de alerta
bd_nombre ='siata'

total_estaciones = list(np.copy(nivel))
total_estaciones.extend(estaciones)
niveles_alerta = []

for niv_ale in total_estaciones:

	niva = "SELECT action_level,minor_flooding,moderate_flooding,major_flooding  FROM estaciones WHERE codigo=("+str(niv_ale)+")"
	db = MySQLdb.connect(host, user,passw,bd_nombre)
	db_cursor = db.cursor()
	db_cursor.execute(niva)
	niv_ale = db_cursor.fetchall()
	niveles_alerta.append(niv_ale[0])


pronostico_txt = [] 
for kk in range(len(nivel)):
    p=precipitacion[kk]
    n=nivel[kk]
    flag = 0
    # open database connection
    Estaciones="SELECT Codigo,Nombreestacion, offsetN, bancallena red FROM estaciones WHERE codigo=("+str(n)+")"
    db = MySQLdb.connect(host, user,passw,bd_nombre)
    db_cursor = db.cursor()
    db_cursor.execute(Estaciones)
    Cod = db_cursor.fetchall()
    localdate=time.localtime(time.time())
    annoi=localdate[0] ; mesi=localdate[1] ; diai=localdate[2] ; horai=localdate[3] ; minui=localdate[4]
    datel=datetime.datetime(int(annoi),int(mesi),int(diai),int(horai),int(minui))
    #datel=datetime.datetime(2018,1,8,19,40)

    def consultaN(dt_fecha,n,red):
        date_fin=dt_fecha
        date_ini=dt_fecha-datetime.timedelta(minutes=120)
        datos_n = "SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s'), ("+str(Cod[0][2])+"-"+red+"), calidad FROM datos WHERE cliente = "+str(n)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"
        db = MySQLdb.connect(host, user,passw,bd_nombre)
        db_cursor = db.cursor()
        db_cursor.execute(datos_n)
        data = db_cursor.fetchall()
        N=[] ; fec=[]
        for dato in data:
            if (dato[5] == 1) and (dato[4] != 1524):
                N.append(dato[4])
                fec.append(dato)
                
        return N,fec

    def consultaP(fecha,rezago,est,tipo):
        cl = []
        P = []
        cal = []
        if tipo == 'ha':
            date_ini=fecha-datetime.timedelta(hours=rezago)
            date_fin=fecha
            datos_p = "SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P1/1000,P2/1000, calidad FROM datos WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"+" UNION SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P,P, calidad FROM tramas WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"
        if tipo == 'pe':
            date_ini=fecha-datetime.timedelta(minutes=rezago)
            date_fin=fecha
            datos_p = "SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P1/1000,P2/1000, calidad FROM datos WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"+" UNION SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s') , Cliente, P,P, calidad FROM tramas WHERE cliente IN "+str(p)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"
        db = MySQLdb.connect(host, user,passw,bd_nombre)
        db_cursor = db.cursor()
        db_cursor.execute(datos_p)
        data = db_cursor.fetchall()

        for dato in data:
            if dato[7] == 1:
                P.append(max(dato[5],dato[6]))
                cl.append(dato[4])
            if dato[7] == 1511:
                P.append(dato[6])
                cl.append(dato[4])
            if dato[7] == 1512:
                P.append(dato[5])
                cl.append(dato[4])
        return cl,P

    def maximo_total(con_ha,con_pe):
        mha=0.
        mpe=0.
        for j in p:
            aa=np.where(np.array(con_ha[0]) == j)[0]
            maa = np.sum(np.array(con_ha[1])[aa])
            cc=np.where(np.array(con_pe[0]) == j)[0]
            mcc = np.sum(np.array(con_pe[1])[cc])
            if maa > mha:
                mha=maa
            if mcc > mpe:
                mpe=mcc
        return mha,mpe

    con_ha=consultaP(datel,72,0,'ha')
    con_pe=consultaP(datel,rezagos[kk],0,'pe')

    try:
        con_niv=consultaN(datel-datetime.timedelta(minutes=5),n,tipos[kk])
        Np=con_niv[0] ; Fp=con_niv[1]
    except:
        Np=np.ones((120))*0
	Fp_date = [datel]
	for delt in range(119):
		flag = 1
		Fp_date.append(Fp_date[delt]+datetime.timedelta(minutes=1))

    if len(Np) == 0:
        Np=np.ones((120))*0
	Fp_date = [datel]
	for delt in range(119):
		flag = 1
		Fp_date.append(Fp_date[delt]+datetime.timedelta(minutes=1))

    ha=maximo_total(con_ha,con_pe)[0]
    pe=maximo_total(con_ha,con_pe)[1]
    mod='3'
    tiempos=[5,10,15,20,25,30]

    if flag == 0:
    	# Volver date las fechas
	Fp_date = []
	for ff in Fp:
		yy = ff[1].split('-')[0] ; mm = ff[1].split('-')[1] ; dd = ff[1].split('-')[2]
		hh = ff[3].split(':')[0] ; minn = ff[3].split(':')[1]
		Fp_date.append(datetime.datetime(int(yy),int(mm),int(dd),int(hh),int(minn)))

    nivp=np.genfromtxt('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/files/niveles'+str(n)+'.txt',dtype=float)
    rango_ha=np.genfromtxt('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/files/rangos_ha'+str(n)+'.txt',dtype=float)
    rango_pe=np.genfromtxt('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/files/rangos_pe'+str(n)+'.txt',dtype=float)
    hum_ant=np.genfromtxt('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/files/acum_antecedente_3dias'+str(n)+'.txt',dtype=float)
    acum_rez=np.genfromtxt('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/files/acum_rezagado'+str(n)+'.txt',dtype=float)
    hum = hum_ant[:,0]
    nivp=nivp+offset[kk]

    def pronosticar1(ha,pe,t):
        clases_ha = 3
        clases_pe = 6
        r_ha=0 ; r_pe=0
        binxa = (100-np.min(hum))/clases_ha
        ejexa = np.arange(np.min(hum),100+binxa, binxa)
        a = list(ejexa) ; a.append(np.max(hum))
        ejexa = a
        binx = (np.max(acum_rez[:,:])-np.min(acum_rez[:,:]))/clases_pe
        ejex = np.arange(np.min(acum_rez[:,:]),np.max(acum_rez[:,:])+binx, binx)
        for i in range(clases_ha+1):
            if (ha >= ejexa[i]) and (ha <= ejexa[i+1]):
                r_ha=i+1
        for k in range(clases_pe):
            if (pe >= ejex[k]) and (pe <= ejex[k+1]):
                r_pe=k+1
        niv_f = []
        for j in range(len(nivp)):
            if (rango_ha[j] == r_ha) and (rango_pe[j,(t/5)-1] == r_pe):
                niv_f.append(nivp[j])
        
        if len(niv_f)>=1:
            return np.percentile(niv_f,5),np.percentile(niv_f,20),np.percentile(niv_f,35),np.percentile(niv_f,50),np.percentile(niv_f,65),np.percentile(niv_f,80),np.percentile(niv_f,95)
        if len(niv_f)<1:
            return 0

    def pronosticar2(pe,t):
        clases_ha = 3
        clases_pe = 6
        r_pe=0
        binx = (np.max(acum_rez[:,:])-np.min(acum_rez[:,:]))/clases_pe
        ejex = np.arange(np.min(acum_rez[:,:]),np.max(acum_rez[:,:])+binx, binx)
        for k in range(clases_pe):
            if (pe >= ejex[k]) and (pe <= ejex[k+1]):
                r_pe=k+1
        niv_f = []
        for j in range(len(nivp)):
            if (rango_pe[j,(t/5)-1] == r_pe):
                niv_f.append(nivp[j])
        if len(niv_f)>=2:
            return np.percentile(niv_f,5),np.percentile(niv_f,20),np.percentile(niv_f,35),np.percentile(niv_f,50),np.percentile(niv_f,65),np.percentile(niv_f,80),np.percentile(niv_f,95)
        if len(niv_f)<2:
            return 0

    #Validar pronostico con el historico de eventos
    per = [10,25,40,50,60,75,90]
    plt.close('all')
    p10=[] ; p10.append(Np[len(Np)-1])
    p25=[] ; p25.append(Np[len(Np)-1])
    p40=[] ; p40.append(Np[len(Np)-1])
    p50=[] ; p50.append(Np[len(Np)-1])
    p60=[] ; p60.append(Np[len(Np)-1])
    p75=[] ; p75.append(Np[len(Np)-1])
    p90=[] ; p90.append(Np[len(Np)-1])
    
    fig=pl.figure(facecolor='w',edgecolor='w',figsize=(12,9))
    formatter = DateFormatter ('%H:%M')

    ti_plot = []
    ti_plot = [Fp_date[len(Fp_date)-1]]
    for delt in range(6):
    	ti_plot.append(ti_plot[delt]+datetime.timedelta(minutes=5))

    for ti in tiempos:
        if mod == '1':
            salida=np.zeros((7,2))
            salida[:,0]=per
            pp1 = pronosticar1(ha,pe,ti)
            if pp1 == 0:
                print '------------------------------------------'
                print 'No hay datos suficientes para este modelo '
                print '------------------------------------------'
                mod = '3'
            if pp1 != 0:
                salida[:,1]=pp1
        if mod == '2':
            salida=np.zeros((7,2))
            salida[:,0]=per
            pp2 = pronosticar2(pe,ti)
            salida[:,1]=pp2
        if mod == '3':
            salida=np.zeros((7,3))
            salida[:,0]=per
            pp1 = pronosticar1(ha,pe,ti)
            pp2 = pronosticar2(pe,ti)
            salida[:,1]=pp1
            salida[:,2]=pp2
        
        if pp2 != 0:
            p10.append(salida[:,2][0]) ; p25.append(salida[:,2][1]) ; p40.append(salida[:,2][2]) ; p50.append(salida[:,2][3]) ; p60.append(salida[:,2][4]) ; p75.append(salida[:,2][5]) ; p90.append(salida[:,2][6])
        if (pp2 == 0) and (pp1 != 0):
            p10.append(salida[:,1][0]) ; p25.append(salida[:,1][1]) ; p40.append(salida[:,1][2]) ; p50.append(salida[:,1][3]) ; p60.append(salida[:,1][4]) ; p75.append(salida[:,1][5]) ; p90.append(salida[:,1][6])
        ejey_min = lim_pos[kk][0] ; ejey = lim_pos[kk][1] ; posy1 = lim_pos[kk][2] ; posy2 = lim_pos[kk][3]

    ye=int(datel.year)
    mo=int(datel.month)
    if mo < 10:
        mo='0'+str(mo)
    da=int(datel.day)
    if da < 10:
        da='0'+str(da)
    ho=int(datel.hour)
    if ho < 10:
        ho='0'+str(ho)
    mi=int(datel.minute)
    if mi < 10:
        mi='0'+str(mi)
    
    # Cargar archivos para corregir pronostico
    f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/reglas/ejex_'+str(n)+'.bin','r')
    ejex_cor=cPickle.load(f)
    f.close()
    
    f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/reglas/offset_'+str(n)+'.bin','r')
    offset_cor=cPickle.load(f)
    f.close()
    for ej in np.arange(1,len(ejex_cor),1):
        if (max(p60) >= ejex_cor[ej-1]) and (max(p60) < ejex_cor[ej]):
            offs = offset_cor[ej]

    if (n == 115) or (n == 101):
        offs = offs*0.    

    p25[1::] = p25[1::] + offs
    p75[1::] = p75[1::] + offs
    p50[1::] = p50[1::] + offs
    p40[1::] = p40[1::] + offs
    p60[1::] = p60[1::] + offs

    if len(ti_plot) != len(p25):
    	ti_plot = ti_plot[0:len(p25)]
    
    #plt.plot(ti_plot,p10,label='Percentil 10-90',c='c',lw=2) plt.plot(ti_plot,p90,c='c',lw=2) 
    #plt.fill_between(ti_plot,p10, p90, color='cyan', alpha='0.5')
    plt.plot(ti_plot,p25,label='Percentil 25-75',c='r',lw=1)
    plt.plot(ti_plot,p75,c='r',lw=1)
    plt.fill_between(ti_plot,p25, p75, color='red', alpha='0.1')
    plt.plot(ti_plot,p40,label='Percentil 40-60',c='g',lw=1)
    plt.plot(ti_plot,p60,c='g',lw=1)
    plt.fill_between(ti_plot,p40, p60, color='green', alpha='0.15',lw=2)
    plt.plot(ti_plot,p50,label='Percentil 50',c='k',lw=2)
    plt.plot(Fp_date,Np,c='b',label='Nivel observado',lw=2)
    plt.title('Actualizacion : '+str(ye)+'/'+str(mo)+'/'+str(da)+' '+str(ho)+':'+str(mi),size=20)
    plt.xlabel('Tiempo '+'$[min]$',size=20)
    plt.ylabel('Nivel '+'$[cm]$',size=20)
    plt.legend(loc=2)
    plt.grid(linestyle='--',alpha=0.3)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(formatter)
    matplotlib.rcParams.update({'font.size': 18})

    # Graficar niveles de alerta
    xx1 = ti_plot[-1::][0] + datetime.timedelta(minutes=10)
    xx2 = ti_plot[-1::][0] + datetime.timedelta(minutes=20)
    pos_niva = np.where(np.array(total_estaciones) == n)[0]

    # nivel 1
    yy1 = 0
    yy2 = np.array(niveles_alerta)[pos_niva][0][0]
    plt.fill_between([xx1,xx2], yy1, yy2, color='grey', alpha='0.3',lw=1)

    # nivel 2
    yy1 = np.array(niveles_alerta)[pos_niva][0][0]
    yy2 = np.array(niveles_alerta)[pos_niva][0][1]
    plt.fill_between([xx1,xx2], yy1, yy2, color='green', alpha='0.3',lw=1)

    # nivel 3
    yy1 = np.array(niveles_alerta)[pos_niva][0][1]
    yy2 = np.array(niveles_alerta)[pos_niva][0][2]
    plt.fill_between([xx1,xx2], yy1, yy2, color='orange', alpha='0.3',lw=1)
 
    # nivel 4    
    yy1 = np.array(niveles_alerta)[pos_niva][0][2]
    yy2 = np.array(niveles_alerta)[pos_niva][0][3]
    plt.fill_between([xx1,xx2], yy1, yy2, color='red', alpha='0.3',lw=1)

    #nivel 5
    yy1 = np.array(niveles_alerta)[pos_niva][0][3]
    yy2 = 1000
    plt.fill_between([xx1,xx2], yy1, yy2, color='purple', alpha='0.3',lw=1)

    plt.ylim(0,np.array(niveles_alerta)[pos_niva][0][3]+30)
    plt.xlim(Fp_date[0], Fp_date[len(Fp_date)-1]+datetime.timedelta(minutes=50)) #-30,30

    try:
        niv_max = [max(p10[1::]),max(p25[1::]),max(p40[1::]),max(p50[1::]),max(p75[1::])]
    except:
        pass
    
    if pe >= umbral_lluvia:
        
        print nivel[kk]
        print offs, max(p50)
        
        pos_max = np.where(np.array(Np) == max(Np))[0][0]
        niv_ult = Np[len(Np)-1]
        try:
            mit_ser = np.array(Np)[0:pos_max] ; mit_fec = np.array(Fp)[0:pos_max]
            mit_ser2 = abs(mit_ser-niv_ult)
            pos_ant = np.where(np.array(mit_ser2) == min(mit_ser2))[0][0]
            date_ult = datetime.datetime(Fp[len(Fp)-1][0].year, Fp[len(Fp)-1][0].month, Fp[len(Fp)-1][0].day,
                                         int(str(Fp[len(Fp)-1][2]).split(':')[0]), int(str(Fp[len(Fp)-1][2]).split(':')[1]))
            date_ant = datetime.datetime(np.array(mit_fec)[pos_ant][0].year, np.array(mit_fec)[pos_ant][0].month,
                                         np.array(mit_fec)[pos_ant][0].day,int(str(np.array(mit_fec)[pos_ant][2]).split(':')[0]),
                                         int(str(np.array(mit_fec)[pos_ant][2]).split(':')[1]))
            date_max = datetime.datetime(np.array(Fp)[pos_max][0].year, np.array(Fp)[pos_max][0].month,
                                         np.array(Fp)[pos_max][0].day,int(str(np.array(Fp)[pos_max][2]).split(':')[0]),
                                         int(str(np.array(Fp)[pos_max][2]).split(':')[1]))
            dif = str(date_ult-date_ant)
            dif_min = int(dif.split(':')[1])

            if int(dif.split(':')[0]) >= 1:
                dif_min = dif_min + 60.*float(dif.split(':')[0])
            if dif_min >= 5:
                estado = 'bajando'
            if dif_min < 5:
                estado = 'subiendo'
            if abs(np.array(Np)[pos_max] - np.array(mit_ser)[pos_ant]) < 5:
                estado = 'estable'
        except:
            estado = 'estable'
            
        print estado

        if (estado == 'estable') or (estado == 'subiendo'):
            plt.savefig('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/Pronostico_N'+str(n)+'.png')
            pronostico_txt.append(str(n)+','+str(max(p25))+','+str(max(p50))+','+str(max(p75))+','+str(30))
        if estado == 'bajando':
            pronostico_txt.append(str(n)+','+str(0)+','+str(0)+','+str(0)+','+str(0))
            os.system('cp /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/NoLluvia.png /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/Pronostico_N'+str(n)+'.png')
    
        print '---'
        
    if pe < umbral_lluvia:
        pronostico_txt.append(str(n)+','+str(0)+','+str(0)+','+str(0)+','+str(0))
        os.system('cp /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/NoLluvia.png /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/Pronostico_N'+str(n)+'.png') 


#-----------------------------------------------------------------------------------
#  2. Modelo de transito de crecidas (entrada: base de datos - modelo estadistico)
#-----------------------------------------------------------------------------------

estaciones = [106,179,91,94,93,99,140]
tipos = ['ni','ni','ni','ni','ni','ni','ni']
tramos = [[106,91],[106,179],[91,94],[94,93],[93,99],[99,140]]
mod_mat = np.zeros((len(pronostico_txt),5))

for m in range(len(pronostico_txt)):
    for col in range(5):
        mod_mat[m,col] = pronostico_txt[m].split(',')[col]


# Consulta de niveles maximos en las estaciones
tra_mat = np.zeros((len(estaciones),4))
tra_mat_ti = np.zeros((len(estaciones),4))

def consultaN(dt_fecha,n,red):
    date_fin=dt_fecha
    date_ini=dt_fecha-datetime.timedelta(minutes=120)
    datos_n = "SELECT fecha, DATE_FORMAT(fecha,'%Y-%m-%d'), hora, DATE_FORMAT(hora, '%H:%i:%s'), ("+str(Cod[0][2])+"-"+red+"), calidad FROM datos WHERE cliente = "+str(n)+" and (((fecha>'"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"') or (fecha='"+str(date_ini.year)+"-"+str(date_ini.month)+"-"+str(date_ini.day)+"' and hora>='"+str(date_ini.hour)+":"+str(date_ini.minute)+":00')) and ((fecha<'"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"') or (fecha='"+str(date_fin.year)+"-"+str(date_fin.month)+"-"+str(date_fin.day)+"' and hora<='"+str(date_fin.hour)+":"+str(date_fin.minute)+":00')))"
    db = MySQLdb.connect(host, user,passw,bd_nombre)
    db_cursor = db.cursor()
    db_cursor.execute(datos_n)
    data = db_cursor.fetchall()
    N=[] ; fec=[]

    for dato in data:
        if (dato[5] == 1) and (dato[4] < 1000):
            N.append(dato[4])
            fec.append(dato)
    return N,fec 

max_BD = [] ; date_BD = [] 
estado_creciente = [] 
registros_nivel = []
registros_date = []

for k in range(len(estaciones)):
    
    # open database connection
    host ='192.168.1.74'
    user ='usrCalidad'
    passw ='aF05wnXC;'
    bd_nombre ='siata'
    Estaciones="SELECT Codigo,Nombreestacion, offsetN, bancallena red FROM estaciones WHERE codigo=("+str(estaciones[k])+")"
    db = MySQLdb.connect(host, user,passw,bd_nombre)
    db_cursor = db.cursor()
    db_cursor.execute(Estaciones)
    Cod = db_cursor.fetchall()
    con_niv=consultaN(datel,estaciones[k],tipos[k])
    Np=con_niv[0] ; Fp=con_niv[1]

    try:
    	aa = np.where(np.array(Np) == max(Np))[0]
    	pos_max = aa[0]
    	niv_ult = Np[len(Np)-1]
    except:
	niv_ult = 0
    
    # Determinar el estado (estable, subiendo, bajando)
    try:
        mit_ser = np.array(Np)[0:pos_max] ; mit_fec = np.array(Fp)[0:pos_max]
        mit_ser2 = abs(mit_ser-niv_ult)
        pos_ant = np.where(np.array(mit_ser2) == min(mit_ser2))[0][0]
        date_ult = datetime.datetime(Fp[len(Fp)-1][0].year, Fp[len(Fp)-1][0].month, Fp[len(Fp)-1][0].day,
                                     int(str(Fp[len(Fp)-1][2]).split(':')[0]), int(str(Fp[len(Fp)-1][2]).split(':')[1]))
        date_ant = datetime.datetime(np.array(mit_fec)[pos_ant][0].year, np.array(mit_fec)[pos_ant][0].month,
                                     np.array(mit_fec)[pos_ant][0].day,int(str(np.array(mit_fec)[pos_ant][2]).split(':')[0]),
                                     int(str(np.array(mit_fec)[pos_ant][2]).split(':')[1]))
        date_max = datetime.datetime(np.array(Fp)[pos_max][0].year, np.array(Fp)[pos_max][0].month,
                                     np.array(Fp)[pos_max][0].day,int(str(np.array(Fp)[pos_max][2]).split(':')[0]),
                                     int(str(np.array(Fp)[pos_max][2]).split(':')[1]))
        dif = str(date_ult-date_ant)
        dif_min = int(dif.split(':')[1])

        if int(dif.split(':')[0]) >= 1:
            dif_min = dif_min + 60.*float(dif.split(':')[0])
        if dif_min >= 5:
            estado = 'bajando'
        if abs(np.array(Np)[pos_max] - np.array(mit_ser)[pos_ant]) < 5:
            estado = 'estable'
        if dif_min < 5:
            estado = 'subiendo'
    except:
        estado = 'estable'
    estado_creciente.append(estado)
    
    try:
    	date_max = datetime.datetime(np.array(Fp)[aa][0][0].year, np.array(Fp)[aa][0][0].month, np.array(Fp)[aa][0][0].day,
                      int(str(np.array(Fp)[aa][0][2]).split(':')[0]), int(str(np.array(Fp)[aa][0][2]).split(':')[1]))
    	date_BD.append(date_max)
    	max_BD.append(np.array(Np)[aa][0])
    	tra_mat[k,0] = estaciones[k]
    	tra_mat[k,1] = np.array(Np)[aa][0] ; tra_mat[k,2] = np.array(Np)[aa][0] ; tra_mat[k,3] = np.array(Np)[aa][0]
    except:
        date_max = datel
        date_BD.append(date_max)
        max_BD.append(0)
        tra_mat[k,0] = estaciones[k]
        tra_mat[k,1] = 0 ; tra_mat[k,2] = 0 ; tra_mat[k,3] = 0

    registros_nivel.append(Np)
    registros_date.append(Fp)

# Analizar tramo a tramo

for t in tramos:  

    print t
    arch = 'reglas_'+str(t[0])+'-'+str(t[1])+'.bin'
    # Carga las reglas para el tramo
    f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Transito/'+arch,'r')
    reglas_transito = cPickle.load(f)
    f.close()
    
    pos_est = np.where(np.array(estaciones) == t[0])[0]
    pos_est1 = np.where(np.array(estaciones) == t[1])[0]
    max_est_bd = np.array(max_BD)[pos_est]
    tra_mat[pos_est,2]
    ejes = reglas_transito['ejes']
    if (max_est_bd > min(ejes)):# and (np.array(estado_creciente)[pos_est] == 'bajando'): # Pronostico usando base de datos y reglas de transito
        pos_eje = np.where(np.array(abs(ejes-max_est_bd)) == min(abs(ejes-max_est_bd)))[0]
        print 'Base de datos'
        
        # Guardar niveles
        tra_mat[pos_est1,1] = np.array(reglas_transito['P25_niv'])[pos_eje][0]
        tra_mat[pos_est1,2] = np.array(reglas_transito['P50_niv'])[pos_eje][0]
        tra_mat[pos_est1,3] = np.array(reglas_transito['P75_niv'])[pos_eje][0]
        
        # Guardar tiempos
	date_pico1 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P25_ti'])[pos_eje][0])
        date_pico2 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P50_ti'])[pos_eje][0])
        date_pico3 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P75_ti'])[pos_eje][0])

	try:
		tiempo_adel1 = int(str(date_pico1 - datel).split(':')[0])*60 + int(str(date_pico1 - datel).split(':')[1])
	        tiempo_adel2 = int(str(date_pico2 - datel).split(':')[0])*60 + int(str(date_pico2 - datel).split(':')[1])
		tiempo_adel3 = int(str(date_pico3 - datel).split(':')[0])*60 + int(str(date_pico3 - datel).split(':')[1])
	except:
		tiempo_adel1 = 0 ; tiempo_adel2 = 0 ; tiempo_adel3 = 0


	tra_mat_ti[pos_est1,1] = tiempo_adel1
	tra_mat_ti[pos_est1,2] = tiempo_adel2
	tra_mat_ti[pos_est1,3] = tiempo_adel3

    
    if max_est_bd <= min(ejes):
        
        pos_mod = np.where(np.array(mod_mat[:,0]) == t[0])[0]
        try:
            if mod_mat[pos_mod][0][1] != 0: # Pronostico usando resultados de modelo de crecidas
                print 'Modelo crecida'
                
                if mod_mat[pos_mod][0][1] > min(ejes):
                    pos_eje = np.where(np.array(abs(ejes-mod_mat[pos_mod][0][1])) == min(abs(ejes-mod_mat[pos_mod][0][1])))[0]
            
                    # Guardar niveles
                    tra_mat[pos_est1,1] = np.array(reglas_transito['P25_niv'])[pos_eje][0]
                    tra_mat[pos_est1,2] = np.array(reglas_transito['P50_niv'])[pos_eje][0]
                    tra_mat[pos_est1,3] = np.array(reglas_transito['P75_niv'])[pos_eje][0]
            
                    # Guardar tiempos
		    date_pico1 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P25_ti'])[pos_eje][0])
                    date_pico2 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P50_ti'])[pos_eje][0])
                    date_pico3 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P75_ti'])[pos_eje][0])

		    try:
		    	tiempo_adel1 = int(str(date_pico1 - datel).split(':')[0])*60 + int(str(date_pico1 - datel).split(':')[1])
                        tiempo_adel2 = int(str(date_pico2 - datel).split(':')[0])*60 + int(str(date_pico2 - datel).split(':')[1])
                        tiempo_adel3 = int(str(date_pico3 - datel).split(':')[0])*60 + int(str(date_pico3 - datel).split(':')[1])
		    except:
		    	tiempo_adel1 = 0 ; tiempo_adel2 = 0 ; tiempo_adel3 = 0

		    tra_mat_ti[pos_est1,1] = tiempo_adel1
                    tra_mat_ti[pos_est1,2] = tiempo_adel2
                    tra_mat_ti[pos_est1,3] = tiempo_adel3

        except:
            pass
        
        max_est_tra = tra_mat[pos_est,2]
        if max_est_tra > min(ejes): # Pronostico usando resultados de modelo de transito
            pos_eje = np.where(np.array(abs(ejes-max_est_tra)) == min(abs(ejes-max_est_tra)))[0]
            print 'Modelo transito'
            
            # Guardar niveles
            tra_mat[pos_est1,1] = np.array(reglas_transito['P25_niv'])[pos_eje][0]
            tra_mat[pos_est1,2] = np.array(reglas_transito['P50_niv'])[pos_eje][0]
            tra_mat[pos_est1,3] = np.array(reglas_transito['P75_niv'])[pos_eje][0]
            
            # Guardar tiempos
            date_pico1 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P25_ti'])[pos_eje][0])
            date_pico2 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P50_ti'])[pos_eje][0])
            date_pico3 = np.array(date_BD)[pos_est][0] + datetime.timedelta(minutes=np.array(reglas_transito['P75_ti'])[pos_eje][0])

            try:
                tiempo_adel1 = int(str(date_pico1 - datel).split(':')[0])*60 + int(str(date_pico1 - datel).split(':')[1])
                tiempo_adel2 = int(str(date_pico2 - datel).split(':')[0])*60 + int(str(date_pico2 - datel).split(':')[1])
                tiempo_adel3 = int(str(date_pico3 - datel).split(':')[0])*60 + int(str(date_pico3 - datel).split(':')[1])
            except:
                tiempo_adel1 = 0 ; tiempo_adel2 = 0 ; tiempo_adel3 = 0

            tra_mat_ti[pos_est1,1] = tiempo_adel1
            tra_mat_ti[pos_est1,2] = tiempo_adel2
            tra_mat_ti[pos_est1,3] = tiempo_adel3

        if max_est_tra < min(ejes): # El maximo en la estacion no corresponde a crecida
            print 'sin crecida'
            tra_mat[pos_est,1] = tra_mat[pos_est,2] = tra_mat[pos_est,3] = 0
            tra_mat[pos_est1,1] = tra_mat[pos_est1,2] = tra_mat[pos_est1,3] = 0
                
    if np.array(estado_creciente)[pos_est] == 'bajando': # Eliminar pronostico cuando paso la creciente
        tra_mat[pos_est,1] = 0 ;tra_mat[pos_est,2] = 0 ; tra_mat[pos_est,3] = 0
        tra_mat_ti[pos_est,1] = 0 ;tra_mat_ti[pos_est,2] = 0 ; tra_mat_ti[pos_est,3] = 0
    
    #if (t[1] == 140) and (np.array(estado_creciente)[pos_est1] == 'bajando'): # Eliminar pronostico cuando paso creciente en Copacabana
    #    tra_mat[pos_est1,1] = 0 ;tra_mat[pos_est1,2] = 0 ; tra_mat[pos_est1,3] = 0
    #    tra_mat_ti[pos_est1,1] = 0 ;tra_mat_ti[pos_est1,2] = 0 ; tra_mat_ti[pos_est1,3] = 0
    
    pos_mod = np.where(np.array(mod_mat[:,0]) == t[0])[0]
    if (t[0] == 106) and (mod_mat[pos_mod][0][2] == 0): # Eliminar pronostico en Tres Aguas si es lluvia pequena
        tra_mat[pos_est,1] = 0 ; tra_mat[pos_est,2] = 0 ; tra_mat[pos_est,3] = 0
        tra_mat[pos_est1,1] = 0 ;tra_mat[pos_est1,2] = 0 ; tra_mat[pos_est1,3] = 0
        
        tra_mat_ti[pos_est,1] = 0 ; tra_mat_ti[pos_est,2] = 0 ; tra_mat_ti[pos_est,3] = 0
        tra_mat_ti[pos_est1,1] = 0 ; tra_mat_ti[pos_est1,2] = 0 ; tra_mat_ti[pos_est1,3] = 0
    
    print '---' 
    
    if max(tra_mat[pos_est1][0][1::]) > 0:
    	plt.close('all')
	fig=plt.figure(edgecolor='w',facecolor='w',figsize=(12,9))
	formatter = DateFormatter ('%H:%M')
	Np = np.array(registros_nivel)[pos_est1][0]
	Fp = np.array(registros_date)[pos_est1][0]

	# Volver date las fechas
	Fp_date = []
	for ff in Fp:
		yy = ff[1].split('-')[0] ; mm = ff[1].split('-')[1] ; dd = ff[1].split('-')[2]
		hh = ff[3].split(':')[0] ; minn = ff[3].split(':')[1]
		Fp_date.append(datetime.datetime(int(yy),int(mm),int(dd),int(hh),int(minn)))

	ejey = np.arange(1,1000,1)

        if tra_mat_ti[pos_est1][0][1] != 0:
            ejex1 = datel + datetime.timedelta(minutes=tra_mat_ti[pos_est1][0][1])
            ejex3 = datel + datetime.timedelta(minutes=tra_mat_ti[pos_est1][0][3])
            ejex2 = datel + datetime.timedelta(minutes=tra_mat_ti[pos_est1][0][2])
        if tra_mat_ti[pos_est1][0][1] == 0:
            ejex1 = date_pico1
            ejex2 = date_pico2
            ejex3 = date_pico3

	ejey1 = tra_mat[pos_est1][0][1] ; ejey3 = tra_mat[pos_est1][0][3] ; ejey2 = tra_mat[pos_est1][0][2]

	plt.fill_between([ejex1,ejex3],ejey1,ejey3,color='r', alpha=0.05)
	plt.plot([ejex1,ejex3],[ejey1,ejey1],c='r',label='Percentil 25-75',lw=0.5)
	plt.plot([ejex1,ejex3],[ejey3,ejey3],c='r',lw=0.5)
	plt.plot([ejex1,ejex1],[ejey1,ejey3],c='r',lw=0.5)
	plt.plot([ejex3,ejex3],[ejey1,ejey3],c='r',lw=0.5)

	plt.scatter(ejex2,ejey2,c='k',s=40,label='Percentil 50')
	print ejex1, ejex2, ejex3

	min1 = int(str((ejex3-ejex2)/2).split(':')[1])
	min2 = int(str((ejex2-ejex1)/2).split(':')[1])
	lim1 = ejex2 + datetime.timedelta(minutes=min1)
	lim2 = ejex2 - datetime.timedelta(minutes=min2)
	plt.fill_between([lim1,lim2],np.mean([ejey1,ejey2]),np.mean([ejey3,ejey2]),color='g', alpha=0.15)
	plt.plot([lim1,lim2],[np.mean([ejey1,ejey2]),np.mean([ejey1,ejey2])],c='g',label='Percentil 40-60',lw=0.5)
	plt.plot([lim1,lim2],[np.mean([ejey3,ejey2]),np.mean([ejey3,ejey2])],c='g',lw=0.5)
	plt.plot([lim1,lim1],[np.mean([ejey1,ejey2]),np.mean([ejey3,ejey2])],c='g',lw=0.5)
	plt.plot([lim2,lim2],[np.mean([ejey1,ejey2]),np.mean([ejey3,ejey2])],c='g',lw=0.5)

	plt.plot(Fp_date,Np,c='b',label='Nivel observado',lw=2)

	plt.title('Actualizacion : '+str(ye)+'/'+str(mo)+'/'+str(da)+' '+str(ho)+':'+str(mi))
	plt.xlabel('Tiempo '+'$[min]$')
	plt.ylabel('Nivel '+'$[cm]$')
	plt.legend(loc=2)
	plt.grid(linestyle='--',alpha=0.3)
	ax = plt.gca()
	ax.xaxis.set_major_formatter(formatter)
	matplotlib.rcParams.update({'font.size': 18})


	# Graficar niveles de alerta
	xx1 = ejex3 + datetime.timedelta(minutes=10)
	xx2 = ejex3 + datetime.timedelta(minutes=20)
	pos_niva = np.where(np.array(total_estaciones) == t[1])[0]

	# nivel 1
	yy1 = 0
	yy2 = np.array(niveles_alerta)[pos_niva][0][0]
	plt.fill_between([xx1,xx2], yy1, yy2, color='grey', alpha='0.3',lw=1)

	# nivel 2
	yy1 = np.array(niveles_alerta)[pos_niva][0][0]
	yy2 = np.array(niveles_alerta)[pos_niva][0][1]
	plt.fill_between([xx1,xx2], yy1, yy2, color='green', alpha='0.3',lw=1)

	# nivel 3
	yy1 = np.array(niveles_alerta)[pos_niva][0][1]
	yy2 = np.array(niveles_alerta)[pos_niva][0][2]
	plt.fill_between([xx1,xx2], yy1, yy2, color='orange', alpha='0.3',lw=1)

	# nivel 4
	yy1 = np.array(niveles_alerta)[pos_niva][0][2]
	yy2 = np.array(niveles_alerta)[pos_niva][0][3]
	plt.fill_between([xx1,xx2], yy1, yy2, color='red', alpha='0.3',lw=1)

	# nivel 5
	yy1 = np.array(niveles_alerta)[pos_niva][0][3]
	yy2 = 1000
	plt.fill_between([xx1,xx2], yy1, yy2, color='purple', alpha='0.3',lw=1)

	plt.ylim(0,np.array(niveles_alerta)[pos_niva][0][3]+30)
	plt.xlim(Fp_date[0],ejex3++datetime.timedelta(minutes=20))	

	plt.savefig('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/Pronostico_N'+str(t[1])+'.png')
	

    if max(tra_mat[pos_est1][0][1::]) == 0:
	os.system('cp /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/NoLluvia.png /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/Pronostico_N'+str(t[1])+'.png')


tra_mat[:,0] = estaciones
tra_mat_ti[:,0] = estaciones
print tra_mat

# Escribir archivo para graficar pronosticos : Salida

pronostico_niveles = [] 

for i in range(len(tra_mat)):
    ii = tra_mat[i]
    jj = tra_mat_ti[i]
    pronostico_niveles.append([int(ii[0]),ii[1],ii[2],ii[3],max(jj[1],0),max(jj[2],0),max(jj[3],0)]) 

delete = []

for k in range(len(mod_mat)):
    pos_mod = np.where(np.array(estaciones) == mod_mat[k][0])[0]
    if np.array(estado_creciente)[pos_mod] != 'bajando': # Estaciones que tienen ambos pronosticos
                                                         # si esta estable o subiendo tomar al estadistico si esta bajando 
                                                         # tomar el de transito
        kk = mod_mat[k]
        pronostico_niveles.append([int(kk[0]),int(kk[1]),int(kk[2]),int(kk[3]),int(kk[4]),int(kk[4]),int(kk[4])])
        delete.append(pos_mod[0])
    
    if len(pos_mod) == 0: # Estaciones que solo tienen modelo estadistico
        kk = mod_mat[k]
        pronostico_niveles.append([kk[0],int(kk[1]),int(kk[2]),int(kk[3]),int(kk[4]),int(kk[4]),int(kk[4])]) 

if len(delete) > 0:
    cont = 0
    for dd in delete:
        del pronostico_niveles[dd-cont]
        cont = cont + 1

#os.system('scp /media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/Figuras_salida/*.png ezapata@192.168.1.74:/var/www/Resumen_Calidad/modelo_estadistico')


f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_crecidas/pronostico_niveles.bin','w') 
cPickle.dump(pronostico_niveles,f)
f.close()
