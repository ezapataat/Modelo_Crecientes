
# coding: utf-8

import numpy as np
import os
import datetime
import sys, getopt
import cPickle

flag = 1 # Se asume desde el inicio que existe evento de lluvia

file_rain1 = '/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm_sp.bin'
file_rain2 = '/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/lluvia_swmm_cpp.bin'

try:
    # Carga informacion de lluvia en las subcuencas bulerias (caso1: sin pronostico)
    f1=open(file_rain1,'r')
    rain1=cPickle.load(f1)
    f1.close()
    
    # Carga informacion de lluvia en las subcuencas bulerias (caso2: con pronostico de pluvios)
    f2=open(file_rain2,'r')
    rain2=cPickle.load(f2)
    f2.close()
except:
    flag = 0 # Se establece que no hay evento de lluvia

if flag == 1:
    
    path = '/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/Eventos/'
    datel = datetime.datetime.now()
    yy = datel.year ; mm = datel.month ; dd = datel.day ; hh = datel.hour
    
    name_folder = str(yy)+'-'+str(mm)+'-'+str(dd)+'_'+str(hh)
    os.system('mkdir '+path+name_folder+'_1')
    os.system('mkdir '+path+name_folder+'_2')

    # Parametros de entrada
    titulo = 'Bulerias'
    
    # Caso 1
    size_serie1 = len(rain1)
    fecha_ini1 = str(rain1[0][1].month)+'/'+str(rain1[0][1].day)+'/'+str(rain1[0][1].year) #mes/dia/ano
    fecha_fin1 = str(rain1[len(rain1)-1][1].month)+'/'+str(rain1[len(rain1)-1][1].day)+'/'+str(rain1[len(rain1)-1][1].year) #mes/dia/ano
    hora_ini1 = str(rain1[0][1].hour)+':'+str(rain1[0][1].minute)+':00' #hora/min/seg
    hora_fin1 = str(rain1[len(rain1)-1][1].hour)+':'+str(rain1[len(rain1)-1][1].minute)+':00' #hora/min/seg
    
    # Caso 2
    size_serie2 = len(rain2)
    fecha_ini2 = str(rain2[0][1].month)+'/'+str(rain2[0][1].day)+'/'+str(rain2[0][1].year) #mes/dia/ano
    fecha_fin2 = str(rain2[len(rain2)-1][1].month)+'/'+str(rain2[len(rain2)-1][1].day)+'/'+str(rain2[len(rain2)-1][1].year) #mes/dia/ano
    hora_ini2 = str(rain2[0][1].hour)+':'+str(rain2[0][1].minute)+':00' #hora/min/seg
    hora_fin2 = str(rain2[len(rain2)-1][1].hour)+':'+str(rain2[len(rain2)-1][1].minute)+':00' #hora/min/seg
    
    # Parametros para convertir unidades
    mm_in = 0.0393701
    m_ft = 3.28
    ha_ac = 2.47105
    man_tuberia = '0.017'

    for caso in [1,2]:
        
        if caso == 1:
            fecha_ini = fecha_ini1 ; hora_ini = hora_ini1 ; fecha_fin = fecha_fin1 ; hora_fin = hora_fin1
            rain = rain1
        
        if caso == 2:
            fecha_ini = fecha_ini2 ; hora_ini = hora_ini2 ; fecha_fin = fecha_fin2 ; hora_fin = hora_fin2
            rain = rain2
    
        x = []
        x.append('[TITLE]')
        x.append(titulo)

        x.append('\n')
        x.append('[OPTIONS]')
        x.append(';;                        Value')
        x.append('FLOW_UNITS'+'            '+'CFS')
        x.append('INFILTRATION'+'          '+'GREEN_AMPT')
        x.append('FLOW_ROUTING'+'          '+'DYNWAVE')
        x.append('START_DATE'+'            '+fecha_ini)
        x.append('START_TIME'+'            '+hora_ini)
        x.append('REPORT_START_DATE'+'     '+fecha_ini)
        x.append('REPORT_START_TIME'+'     '+hora_ini)
        x.append('END_DATE'+'              '+fecha_fin)
        x.append('END_TIME'+'              '+hora_fin)
        x.append('SWEEP_START'+'           '+'01/01')
        x.append('SWEEP_END'+'             '+'01/02')
        x.append('DRY_DAYS'+'              '+'0')
        x.append('REPORT_STEP'+'           '+'00:10:00')  
        x.append('WET_STEP'+'              '+'00:10:00') # step time en tiempo humedo
        x.append('DRY_STEP'+'              '+'00:10:00') # step time en tiempo seco
        x.append('ROUTING_STEP'+'          '+'00:01:00')
        x.append('ALLOW_PONDING'+'         '+'NO')
        x.append('INERTIAL_DAMPING'+'      '+'NONE')
        x.append('VARIABLE_STEP'+'         '+'0.50') # factor de seguridad para relacion de Courant
        x.append('LENGTHENING_STEP'+'      '+'5.0') # alargamiento maximo permitido en los conductos para satisfacer Courant
        x.append('MIN_SURFAREA'+'          '+'12.566')
        x.append('NORMAL_FLOW_LIMITED'+'   '+'BOTH') # criterio para determinat flujo supercritico (slope, froude, both)
        x.append('SKIP_STEADY_STATE'+'     '+'NO')
        x.append('FORCE_MAIN_EQUATION'+'   '+'H-W') # Hazen-Williams (H-W) or the Darcy-Weisbach (D-W) para calcular perdidas en conductos circulares
        x.append('LINK_OFFSETS'+'          '+'ELEVATION') # modo de referenciar los enlaces (depth, elevation)
        x.append('MIN_SLOPE'+'             '+'0')

        #Carga informacion de los nodos Bulerias
        f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/nodos.bin','r')
        nodos=cPickle.load(f)
        f.close()

        ID_nodos = [] ; elev_nodos = []

        #x.append('\n')
        #x.append('[JUNCTIONS]')
        #x.append(';;               Invert     Max.       Init.      Surcharge  Ponded    ')
        #x.append(';;Name           Elev.      Depth      Depth      Depth      Area      ')

        #for n in range(len(nodos)):
            #elev_nodos.append(float(nodos[n].split(',')[3])*m_ft)

        #for n in range(len(nodos)):
            #name = int(nodos[n].split(',')[0])
            #name_str = str(name)
            #if name < 100:
                #name_str = '0'+str(name)
            #if name < 10:
                #name_str = '0'+name_str
            #ID_nodos.append('N'+name_str)

            #if int(nodos[n].split(',')[0]) not in (290,371,373,293): # No declarar como JUNCTIONS los nodos que son OUTFALLS (salidas)
                #Ymax = (float(nodos[n].split(',')[4]) - float(nodos[n].split(',')[3]))*m_ft
                #x.append('N'+name_str+'   '+"%0.3f" % (float(nodos[n].split(',')[3])*m_ft-2.)+'    '+"%0.3f" % float(Ymax)+'    '+'0'+'    '+'0'+'    '+'0')

        #-----------------------------------------------------------------------
        # Ensayar con STORAGE en los nodos

        x.append('\n')
        x.append('[STORAGE]')
        x.append(';;               Invert     Max.       Init.      Storage     Curve            Ponded')
        x.append(';;Name           Elev.      Depth      Depth      Curve       Params           Area ')

        for n in range(len(nodos)):
            elev_nodos.append(float(nodos[n].split(',')[3])*m_ft)

        for n in range(len(nodos)):
            name = int(nodos[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            ID_nodos.append('N'+name_str)
            radio = float(nodos[n].split(',')[4])

            if int(nodos[n].split(',')[0]) not in (290,371,373,293): # No declarar como JUNCTIONS los nodos que son OUTFALLS (salidas)
                Ymax = float(nodos[n].split(',')[5])*m_ft
                x.append('N'+name_str+'   '+"%0.3f" % (float(nodos[n].split(',')[3])*m_ft)+'    '+"%0.3f" % float(Ymax)+'    '+'0'+'     '+'FUNCTIONAL'+'    '+str(((radio*m_ft)**2)*3.1416)+'      '+'0'+'     '+'0'+'    '+'0'+'    '+'0')

        #------------------------------------------------------------------------
        
        x.append('\n')
        x.append('[RAINGAGES]')
        x.append(';Name           Format      Interval     SCF     DataSource       SourceName')
        x.append(';---------------------------------------------------------------------------')
        for n in range(len(nodos)):
            name = int(nodos[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            x.append('RG'+name_str+'     '+'VOLUME'+'     '+'0:10'+'     '+'1.0'+'     '+'TIMESERIES'+'       '+'SR'+name_str)

        x.append('\n')
        x.append('[TIMESERIES]')
        x.append(';;Name           Date      Time     Value ')
        for n in range(len(rain)):
            name = int(rain[n][0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str

            date_inicial = rain[0][1]
            date_trans = rain[n][1] - datetime.timedelta(hours=date_inicial.hour) - datetime.timedelta(minutes=date_inicial.minute)
            hora = date_trans.hour ; minuto = date_trans.minute
            fecha = datetime.datetime(1990,1,1,hora,minuto)

            dia = fecha.day ;  mes = fecha.month ; anho = fecha.year
            hora = fecha.hour ; minuto = fecha.minute

            pp = float(rain[n][2])*mm_in # Convertir de milimetros a pulgadas
            x.append('SR'+name_str+'    '+str(mes)+'/'+str(dia)+'/'+str(anho)+'    '+str(hora)+':'+str(minuto)+'   '+str(pp))
            #x.append('SR'+name_str+'    '+'1/1/1990'+'   '+str(hora)+':'+str(minuto)+'    '+str(pp))

        # Carga informacion de los conductos Bulerias
        f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/conductos.bin','r')
        conductos=cPickle.load(f)
        f.close()

        ID_cond = [] ; ID_nodo2 = []

        x.append('\n')
        x.append('[CONDUITS]')
        x.append(';Name   Nodo1      Nodo2      Lenght      Nvalue     Zup     Zdown    InitFlow')
        x.append(';-------------------------------------------------------------------------------')
        for c in range(len(conductos)):
            # Nombre
            name = int(c)
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            # Nodo1
            nodo1 = int(conductos[c].split(',')[0])
            nodo1_str = str(nodo1)
            if nodo1 < 100:
                nodo1_str = '0'+str(nodo1)
            if nodo1 < 10:
                nodo1_str = '0'+nodo1_str
            # Nodo2
            nodo2 = int(conductos[c].split(',')[1])
            nodo2_str = str(nodo2)
            if nodo2 < 100:
                nodo2_str = '0'+str(nodo2)
            if nodo2 < 10:
                nodo2_str = '0'+nodo2_str
            # Longitud
            leng = float(conductos[c].split(',')[2])*m_ft
            ID_cond.append('C'+name_str) ; ID_nodo2.append(nodo2_str)
            posN1 = np.where(np.array(ID_nodos) == 'N'+nodo1_str)[0] ; elev1 = 2 #elev1 = float(elev_nodos[posN1[0]]) 
            posN2 = np.where(np.array(ID_nodos) == 'N'+nodo2_str)[0] ; elev2 = 2 #elev2 = float(elev_nodos[posN2[0]]) 
            x.append('C'+name_str+'     '+'N'+nodo1_str+'    '+'N'+nodo2_str+'     '+"%0.3f" % leng+'     '+man_tuberia+'     '+"%0.3f" % float(elev1)+'     '+"%0.3f" % float(elev2)+'     '+'0')

        x.append('\n')
        x.append('[XSECTIONS]')
        x.append(';Name   TYPE      G1      G2      G3     G4')
        x.append(';------------------------------------------')
        for c in range(len(conductos)):
            # Nombre
            name = int(c)
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            diam = float(conductos[c].split(',')[4])/1000.

            x.append('C'+name_str+'   '+'CIRCULAR'+'   '+str(diam*m_ft)+'   '+str(0)+'   '+str(0)+'     '+str(0))

        x.append('\n')
        x.append('[OUTFALLS]')
        x.append(';               Invert     Outfall    Stage/Table      Tide')
        x.append(';Name           Elev.      Type       Time Series      Gate')
        x.append(';------------------------------------ ---------------------')
        for c in range(len(conductos)):
            salida = conductos[c].split(',')[3]
            if int(salida) == 1:
                # Nombre
                name = int(c)
                name_str = str(name)
                if name < 100:
                    name_str = '0'+str(name)
                if name < 10:
                    name_str = '0'+name_str
                posC = np.where(np.array(ID_cond) == 'C'+name_str)[0]
                posN = np.where(np.array(ID_nodos) == 'N'+str(ID_nodo2[posC[0]]))[0]
                x.append('N'+str(ID_nodo2[posC[0]])+'   '+"%0.3f" % (float(elev_nodos[posN[0]])-2.)+'   '+'FREE'+'     '+'NO')

        x.append('\n')
        x.append('[COORDINATES]')
        x.append(';Nodo           CoordX           CoordY')
        x.append(';--------------------------------------')
        for n in range(len(nodos)):
            name = int(nodos[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            x.append('N'+name_str+'   '+"%0.3f" % float(nodos[n].split(',')[1])+'   '+"%0.3f" % float(nodos[n].split(',')[2]))

        # Carga geometria de las subcuencas
        f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/geom_sub.bin','r')
        geom_sub=cPickle.load(f)
        f.close()

        # Carga parametros de las subcuencas
        f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/param_sub.bin','r')
        param_sub=cPickle.load(f)
        f.close()

        id_aux = []
        for pa in param_sub:
            id_aux.append(float(pa.split(',')[0]))

        x.append('\n')
        x.append('[SUBCATCHMENTS]')
        x.append(';Name    Raingage     Outlet    Area    %Imperv    Width    Slope')
        x.append(';----------------------------------------------------------------')
        for n in range(len(geom_sub)):
            name = int(geom_sub[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str

            pos_param = np.where(np.array(id_aux) == float(name))[0]
            por_imper = param_sub[pos_param[0]].split(',')[1]
            slope = param_sub[pos_param[0]].split(',')[2]

            area_sub = float(geom_sub[n].split(',')[1])*ha_ac
            wid_sub = float(geom_sub[n].split(',')[2])*m_ft
            tex = 'S'+name_str+'    '+'RG'+name_str+'    '+'N'+name_str+'    '+"%0.3f" % float(area_sub)+'    '+"%0.1f" % float(por_imper)+'    '+"%0.3f" % float(wid_sub)+'    '+"%0.3f" % float(slope)+'    '+'0.0'
            x.append(tex)

        x.append('\n')
        x.append('[SUBAREAS]')
        x.append(';;Subcatchment   N-Imperv   N-Perv     S-Imperv   S-Perv     PctZero    RouteTo    PctRouted')
        x.append(';;-------------- ---------- ---------- ---------- ---------- ---------- ---------- ---------')
        for n in range(len(param_sub)):
            name = int(param_sub[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str

            N_imper = param_sub[pos_param[0]].split(',')[3]
            N_perm = param_sub[pos_param[0]].split(',')[4]
            S_imper = float(param_sub[pos_param[0]].split(',')[5])*mm_in
            S_perm = float(param_sub[pos_param[0]].split(',')[6])*mm_in
            PctZero = param_sub[pos_param[0]].split(',')[7]

            tex = 'S'+name_str+'    '+"%0.3f" % float(N_imper)+'    '+"%0.3f" % float(N_perm)+'    '+"%0.3f" % float(S_imper)+'    '+"%0.3f" % float(S_perm)+'    '+"%0.0f" % float(PctZero)+'    '+'OUTLET'
            x.append(tex)

        x.append('\n')
        x.append('[INFILTRATION]')
        x.append(';;Subcatchment   Suction    HydCon     IMDmax')
        x.append(';;-------------- ---------- ---------- ----------')
        for n in range(len(geom_sub)):
            name = int(geom_sub[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str
            tex = 'S'+name_str+'    '+'6.57'+'    '+'0.268'+'    '+'0.167'
            x.append(tex) 

        # Carga vertices de las subcuencas
        f=open('/media/nicolas/Home/Jupyter/Esneider/modelo_acoplado/Operacional_Bulerias/binarios/subcuencas.bin','r')
        subcuencas=cPickle.load(f)
        f.close()

        x.append('\n')
        x.append('[Polygons]')
        x.append(';Subcatchment    X-Coord     Y-Coord')
        x.append(';-----------------------------------')
        for n in range(len(subcuencas)):
            name = int(subcuencas[n].split(',')[0])
            name_str = str(name)
            if name < 100:
                name_str = '0'+str(name)
            if name < 10:
                name_str = '0'+name_str

            Xcoord = subcuencas[n].split(',')[1]
            Ycoord = subcuencas[n].split(',')[2]
            tex1 = 'S'+name_str+'   '+"%0.3f" % float(Xcoord)+'   '+"%0.3f" % float(Ycoord)
            x.append(tex1)

        x.append('\n')
        x.append('[EVAPORATION]')
        x.append(';;Type       Parameters')
        x.append(';;---------- ----------')
        x.append('MONTHLY      0.07   0.07   0.07   0.15   0.18   0.21   0.22   0.19   0.14   0.09   0.07   0.07')  
        x.append('DRY_ONLY     YES')

        x.append('\n')
        x.append('[REPORT]')
        x.append(';;              Status')
        x.append('INPUT           YES')    
        x.append('SUBCATCHMENTS   ALL')
        x.append('NODES           ALL')
        x.append('LINKS           C100         C200')
    
        print '------------------------'
        print 'Escribe archivo .inp'
        print name_folder+'_'+str(caso)
        print '------------------------'
        
        b=open(path+name_folder+'_'+str(caso)+'/'+titulo+'.inp', 'w')
        np.savetxt(path+name_folder+'_'+str(caso)+'/'+titulo+'.inp', x, fmt='%s')
        b.close()

if flag == 0:
	print '----------------------------------------------'
	print 'No existen eventos'
	print 'No se generan archivos .inp'


