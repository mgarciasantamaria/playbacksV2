#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, sys, traceback, json, time
import datetime
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
	beginning=time.time() #Captura del dato de hora y fecha al momento de empezar la ejecucion del codigo.
	dict={} #-Diccionario para guardar datos para escribir en el log de eventos
	if Flag_Status('r'):	
		try:
			date_now=datetime.datetime.now() #Captura del dato de hora y fecha.
			dict["segmentsdelete"]=segmentsDelete(date_now)
			date_log=str(datetime.datetime.strftime(date_now, "%Y-%m-%d")) #Transformacion de dato de hora y fecha al formato "AAAA-MM-DD" y a tipo string.
			date_run=str(datetime.datetime.strftime(date_now - datetime.timedelta(days=1), "%Y-%m-%d")) #Se resta un dia y se Transforma el dato de hora y fecha al formato "%Y-%m-%d" y a tipo string.
			postgresql=psycopg2.connect(data_base_connect_prod) #Se crea conexion con la base de datos de acuerdo con los datos almacenados en la variable data_base_connect_prod.
			curpsql=postgresql.cursor() #Se crea el objeto cursor para ejecutar comandos SQL.
			curpsql.execute(f"SELECT COUNT(*) FROM new_manifests WHERE datetime LIKE '%{date_run}%';") #Se ejecuta la consulta SQL que permite contar el numero de registros en la tabla new_manifests cuya columna datetime contiene el valor de la variable date_run.
			dict["Manifest_Finded"]=str(curpsql.fetchone()[0]) #Se asigna el valor que devuelve la consulta a la clave Manifest_Finded del dicionario dict.
			curpsql.execute(f"DELETE FROM new_manifests WHERE manifestid IN (SELECT new_manifests.manifestid FROM new_manifests LEFT JOIN new_segmentos ON new_manifests.manifestid = new_segmentos.manifestid where new_segmentos.manifestid is NULL) AND datetime LIKE '%{date_run}%';") # ejecuta una consulta SQL que elimina registros de la tabla new_manifests cuyo manifestid no está en la tabla new_segmentos y cuya columna datetime contiene la cadena de texto de la variable date_run
			dict["Manifets_Deleted"]=str(curpsql.rowcount) #Se asigna el valor del conteo de los registros eliminados con la consulta anterior a la clave Manifests_Delete del diccionario dict.
			curpsql.execute("SELECT DISTINCT SUBSTR(new_manifests.contentid, 3) FROM new_manifests LEFT JOIN xmldata ON SUBSTR(new_manifests.contentid, 3) = xmldata.contentid where  xmldata.contentid is NULL;")# ejecuta una consulta SQL que extrae registros de la tabla new_manifests cuyo contentid no está en la tabla xmldata.
			contentid_list=curpsql.fetchall() #Se almacena como una lista el dato que arroja la consulta anterior.
			if contentid_list != []: #Se valida si la lista contentid_list no esta vacia.
				dict["XmlData"]=extract_xml_data(contentid_list, date_log) #Se asigna la lista que retorna la funcion extract_xml_data a la clave OkXmlData del diccionario dict. 
			curpsql.execute(sql_statement(date_run)) #Se ejecuta la consulta SQL principal la cual permite generar los datos a registrar en la tabla playbacks.
			dict["Playbacks_Registered"]=str(curpsql.rowcount) #Se asigna la cantidad de registros a la clave Playbacks_Registered del diccionario dict.
			postgresql.commit() #Se confirman los cambios en la base de datos
			postgresql.close() #Se cierra la conexion a la base de datos 
			finish=time.time() #Se almacena el dato de hora y fecha al finalizar las consultas SQL.
			dict['Process_duration']=str(round((finish-beginning),3)) #Se asigna a la clave Process_duration del diccionario dict el calculo de la duracion del proceso.
			dict_str_json=json.dumps(dict, sort_keys=False, indent=8) #-Se da formato tipo json-string al diccionario
			SendMail(str(dict_str_json), "Playbacks execution summary PROD") #-Uso de la funcion SendMail para enviar email con el resumen de la ejecucion
			print_log(dict_str_json, date_log) #Se ejecuta la funcion print_log que escribe en el archivo log el texto de la variable dict_str_json.
		except:
			error=sys.exc_info()[2] #-------Captura del error que arroja el sistema
			errorinfo=traceback.format_tb(error)[0] #-Captura el detalle del error
			dict["Excute_Error"]=[errorinfo, str(sys.exc_info()[1])] #-Se agrega al diccionario detalle del error generado
			dict_str_json=json.dumps(dict, sort_keys=False, indent=8) #-Se da formato tipo json al dicionario 
	#-------Se crea el archivo log con el resumen de la ejecucion y el error
			SendMail(str(dict_str_json), "Playbacks execution summary PROD") #-Se usa la funcion SendEmail para enviar email con el resumen de la ejecucion y el error
			print_log(dict_str_json, date_log) ##Se ejecuta la funcion print_log que escribe en el archivo log el texto de la variable dict_str_json.
	else:
		SendMail('etltoolbox application failure not recognized\n', 'etlcdnata application failure not recognized')