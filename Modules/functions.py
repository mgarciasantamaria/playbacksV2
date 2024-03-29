#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, psycopg2, boto3, smtplib, json, sys, traceback
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET
from email.message import EmailMessage
from Modules.constants import *

#*******************--SEND MAIL Function--************************************************************************
#Este código de Python define una función llamada SendMail que toma dos argumentos: text y mail_subject. Esta función se utiliza para enviar correos electrónicos utilizando la librería smtplib de Python.
#La función comienza creando un objeto EmailMessage vacío y luego establece el contenido del correo electrónico utilizando el argumento text. Luego, establece el asunto del correo electrónico utilizando el argumento mail_subject.
#A continuación, se establece la dirección de correo electrónico del remitente en el encabezado From utilizando la cadena 'alarmas-aws@vcmedios.com.co'. La dirección de correo electrónico del destinatario se establece en el encabezado To utilizando el valor de la variable Mail_To.
#Después de establecer los encabezados del correo electrónico, la función crea una conexión SMTP utilizando el host y puerto especificados en la función smtplib.SMTP(). En este caso, el host es 10.10.122.17 y el puerto es 25.
#Luego, la función envía el correo electrónico utilizando el método send_message() del objeto SMTP creado anteriormente. Finalmente, la conexión SMTP se cierra usando el método quit().
#La función no devuelve nada ya que simplemente se utiliza para enviar el correo electrónico y no requiere de una salida específica.
def SendMail(text, mail_subject): #se define la función llamada 'SendMail' que acepta dos argumentos: 'text' y 'mail_subject'.
    msg = EmailMessage() #Se crea un objeto 'EmailMessage' vacío para almacenar los detalles del correo electrónico.
    msg.set_content(text) #Se establece el contenido del correo electrónico utilizando el valor del argumento 'text'.
    msg['Subject'] = mail_subject #Se establece el asunto del correo electrónico utilizando el valor del argumento 'mail_subject'.
    msg['From'] = 'alarmas-aws@vcmedios.com.co' #Se establece la dirección de correo electrónico del remitente en el encabezado 'From' del correo electrónico.
    msg['To'] = [Mail_To] # Se establece la dirección de correo electrónico del destinatario en el encabezado 'To' del correo electrónico. El valor de la variable 'Mail_To' se utiliza como dirección de correo electrónico del destinatario.
    conexion = smtplib.SMTP(host='10.10.130.217', port=25) #Se crea una conexión SMTP utilizando el host '10.10.130.217' y el puerto '25'.
    conexion.ehlo() #Se inicia la conexión SMTP con el servidor.
    conexion.send_message(msg) #Se envía el correo electrónico utilizando el método 'send_message()' del objeto SMTP creado anteriormente.
    conexion.quit() #Se cierra la conexión SMTP utilizando el método quit() del objeto SMTP.
    return
#***********************************--END--************************************************************************************************

#*******************************--FUNCTION extrac_xml_data--****************************************************************************************
# Se define una función llamada 'extract_xml_data' que toma como argumentos una lista de contentid y una fecha de registro. 
# La función se conecta a una base de datos PostgreSQL y a un recurso S3 de AWS usando las bibliotecas psycopg2 y boto3 respectivamente. 
# Luego, para cada contentid en la lista, la función extrae un archivo XML de S3 y lo analiza con la biblioteca ElementTree de Python para 
# extraer información sobre el contenido y almacenarla en la base de datos PostgreSQL. La función devuelve una lista de contentid que 
# fueron procesados exitosamente.
def extract_xml_data(contentid_list, DATE_LOG): #Se define la función llamada extract_xml_data que acepta dos argumentos: contentid_list y DATE_LOG.
    try:
        list_OkXmlData = [] #Se crea una lista vacía llamada list_OkXmlData.
        postgresql=psycopg2.connect(data_base_connect_prod) #Se establece la conexión a la base de datos PostgreSQL utilizando la información proporcionada en la variable data_base_connect. 
        curpsql=postgresql.cursor() #Se crea un cursor curpsql para ejecutar consultas en la base de datos.
        aws_session=boto3.Session(profile_name=aws_profile) #Se establece una sesión de AWS utilizando el perfil especificado en la variable aws_profile.
        s3_resource=aws_session.resource('s3') #Se crea un recurso s3_resource para acceder a los objetos de Amazon S3.
        for d in contentid_list: #Se itera a través de cada elemento en la lista contentid_list.
            contentid=d[0] #Se extrae el valor del primer elemento de cada tupla en la lista y se almacena en la variable contentid.
            bucket = Buckets[contentid[6:8]][0] # Se extrae el primer elemento del dicionario Buckets segun el key que contenga la variable contentid y se almacena en la variable 'bukect'.
            Folder= Buckets[contentid[6:8]][1] # Se extrae el segundo elemento del dicionario Buckets segun el key que contenga la variable contentid y se almacena en la variable 'Folder'.
            Object_key = Folder+'/'+contentid+'/'+contentid+'.xml' #se utilizan los valores de bucket y la variable Folder para defiir el nombre del bucket y la ruta del objeto XML en Amazon S3.
            xml_data=s3_resource.Bucket(bucket).Object(Object_key).get()['Body'].read().decode('utf-8') #Se obtiene el objeto XML desde Amazon S3, Se lee el contenido del objeto y se almacena en la variable xml_data.
            xml_root = ET.fromstring(xml_data) #Se utiliza la función ET.fromstring del módulo ElementTree para analizar el contenido XML y se almacena en la variable xml_root.
            contentType = xml_root.find("contentType").text #Se extrae el valor de la etiqueta contentType del XML y se almacena en la variable contentType.
            data=[ #se crea una lista llamada data que contiene el valor extraido de las etiquetas externalId y channel, como tambien la variable contentType.
                xml_root.find("externalId").text, # contentid
                contentType, # contenttype
                xml_root.find('channel').text #channel
            ]
            for title in xml_root.iter('title'): #se itera a través de las etiquetas title en el XML.
                data.append(title.text) #Se agrega el valor a la lista data.
                break
            if contentType=="movie": #Se comprueba el valor de contentType.
                data.append("na") #Si es igual a "movie", se agrega "na" a la lista data como valor para serietitle.
            elif contentType=="episode":
                for serietitle in xml_root.iter("seriesTitle"):# Si es igual a episode se itera a traves de la etiqueta seriesTitle 
                    data.append(serietitle.text) #Se agrega el primer valor a la lista data.
                    break
            data.append(xml_root.find("release").text) #Se agrega el valor de la etiqueta release a la lista data.
            if contentType=='movie': #Si contendtype es movie se agrga no aplica a la lista data.
                data.append('na') #season
                data.append('na') #episode
            elif contentType=='episode': #Si el contenido es episode se agregan los valores de las etiquetas season y episode a la lista data
                data.append(xml_root.find("season").text) #season
                data.append(xml_root.find("episode").text) #episode
                
            for genre in xml_root.iter('genre'): #Se itera a traves de la etiqueta genre y se agrega el primer valor a la lista data 
                data.append(genre.text) #genre
                break
            for rating in xml_root.iter('rating'): #Se itera a traves de la etiqueta rating y se agrega el primer valor a la lista data 
                data.append(rating.text) #rating
                break
                    
            dur=xml_root.find("duration").text #Se almacena el valor de la etiqueta duration en la variable dur.
            duration_split=dur.split(':') #Separamos en forma de lista los digitos numericos del dato dur.
            duration=Duration_Transform(duration_split) #Se ejecuta la funcion Duration_Transform la cual transforma y entrega el dato en segundos, se almacena el mismo en la variable diration 
            data.append(duration) #Se agrega el valor de la variable duration a la lista data.
            
            print(data) # Se imprime la lista data.
            SQL="INSERT INTO xmldata VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" #Sentencia SQL para insertar datos en la tabla xmldata
            DATA=( #Datos a registrar con la sentencia anterior
                data[0], #contentid
                data[1], #contenttype
                data[2], #channel
                data[3], #title
                data[4], #serietitle
                data[5], #releaseyear
                data[6], #season
                data[7], #episode
                data[8], #genre
                data[9], #rating
                data[10] #duration
            )
            curpsql.execute(SQL,DATA) #Se ejecuta la sentencia SQL con los datos especificados en la tupla DATA
            list_OkXmlData.append(contentid) #Se agrega el contentid a las lista list_OkXmlData
        postgresql.commit() #Se confirman los cambios en la base de datos
        postgresql.close() #Se cierra la conexion con la base de datos
        return list_OkXmlData #Retorna la lista de contentid a los cuales se extrajo los datos del xml.
        #print(xml_data)
    except:
        postgresql.close() #Se cierra la conexion con la base de datos
        error=sys.exc_info()[2] #-------Captura del error que arroja el sistema
        errorinfo=traceback.format_tb(error)[0] #-Captura el detalle del error
        print(errorinfo, str(sys.exc_info()[1])) #-Se agrega al diccionario detalle del error generado
        return [errorinfo, str(sys.exc_info()[1])]

#************************************************************--END--*******************************************

#*************************************--FUNCTION Duration_Transform--***************************************
# Funcion que permite transfomar le dato de duracion del contenido cuando este no cumple el formato HH:MM:SS.
#y convierte el dato en segundos. Toma como argumento la lista de digitos del dato de duracion del contenido
# y devuelve el mismo en segundos
def Duration_Transform(duration):
    while len(duration)>3:
        duration.remove('')
    duration_seconds=str(
        round(
        	datetime.timedelta(
    		    hours=int(duration[0]),
	    	    minutes=int(duration[1]),
		        seconds=int(duration[2])
                ).total_seconds()
            )
	    )
    return duration_seconds
#*****************************************--END--*************************************************************

#*********************--FUNCTION print_log--************************************************************
#Funcion que permite escribir texto en un archivo txt. Toma como argumento la variable TEXT que corresponde al texto a imprimir en el archivo
#y la variable DATE_LOG que corresponde a la fecha de creacion del log. No devuelve.
def print_log(TEXT, DATE_LOG):
    log_file=open(f"{log_Path}/{DATE_LOG}_log.txt", "a")
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n")
    log_file.close()
#************************************--END--************************************************************

#********************--FUNCTION sql_statement--*****************************
#Funcion que permite generar una sentecia SQL a partir de una variable como argumento.
#Devuelve la sentencia SQL
def sql_statement(date):
    sql=f"""INSERT INTO playbacks
SELECT 
new_manifests.datetime,
new_manifests.country,
new_manifests.mso,
new_manifests.device,
SUBSTR(new_manifests.client, 3),
SUBSTR(new_manifests.contentid, 3),
xmldata.contenttype,
xmldata.channel,
xmldata.title,
xmldata.serietitle,
xmldata.releaseyear,
xmldata.season,
xmldata.episode,
xmldata.genre,
xmldata.rating,
xmldata.duration,
COUNT(new_segmentos.manifestid) * cast(new_manifests.segduration AS INTEGER) as segduration
FROM new_manifests
LEFT JOIN new_segmentos ON new_manifests.manifestid = new_segmentos.manifestid
LEFT JOIN xmldata ON SUBSTR(new_manifests.contentid, 3) = xmldata.contentid
WHERE new_manifests.datetime LIKE '%{date}%'
GROUP BY new_manifests.datetime,
new_manifests.country,
new_manifests.mso,
new_manifests.device,
new_manifests.client,
new_manifests.contentid,
xmldata.contenttype,
xmldata.channel,
xmldata.title,
xmldata.serietitle,
xmldata.releaseyear,
xmldata.season,
xmldata.episode,
xmldata.genre,
xmldata.rating,
xmldata.duration,
new_manifests.segduration;"""
    return sql
#***************************************--END--****************************

def Flag_Status(OPTION):
    with open(json_path, "r") as json_file:
            json_data=json.load(json_file)
    if OPTION=="r":
        return json_data["FLAG"]
    elif OPTION=="w":
        json_data["FLAG"]=False
        with open(json_path, "w") as json_file:
            json.dump(json_data, json_file)
    else:
        pass

def segmentsDelete(date_in):
    try:
        psql_db=psycopg2.connect(data_base_connect_prod)
        psql_cursor=psql_db.cursor()
        date=date_in-relativedelta(days=3)
        date_sql=str(datetime.datetime.strftime(date, "%Y-%m-%d"))
        psql_cursor.execute(f"DELETE FROM new_segmentos WHERE datetime LIKE'%{date_sql}%';")
        text_mail=psql_cursor.statusmessage
        psql_db.commit()
        psql_cursor.close()
        psql_db.close()
        return text_mail
    except:
        return "error segmentsdelete"
