#!/usr/bin/env python
#_*_ codig: utf8 _*_
log_Path="./logs" # Ruta del folder donde se alojan los archivos logs
data_base_connect_prod="host=10.10.130.38 dbname=cdndb user=vodtransfers3 password=vod-2022" #Datos para establecer conexion con la base de datos de produccion.
aws_profile='pythonapps' #Nombre del perfil de aconexion a AWS
Mail_To='IngenieriaVCMC@vcmedios.com.co' #E-mail de destino 
Buckets={   #Diccionario con keys y values que identifican el canal y el bucket segun contentid.
    "11": ["aenla-in-toolbox", "A&E"],
    "21": ["aenla-in-toolbox", "History"],
    "31": ["aenla-in-toolbox", "Lifetime"],
    "41": ["aenla-in-toolbox", "History2"],
    "62": ["spe-in-toolbox", "SONY-AXN"],
    }