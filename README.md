# Henry Labs #1 - Data Engineer
### Autor: Maico Bernal
email de contacto: bernalmaico@gmail.com

## Proceso de ETL con carga incremental
Bievenido! Este es un proyecto individual para confeccionar en 72hs un proyecto desde cero de ETL con carga incremental.
Los datos corresponden a una lista de precios del año 2020, si bien no se especifica, es probable que sean del programa Precios Cuidados. 

## Objetivos:
- ETL automatizado de archivos locales de múltiples formatos
- Limpieza de datos y depuración automatizada
- Gestión del flujo de trabajo vía Apache Airflow / DAGs
- Carga de datos a MySQL (servidor local)
- Incorporación de Multi-Cloud Object Storage compatible con AWS S3 (uso de sensors y hooks)
- Visualización básica en HTML de querys via Flask

## Video explicativo de su funcionalidad: [LINK](https://youtu.be/O7SpR_09q3A)

## Esquema
![](https://github.com/maicobernal/henrylab1/blob/main/images/diagrama.png)


## Principales componentes:
##### - Entorno de trabajo: MacBook Pro con chip M2 - Todas las imagenes de Docker son Unix/ARM64
##### - Database: MySQL Community Server 8.0.30
##### - Docker Desktop 4.12
##### - Airflow 2.42 Official Docker Image con Python 3.7: Solamente Webserver, Scheduler y Postgre
##### - Image Extending para instalación de librerias adicionales en Airflow (Pandas, SQLAlchemy, Flask)
##### - MinIO Object Storage Server RELEASE.2022-10-24T18-35-07Z 



## Instalación de containers:
#### Puede variar segun OS y arquitectura(testeado en ARM64/MacOS Monterrey 12.6)
1) MinIO RootFull, ver documentación oficial segun OS --> [LINK](https://min.io/docs/minio/container/index.html)

- Instalación:
```rb
mkdir -p ~/minio/data
docker run \
   -p 9000:9000 \
   -p 9090:9090 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=ROOTNAME" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   quay.io/minio/minio server /data --console-address ":9090"
```

Luego conectar desde Airflow entrando en Config --> Connections --> Generic.
- En conn_ID colocar: "minio_conn" sin comillas
- En el campo Extra colocar:

```rb
{
    "aws_access_key_id": "ROOTNAME", 
    "aws_secret_access_key": "CHANGEME123", 
    "host": "http://host.docker.internal:9000"
}
```

2) Airflow: pasos secuenciales, ver documentación oficial --> [LINK](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

```rb
0) mkdir ./dags ./logs ./plugins
1) docker-compose up airflow-init
2) docker-compose up
3) docker-compose down
4) docker-compose build (instala las librerias de Python en el container de Airflow)
5) docker-compose up
```

## Python: descripción de principales funciones (ETL.py | Functions.py)

<b>- FileImporter:</b> carga individual de archivos base (sucursales y productos)
<b>- FolderImporterPrecios:</b> detección automática de archivos en directorio, soporte de múltiples formatos (XLSX, TXT, CSV, Parquet, JSON) y limpieza posterior. Devuelve un archivo único con valores estandarizados. 
<b>- CleanSucursal, CleanProducto, CleanPrecios:</b> Normalización de los datos.
<b>- DownloadAndRenameFile:</b> Gestiona la descarga de archivos desde el Bucket de Minio. 
<b>- Dag_Initial_Loading:</b> Gestiona la carga/cleaning y upload a MySQL incremental de archivos locales en Airflow
<b>- Dag_S3Bucket_Loading:</b> Gestiona el S3Sensor para vigilar nuevos archivos que se carguen a Minio Bucket y luego activa el S3Hook para realizar descarga, renombre, limpieza y upload a MySQL de los archivos de forma incremental en Airflow. Al hacer un trigger de prueba tiene una ventana de 30 segundos para hacer la carga (se puede modificar). 

## Airflow: Dos DAGs principales:
### Initial_Loading
![](https://github.com/maicobernal/henrylab1/blob/main/images/dag1.png)

Gestiona la carga incremental de los archivos iniciales, a posterior se hace una limpieza y se almacena temporalmente en CSV (Airflow no permite returns entre tasks de más de 48kb), y posteriormente se realiza la carga en la base de datos de MySQL, la cuál corre de forma local en la computadora. 

### DAG_Minio_S3_Wait_for_File
![](https://github.com/maicobernal/henrylab1/blob/main/images/dag2.png)
![](https://github.com/maicobernal/henrylab1/blob/main/images/minio.png)

Este DAG consta de dos partes: Para su correcta funcionalidad se realizó la conexión pertinente entre Airflow y Minio Storage Service (el cuál corre a traves de un contenedor de Docker). 
Minio permite la utilización de la API de AWS para la gestión de archivos vía Airflow, lo cuál permitiría en una instancia posterior facilitar en deploy en la nube. 
El primer paso es utilizar un S3.Sensor para vigilar cada X intervalo de tiempo si se cargan nuevos archivos en el Bucket llamado 'Data'.
Una vez detectado un archivo nuevo se procede a su identificación, descarga local en Airflow, transformación y carga en SQL.

Tanto este DAG como el previo finalizan con un Query de prueba a MySQL con el QUERY solicitado en la consigna: Precio promedio de la sucursal 9-1-688

### Flask
No hubo tiempo para hacer mejoras estéticas, pero si se corre el archivo 'main.py' se puede acceder de forma local en el navegador a la API que devuelve el resultado de la QUERY de la consigna. 

### Aspectos a mejorar/cosas pendientes: 
-Script con programación orientada a objetos
-Nuevas funcionalidades con DAG
-El entorno de trabajo es Unix/ARM64, hay que verificar su puesta en marcha en sistemas x86 u otros ya que las imágenes de Docker cambian. 
-Deploy en AWS
-Utilizar Flask y HTML para generar un web-deploy con API REST que permita comandar distintas QUERYs básicas


### Nota:
El archivo Excel tenía una de sus columnas con formato múltiple (datetime y float) lo cuál hacia imposible la importación correcta con Pandas con el engine OpenPyXL. Según lo investigado en StackOverFlow es una limitación del engine que genera un comportamiento azaroso y fuerza en algunas ocasiones los datos a datetime a pesar de especificar lo opuesto, y por lo pronto no hay otro engine que soporte Pandas y XLSX. Corregido esto no hubo inconvenientes en la carga y transformación. 