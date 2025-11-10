import boto3
import uuid
import os
import json # <-- Importar json

# Crear clientes de AWS
# fuera del handler
# para reutilizar conexiones
dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    
    # Obtener nombres
    # desde variables de entorno
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_ingesta = os.environ["BUCKET_NAME"] # <-- NUEVO

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        }
    }
    
    # 1. Grabar en DynamoDB (como antes)
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario)
    
    # --- NUEVO: Grabar JSON en S3 ---
    
    # Definir nombre
    # del archivo en S3
    nombre_archivo_s3 = f"{uuidv1}.json" 
    
    # Subir el archivo
    # al bucket S3
    s3.Object(
        bucket_ingesta, 
        nombre_archivo_s3
    ).put(
        Body=json.dumps(comentario) # <-- Convertir dict a JSON
    )
    
    # --- Fin de la sección S3 ---

    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response_dynamo': response_dynamo
    }