# Amankay – Pipeline de datos de emergencias (Perú)

Pipeline fuente → bronze → silver → gold → integración, con datos de Open-Meteo, IGP, INDECI, INGEMMET y noticias (Google News).
Los archivos viven en un bucket de Amazon S3 y el código se ejecuta en Databricks.

## Estructura
```
notebooks/pipeline_amankay_databricks.py      # pipeline completo (notebook de Databricks)
notebooks/actualizar_credenciales_aws.py      # guarda las llaves AWS en el secret scope
.github/workflows/sync-databricks.yml         # sincroniza GitHub -> Git folder
```

## 1. Repositorio GitHub ⇄ Git folder de Databricks
1. Sube este repo a GitHub (cada integrante trabaja en su rama y une a `develop` con pull request).
2. En Databricks: *Workspace → Create → Git folder* → URL del repo → proveedor GitHub → usuario y token personal (permiso `repo`).
3. En GitHub: *Settings → Secrets and variables → Actions*: crea `DATABRICKS_HOST` (URL del workspace) y `DATABRICKS_TOKEN`.
4. En el workflow, ajusta `GIT_FOLDER` a la ruta de tu Git folder.
5. Cada push a `develop` ejecuta el workflow y deja el Git folder en el último commit.

## 2. Conexión Databricks ⇄ Amazon S3
Las llaves de AWS no van en el código: se guardan en un **secret scope** llamado `amankay`
(`aws_access_key`, `aws_secret_key`, `aws_session_token`).
- Con CLI: `databricks secrets create-scope amankay` y `databricks secrets put-secret amankay aws_access_key` (igual para las otras dos).
- O ejecutando `actualizar_credenciales_aws.py`.

El pipeline lee los secretos, valida las credenciales y trabaja con boto3. Si vencen a mitad de la corrida, avisa,
espera (hasta 10 min) a que actualices los secretos y continúa donde se quedó.

La lectura de **CSV y Excel** desde S3 está en la última celda del notebook: descarga el archivo con boto3 y lo
convierte a un DataFrame de Spark (`leer_s3_csv_spark`, `leer_s3_excel_spark`).

## 3. Ejecución
1. Abre `pipeline_amankay_databricks.py` desde el Git folder y adjunta cómputo.
2. Revisa `FUENTES` y `ETAPAS` al inicio y el widget `bucket`.
3. Ejecuta todas las celdas. Es idempotente: si algo ya existe en S3, no se vuelve a descargar.


## 4. Comando - Secret and Token (AWS)
     aws configure export-credentials --format env