# Databricks notebook source
# MAGIC %md
# MAGIC # Actualizar credenciales AWS (secret scope)
# MAGIC Pega las 3 llaves de la sesión actual en los widgets y ejecuta la celda. Los valores se guardan en el secret scope
# MAGIC y los widgets se borran al terminar. Úsalo en una pestaña aparte mientras el pipeline espera credenciales nuevas.

# COMMAND ----------

dbutils.widgets.text("scope", "amankay", "Secret scope")
dbutils.widgets.text("access_key", "", "Access key ID")
dbutils.widgets.text("secret_key", "", "Secret access key")
dbutils.widgets.text("session_token", "", "Session token")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

scope = dbutils.widgets.get("scope").strip()
valores = {
    "aws_access_key": dbutils.widgets.get("access_key").strip(),
    "aws_secret_key": dbutils.widgets.get("secret_key").strip(),
    "aws_session_token": dbutils.widgets.get("session_token").strip(),
}
if not valores["aws_access_key"] or not valores["aws_secret_key"]:
    raise ValueError("Falta la access key o la secret key")

w = WorkspaceClient()
try:
    w.secrets.create_scope(scope=scope)
    print(f"Scope '{scope}' creado")
except Exception as e:
    print(f"Scope '{scope}': {str(e)[:80]} (si ya existe, se usa el actual)")

for clave, valor in valores.items():
    if valor:
        w.secrets.put_secret(scope=scope, key=clave, string_value=valor)
        print(f"Guardado: {scope}/{clave}")

dbutils.widgets.removeAll()  # no dejar las llaves visibles en los widgets