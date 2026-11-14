#####Utilizaremos Pyspark para generar datos random de SAP B1 y guardarlos en tablas Delta en la capa Bronze de nuestro Lakehouse.

import random
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession

# Inicializar sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Semilla para reproducibilidad de los datos sintéticos
random.seed(42)
num_records = 100
start_date = datetime(2025, 1, 1)

print("--- Iniciando generación de datos sintéticos SAP B1 (Capa Bronze) ---")

# 1. Crear esquema (Schema / Database) en Unity Catalog
spark.sql("CREATE CATALOG IF NOT EXISTS workspace;")
spark.sql("CREATE DATABASE IF NOT EXISTS workspace.sap_b1_bronze;")
spark.sql("USE CATALOG workspace;")
spark.sql("USE DATABASE sap_b1_bronze;")

# --- 2. OITM: Artículos / Productos ---
item_categories = [101, 102, 103, 104]
items_data = []
for i in range(1, num_records + 1):
  item_code = f"ITM{i:04d}"
  items_data.append({
      "ItemCode": item_code,
      "ItemName": f"Producto SAP B1 #{i}",
      "ItmsGrpCod": random.choice(item_categories),
      "AvgPrice": round(random.uniform(15.0, 850.0), 2),
  })
df_oitm = pd.DataFrame(items_data)

# --- 3. OINV: Encabezado de Facturas de Ventas ---
customers = [f"CUST{val:04d}" for val in range(1, 26)]
oinv_data = []
for i in range(1, num_records + 1):
  doc_date = start_date + timedelta(days=random.randint(0, 300))
  oinv_data.append({
      "DocEntry": i,
      "DocNum": 70000 + i,
      "CardCode": random.choice(customers),
      "DocDate": doc_date.strftime("%Y-%m-%d"),
      "DocTotal": 0.0,  # Se calcula con base en INV1
  })
df_oinv = pd.DataFrame(oinv_data)

# --- 4. INV1: Detalle de Facturas de Ventas ---
inv1_data = []
for i in range(1, num_records + 1):
  num_lines = random.randint(1, 3)  # 1 a 3 líneas por factura
  for l in range(num_lines):
    item = random.choice(items_data)
    quantity = random.randint(1, 12)
    price = item["AvgPrice"]
    inv1_data.append({
        "DocEntry": i,
        "LineNum": l,
        "ItemCode": item["ItemCode"],
        "Quantity": quantity,
        "Price": price,
        "LineTotal": round(quantity * price, 2),
    })
df_inv1 = pd.DataFrame(inv1_data)

# Actualizar el total en OINV sumando los detalles de INV1
totals = df_inv1.groupby("DocEntry")["LineTotal"].sum().to_dict()
df_oinv["DocTotal"] = df_oinv["DocEntry"].map(totals).fillna(0.0)

# --- 5. OJDT & JDT1: Asientos Contables (Journal Entries) ---
ojdt_data = []
jdt1_data = []
for i in range(1, num_records + 1):
  trans_id = 90000 + i
  ref_date = start_date + timedelta(days=random.randint(0, 300))
  ojdt_data.append({
      "TransId": trans_id,
      "RefDate": ref_date.strftime("%Y-%m-%d"),
      "Memo": f"Asiento automático SAP B1 - Transacción {i}",
  })

  monto = round(random.uniform(150.0, 12000.0), 2)
  jdt1_data.extend([
      {
          "TransId": trans_id,
          "Line_ID": 0,
          "Account": "1101-Caja General",
          "Debit": monto,
          "Credit": 0.0,
      },
      {
          "TransId": trans_id,
          "Line_ID": 1,
          "Account": "4101-Ventas Netas",
          "Debit": 0.0,
          "Credit": monto,
      },
  ])

df_ojdt = pd.DataFrame(ojdt_data)
df_jdt1 = pd.DataFrame(jdt1_data)

# --- 6. Persistir en Tablas Delta (Capa Bronze) ---
tables_to_save = {
    "bronze_oitm": df_oitm,
    "bronze_oinv": df_oinv,
    "bronze_inv1": df_inv1,
    "bronze_ojdt": df_ojdt,
    "bronze_jdt1": df_jdt1,
}

for table_name, pdf in tables_to_save.items():
  sdf = spark.createDataFrame(pdf)
  sdf.write.mode("overwrite").saveAsTable(table_name)
  print(f"-> Tabla '{table_name}' guardada exitosamente en Unity Catalog.")

print("¡Proceso de ingesta Bronze completado con éxito!")