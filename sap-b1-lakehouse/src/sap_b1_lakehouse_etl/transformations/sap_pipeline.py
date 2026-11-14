import random
from datetime import datetime, timedelta
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Row


# ==========================================
# CAPA BRONZE: Ingesta de Datos Randoom SAP B1
# ==========================================
@dlt.table(
    name="bronze_oitm",
    comment="Artículos y productos raw de SAP B1",
    table_properties={"quality": "bronze"},
)
def bronze_oitm():
  random.seed(42)
  items = []
  for i in range(1, 101):
    items.append(
        Row(
            ItemCode=f"ITM{i:04d}",
            ItemName=f"Producto SAP B1 #{i}",
            ItmsGrpCod=random.choice([101, 102, 103, 104]),
            AvgPrice=round(random.uniform(15.0, 850.0), 2),
        )
    )
  return spark.createDataFrame(items)


@dlt.table(
    name="bronze_oinv",
    comment="Encabezado de facturas de ventas raw de SAP B1",
    table_properties={"quality": "bronze"},
)
def bronze_oinv():
  random.seed(42)
  start_date = datetime(2025, 1, 1)
  invoices = []
  for i in range(1, 101):
    doc_date = start_date + timedelta(days=random.randint(0, 300))
    invoices.append(
        Row(
            DocEntry=i,
            DocNum=70000 + i,
            CardCode=f"CUST{random.randint(1, 25):04d}",
            DocDate=doc_date.strftime("%Y-%m-%d"),
        )
    )
  return spark.createDataFrame(invoices)


@dlt.table(
    name="bronze_inv1",
    comment="Detalle de facturas de ventas raw de SAP B1",
    table_properties={"quality": "bronze"},
)
def bronze_inv1():
  random.seed(42)
  details = []
  for i in range(1, 101):
    num_lines = random.randint(1, 3)
    for l in range(num_lines):
      details.append(
          Row(
              DocEntry=i,
              LineNum=l,
              ItemCode=f"ITM{random.randint(1, 100):04d}",
              Quantity=random.randint(1, 12),
              Price=round(random.uniform(15.0, 850.0), 2),
          )
      )
  return spark.createDataFrame(details)


# ==========================================
# CAPA SILVER: Limpieza y Enriquecimiento
# ==========================================
@dlt.table(
    name="silver_ventas_detalle",
    comment=(
        "Detalle de ventas enriquecido con nombres de productos y montos"
        " calculados"
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_quantity", "Quantity > 0")
def silver_ventas_detalle():
  inv1 = dlt.read("bronze_inv1")
  oinv = dlt.read("bronze_oinv")
  oitm = dlt.read("bronze_oitm")

  return (
      inv1.join(oinv, "DocEntry", "inner")
      .join(oitm, "ItemCode", "inner")
      .select(
          inv1.DocEntry,
          oinv.DocNum,
          oinv.CardCode,
          oinv.DocDate,
          inv1.ItemCode,
          oitm.ItemName,
          oitm.ItmsGrpCod,
          inv1.Quantity,
          inv1.Price,
          F.round(inv1.Quantity * inv1.Price, 2).alias("LineTotal"),
      )
  )


# ==========================================
# CAPA GOLD: Data Marts Analíticos para Power BI
# ==========================================
@dlt.table(
    name="gold_mart_ventas",
    comment="Data Mart de ventas por cliente y categoría de producto",
    table_properties={"quality": "gold"},
)
def gold_mart_ventas():
  df = dlt.read("silver_ventas_detalle")
  return df.groupBy("DocDate", "CardCode", "ItmsGrpCod").agg(
      F.sum("LineTotal").alias("TotalVentas"),
      F.sum("Quantity").alias("TotalUnidades"),
      F.countDistinct("DocNum").alias("TotalFacturas"),
  )