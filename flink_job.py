#!/usr/bin/env python3
import os
import sys
import json
import time
from dataclasses import dataclass
from datetime import datetime
import psycopg2
import hashlib

os.environ['PYFLINK_CLIENT_EXECUTABLE'] = 'python3'

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcExecutionOptions,
    JdbcConnectionOptions
)
from pyflink.common import Row, Types

class DimensionEnrichment:
    def __init__(self, pg_host: str, pg_port: str, db_name: str, user: str, pwd: str):
        try:
            self.conn = psycopg2.connect(
                host=pg_host,
                database=db_name,
                user=user,
                password=pwd,
                port=pg_port
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise
        
    def upsert_and_get_sk(self, table: str, business_key: tuple, columns: dict) -> int:
        try:
            cols = ', '.join(columns.keys())
            vals = tuple(columns.values())
            placeholders = ', '.join(['%s'] * len(vals))
            
            if table == "dim_date":
                conflict_cols = "full_date"
            elif table == "dim_customer":
                conflict_cols = "customer_id"
            elif table == "dim_product":
                conflict_cols = "product_id"
            elif table == "dim_seller":
                conflict_cols = "seller_id"
            elif table == "dim_store":
                conflict_cols = "store_id"
            elif table == "dim_supplier":
                conflict_cols = "supplier_id"
            else:
                conflict_cols = business_key[0]
            
            set_parts = []
            for col in columns.keys():
                if col != conflict_cols:
                    set_parts.append(f"{col} = EXCLUDED.{col}")
            
            set_clause = ', '.join(set_parts) if set_parts else f"{table.replace('dim_', '')}_key = EXCLUDED.{table.replace('dim_', '')}_key"
            
            sql = f"""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {set_clause}
                RETURNING {table.replace('dim_', '')}_key
            """
            
            self.cur.execute(sql, vals)
            result = self.cur.fetchone()
            self.conn.commit()
            
            if result:
                return result[0]
            else:
                raise Exception("No key returned from upsert")
                
        except Exception as e:
            self.conn.rollback()
            print(f"Error in upsert for table {table}: {e}")
            raise

@dataclass
class EnrichedRecord:
    date_key: int
    customer_key: int
    seller_key: int
    product_key: int
    store_key: int
    supplier_key: int
    quantity: int
    unit_price: float
    total_price: float
    discount: float = 0.0
    shipping_cost: float = 0.0
    tax_amount: float = 0.0
    net_price: float = 0.0

class EnrichmentMapFunction(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        pg_host = "postgres"
        pg_port = "5432"
        db_name = "postgres"
        user = os.getenv("POSTGRES_USER", "postgres")
        pwd = os.getenv("POSTGRES_PASSWORD", "mypassword")
        
        self.enricher = DimensionEnrichment(pg_host, pg_port, db_name, user, pwd)
    
    def map(self, value: str) -> EnrichedRecord:
        try:
            record = json.loads(value)
            sale_id = record.get("id", "unknown")
            
            sale_date = record.get("sale_date", "")
            dt = None
            
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y"):
                try:
                    dt = datetime.strptime(sale_date, fmt)
                    break
                except ValueError:
                    continue
            
            if not dt:
                dt = datetime.now()
            
            full_date = dt.strftime("%Y-%m-%d")
            
            date_key = self.enricher.upsert_and_get_sk(
                "dim_date",
                ("full_date",),
                {
                    "full_date": full_date,
                    "year": dt.year,
                    "quarter": (dt.month - 1) // 3 + 1,
                    "month": dt.month,
                    "month_name": dt.strftime("%B"),
                    "day": dt.day,
                    "day_of_week": dt.weekday() + 1,
                    "day_name": dt.strftime("%A"),
                    "is_weekend": dt.weekday() >= 5,
                },
            )
            
            customer_key = self.enricher.upsert_and_get_sk(
                "dim_customer",
                ("customer_id",),
                {
                    "customer_id": int(record.get("sale_customer_id", 0)),
                    "first_name": record.get("customer_first_name", ""),
                    "last_name": record.get("customer_last_name", ""),
                    "age": int(record.get("customer_age", 0)),
                    "email": record.get("customer_email", ""),
                    "country": record.get("customer_country", ""),
                    "postal_code": record.get("customer_postal_code", ""),
                    "pet_type": record.get("customer_pet_type", ""),
                    "pet_name": record.get("customer_pet_name", ""),
                    "pet_breed": record.get("customer_pet_breed", ""),
                },
            )
            
            seller_key = self.enricher.upsert_and_get_sk(
                "dim_seller",
                ("seller_id",),
                {
                    "seller_id": int(record.get("sale_seller_id", 0)),
                    "first_name": record.get("seller_first_name", ""),
                    "last_name": record.get("seller_last_name", ""),
                    "email": record.get("seller_email", ""),
                    "country": record.get("seller_country", ""),
                    "postal_code": record.get("seller_postal_code", ""),
                },
            )
            
            product_key = self.enricher.upsert_and_get_sk(
                "dim_product",
                ("product_id",),
                {
                    "product_id": int(record.get("sale_product_id", 0)),
                    "product_name": record.get("product_name", ""),
                    "category": record.get("product_category", ""),
                    "price": float(record.get("product_price", 0.0)),
                    "weight": float(record.get("product_weight", 0.0)),
                    "color": record.get("product_color", ""),
                    "size": record.get("product_size", ""),
                    "brand": record.get("product_brand", ""),
                    "material": record.get("product_material", ""),
                    "description": record.get("product_description", ""),
                    "rating": float(record.get("product_rating", 0.0)),
                    "reviews": int(record.get("product_reviews", 0)),
                    "release_date": record.get("product_release_date", full_date),
                    "expiry_date": record.get("product_expiry_date", None),
                    "pet_category": record.get("pet_category", ""),
                },
            )
            
            store_name = record.get("store_name", "")
            store_id = abs(hash(store_name)) % 1000000
            store_key = self.enricher.upsert_and_get_sk(
                "dim_store",
                ("store_id",),
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "location": record.get("store_location", ""),
                    "city": record.get("store_city", ""),
                    "state": record.get("store_state", ""),
                    "country": record.get("store_country", ""),
                    "phone": record.get("store_phone", ""),
                    "email": record.get("store_email", ""),
                },
            )
            
            supplier_name = record.get("supplier_name", "")
            supplier_id = abs(hash(supplier_name)) % 1000000
            supplier_key = self.enricher.upsert_and_get_sk(
                "dim_supplier",
                ("supplier_id",),
                {
                    "supplier_id": supplier_id,
                    "supplier_name": supplier_name,
                    "contact": record.get("supplier_contact", ""),
                    "email": record.get("supplier_email", ""),
                    "phone": record.get("supplier_phone", ""),
                    "address": record.get("supplier_address", ""),
                    "city": record.get("supplier_city", ""),
                    "country": record.get("supplier_country", ""),
                },
            )
            
            print(f"Processing record ID: {sale_id}")
            print(f"Date key: {date_key}")
            print(f"Customer key: {customer_key}")
            print(f"Seller key: {seller_key}")
            print(f"Product key: {product_key}")
            print(f"Store key: {store_key}")
            print(f"Supplier key: {supplier_key}")
            
            quantity = int(record.get("sale_quantity", 1))
            total_price = float(record.get("sale_total_price", 0.0))
            unit_price = float(record.get("product_price", 0.0))
            discount = 0.0
            shipping_cost = 0.0
            tax_amount = total_price * 0.20
            net_price = total_price - discount + shipping_cost + tax_amount
            
            return EnrichedRecord(
                date_key=date_key,
                customer_key=customer_key,
                seller_key=seller_key,
                product_key=product_key,
                store_key=store_key,
                supplier_key=supplier_key,
                quantity=quantity,
                unit_price=unit_price,
                total_price=total_price,
                discount=discount,
                shipping_cost=shipping_cost,
                tax_amount=tax_amount,
                net_price=net_price,
            )
            
        except Exception as e:
            print(f"Error processing record: {e}")
            raise

def main():
    print("Starting Flink Star Schema Job...")
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "test-topic")
    pg_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/postgres")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "mypassword")
    
    print(f"Kafka: {kafka_bootstrap}")
    print(f"Topic: {kafka_topic}")
    print(f"PostgreSQL URL: {pg_url}")
    print(f"PostgreSQL User: {pg_user}")
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    consumer = FlinkKafkaConsumer(
        topics=[kafka_topic],
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"flink-star-group-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true"
        },
    )
    
    ds = env.add_source(consumer)
    enriched_ds = ds.map(EnrichmentMapFunction())
    
    row_ds = enriched_ds.map(
        lambda e: Row(
            e.customer_key,
            e.product_key,
            e.seller_key,
            e.store_key,
            e.supplier_key,
            e.date_key,
            e.quantity,
            e.unit_price,
            e.total_price,
            e.discount,
            e.shipping_cost,
            e.tax_amount,
            e.net_price,
            f"sale-{int(time.time())}",
            "credit_card",
            "delivered",
        ),
        output_type=Types.ROW([
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ])
    )
    
    insert_sql = """
        INSERT INTO fact_sale (
            customer_key, product_key, seller_key, store_key, 
            supplier_key, date_key, quantity, unit_price, 
            total_price, discount, shipping_cost, tax_amount,
            net_price, original_sale_id, payment_method, delivery_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    jdbc_connection = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(pg_url) \
        .with_driver_name("org.postgresql.Driver") \
        .with_user_name(pg_user) \
        .with_password(pg_pass) \
        .build()
    
    execution_options = JdbcExecutionOptions.builder() \
        .with_batch_size(100) \
        .with_batch_interval_ms(500) \
        .with_max_retries(3) \
        .build()
    
    jdbc_sink = JdbcSink.sink(
        insert_sql,
        Types.ROW([
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ]),
        jdbc_connection,
        execution_options
    )
    
    row_ds.add_sink(jdbc_sink)
    
    print("Starting Flink job execution...")
    env.execute("Star Schema Data Pipeline")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error in main: {e}")
        sys.exit(1)