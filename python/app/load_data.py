import os
from datetime import datetime, timezone

import psycopg


def get_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def create_orders_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL
            )
        """)


def load_data(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO orders (
                order_id, customer_name, product_name,
                quantity, unit_price, loaded_at
            )
            SELECT
                order_id, customer_name, product_name,
                quantity, unit_price, %s
            FROM staging_orders
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                product_name = EXCLUDED.product_name,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                loaded_at = EXCLUDED.loaded_at
            """,
            (datetime.now(timezone.utc),),
        )
        return cursor.rowcount


def main():
    print("Loading validated data...")
    with get_connection() as connection:
        create_orders_table(connection)
        rows_processed = load_data(connection)
        connection.commit()
    print(f"Load completed successfully. {rows_processed} rows processed.")


if __name__ == "__main__":
    main()
