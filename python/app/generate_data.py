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


def create_staging_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_orders (
                order_id INTEGER PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL
            )
        """)


def generate_data(connection):
    orders = [
        (1001, "Customer A", "Laptop", 2, 850.00),
        (1002, "Customer B", "Monitor", 3, 275.50),
        (1003, "Customer C", "Keyboard", 5, 79.95),
    ]

    now = datetime.now(timezone.utc)
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO staging_orders (
                order_id, customer_name, product_name,
                quantity, unit_price, generated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                product_name = EXCLUDED.product_name,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                generated_at = EXCLUDED.generated_at
            """,
            [(*order, now) for order in orders],
        )


def main():
    print("Generating demo data...")
    with get_connection() as connection:
        create_staging_table(connection)
        generate_data(connection)
        connection.commit()
    print("Demo data generated successfully.")


if __name__ == "__main__":
    main()
