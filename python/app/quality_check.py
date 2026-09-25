import os

import psycopg


def get_connection():
    """Create a connection to the production database."""
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def quality_check(connection):
    """Perform quality checks on the production orders table."""
    with connection.cursor() as cursor:

        # Check that the target table contains data
        cursor.execute("""
            SELECT COUNT(*)
            FROM orders
        """)

        row_count = cursor.fetchone()[0]

        if row_count == 0:
            raise ValueError(
                "Quality check failed: orders contains no data."
            )

        # Check for invalid production records
        cursor.execute("""
            SELECT COUNT(*)
            FROM orders
            WHERE order_id IS NULL
               OR customer_name IS NULL
               OR customer_name = ''
               OR product_name IS NULL
               OR product_name = ''
               OR quantity <= 0
               OR unit_price <= 0
        """)

        invalid_rows = cursor.fetchone()[0]

        if invalid_rows > 0:
            raise ValueError(
                f"Quality check failed: {invalid_rows} invalid rows found."
            )

        cursor.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT order_id
                FROM orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            ) duplicates
        """)

        duplicate_orders = cursor.fetchone()[0]

        if duplicate_orders > 0:
            raise ValueError(
                f"Quality check failed: {duplicate_orders} duplicate orders found."
            )

        return row_count


def main():
    print("Running production data quality check...")

    with get_connection() as connection:
        row_count = quality_check(connection)

    print(
        f"Quality check successful. "
        f"{row_count} production rows verified."
    )


if __name__ == "__main__":
    main()