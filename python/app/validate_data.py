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


def validate_data(connection):
    """Validate data in the staging table."""
    with connection.cursor() as cursor:

        # Check that staging contains data
        cursor.execute("""
            SELECT COUNT(*)
            FROM staging_orders
        """)

        row_count = cursor.fetchone()[0]

        if row_count == 0:
            raise ValueError("Validation failed: staging_orders contains no data.")

        # Check for invalid records
        cursor.execute("""
            SELECT COUNT(*)
            FROM staging_orders
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
                f"Validation failed: {invalid_rows} invalid rows found."
            )

        return row_count


def main():
    print("Validating staging data...")

    with get_connection() as connection:
        row_count = validate_data(connection)

    print(f"Validation successful. {row_count} rows validated.")


if __name__ == "__main__":
    main()