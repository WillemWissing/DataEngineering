from airflow import settings
from airflow.models import Connection
from airflow.utils.session import create_session

# Define the connection details for Spark
conn_id = 'spark_default'  # Connection ID used in Airflow tasks
conn_type = 'spark'  # Type of connection (for Spark, it's 'spark')
host = 'spark://spark-master'  # URL for the Spark master
schema = 'spark'  # Optional: use 'spark' schema
port = 7077  # Spark master port
extra = '{"deploy_mode": "client"}'  # Additional Spark config (deploy mode)

def create_spark_connection():
    """
    Creates a Spark connection in Airflow if it doesn't already exist.
    The connection is defined by the variables set above.
    """
    with create_session() as session:
        # Check if the connection with the specified conn_id already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if not existing_conn:
            # If connection does not exist, create and add it to the session
            new_conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                schema=schema,
                port=port,
                extra=extra
            )
            session.add(new_conn)  # Add new connection to the session
            session.commit()  # Commit the session to save the connection
            print(f"Connection '{conn_id}' created successfully.")
        else:
            print(f"Connection '{conn_id}' already exists.")

# Run the function to create the Spark connection when executed as a script
if __name__ == "__main__":
    create_spark_connection()
