import pygridgain_dbapi
from pygridgain_dbapi import DatabaseError

def create_simple_connection():
  """Create a basic connection to GridGain cluster."""
  addr = ['127.0.0.1:10800']
  return pygridgain_dbapi.connect(address=addr, timeout=10)

def create_ssl_connection():
  """Create SSL-enabled connection to GridGain cluster."""
  addr = ['127.0.0.1:10800']
  return pygridgain_dbapi.connect(
      address=addr,
      timeout=10,
      use_ssl=True,
      ssl_keyfile='<path_to_ssl_keyfile.pem>',
      ssl_certfile='<path_to_ssl_certfile.pem>',
      # Optional: ssl_ca_certfile='<path_to_ssl_ca_certfile.pem>'
  )

def create_authenticated_connection():
  """Create authenticated connection to GridGain cluster."""
  addr = ['127.0.0.1:10800']
  return pygridgain_dbapi.connect(
      address=addr,
      timeout=10,
      identity='user',
      secret='password'
  )

def basic_operations_example():
  """Demonstrate basic CRUD operations."""
  with create_simple_connection() as conn:
    with conn.cursor() as cursor:
      try:
        # Drop table if exists (for clean runs)
        cursor.execute('DROP TABLE IF EXISTS Person')

        # Create table
        cursor.execute('''
                  CREATE TABLE Person(
                      id INT PRIMARY KEY, 
                      name VARCHAR, 
                      age INT
                  )
              ''')

        # Sample data
        sample_data = [
          [1, "John", 30],
          [2, "Jane", 32],
          [3, "Bob", 28]
        ]

        # Insert data (fixed table name)
        cursor.executemany('INSERT INTO Person VALUES(?, ?, ?)', sample_data)

        # Query data
        cursor.execute('SELECT * FROM Person ORDER BY id')
        results = cursor.fetchall()

        print("All persons in database:")
        for row in results:
          print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")

        # Query specific record
        cursor.execute('SELECT name, age FROM Person WHERE id = ?', [2])
        person = cursor.fetchone()
        if person:
          print(f"\nFound person: {person[0]}, age {person[1]}")
      except Exception as e:
        print(f"Database error: {e}")


def transaction_example():
  """Demonstrate transaction handling with proper rollback scenarios."""
  conn = create_simple_connection()

  try:
    # Disable autocommit for manual transaction control
    conn.autocommit = False

    with conn.cursor() as cursor:
      try:
        # Insert valid records
        cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [4, "Alice", 29])
        cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [5, "Charlie", 31])

        # Simulate a business logic check that fails
        # (e.g., age validation, duplicate check, etc.)
        new_age = 150  # Invalid age
        if new_age > 120:
          raise ValueError("Age validation failed: unrealistic age")

        cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [6, "Invalid", new_age])

        # If we get here, commit all changes
        conn.commit()
        print("Transaction committed successfully")

      except Exception as e:
        # Rollback on any error
        conn.rollback()
        print(f"Transaction rolled back due to error: {e}")

      # Verify what's actually in the database
      cursor.execute('SELECT COUNT(*) FROM Person')
      count = cursor.fetchone()[0]
      print(f"Total persons in database: {count}")

  except Exception as e:
    print(f"Connection error: {e}")
  finally:
    conn.close()

def connection_examples():
  """Show different connection types."""
  print("=== Connection Examples ===")

  # Basic connection
  print("1. Basic connection:")
  with create_simple_connection() as basic_conn:
    print(f"   Connected: {basic_conn is not None}")

  # Note: SSL and auth connections would be created similarly
  # but require actual certificates/credentials to test
  print("2. SSL and authenticated connections available via helper functions")

def main():
  """Main function demonstrating GridGain DB API usage."""
  print("GridGain DB API Example")
  print("=" * 30)

  try:
    connection_examples()
    print("\n=== Basic Operations ===")
    basic_operations_example()
    print("\n=== Transaction Handling ===")
    transaction_example()

  except Exception as e:
    print(f"Application error: {e}")

if __name__ == "__main__":
  main()