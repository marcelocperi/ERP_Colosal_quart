import database
print("Database file location:", database.__file__)
print("Items in database module:", dir(database))
try:
    from database import atomic_transaction
    print("Success! atomic_transaction is present.")
except ImportError as e:
    print("ImportError:", e)
