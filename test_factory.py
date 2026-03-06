
from services.book_service_factory import BookServiceFactory, NativeService, OpenLibraryService
import mariadb
from database import DB_CONFIG

def test_factory():
    print("Testing BookServiceFactory...")
    
    # Check what service is configured for Tipo 1 (Libros)
    # We routed enterprise_id = 1 in our setup
    svc = BookServiceFactory.get_service_for_type(1, 1)
    
    print(f"Service for Type 1: {type(svc).__name__}")
    
    if isinstance(svc, OpenLibraryService):
        print("PASS: Libros is correctly linked to OpenLibraryService.")
        
        # Test fetching a real book
        isbn = "9780140328721" # Roald Dahl
        print(f"Fetching info for {isbn}...")
        found, data = svc.get_info(isbn)
        if found:
            print(f"PASS: Found book: {data.get('titulo')}")
        else:
            print("FAIL: Book not found (network or api issue?)")
            
    else:
        print("FAIL: Libros should be linked to OpenLibraryService but got Native.")

    # Test unknown type
    svc_native = BookServiceFactory.get_service_for_type(999, 1)
    print(f"Service for Type 999: {type(svc_native).__name__}")
    if isinstance(svc_native, NativeService):
        print("PASS: Unknown type defaults to NativeService.")
    else:
        print("FAIL: Should be NativeService.")

if __name__ == "__main__":
    test_factory()
