from services.library_api_service import get_book_info_from_google_books
import json

isbns = ["9788478970704", "9781491962299", "9780553448146"] # Some more modern/technical ones
for isbn in isbns:
    print(f"\n--- Testing ISBN {isbn} ---")
    success, data = get_book_info_from_google_books(isbn)
    if success:
        print(f"Title: {data.get('titulo')}")
        print(f"Ebook Access: {json.dumps(data.get('ebook_access'), indent=2)}")
    else:
        print("Failed to get info")
