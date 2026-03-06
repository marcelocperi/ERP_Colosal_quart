from services.library_api_service import get_book_info_from_google_books
import json

isbn = "9788478970704"
success, data = get_book_info_from_google_books(isbn)

if success:
    print(f"Title: {data.get('titulo')}")
    print(f"Ebook Access: {json.dumps(data.get('ebook_access'), indent=2)}")
else:
    print("Failed to get info")
