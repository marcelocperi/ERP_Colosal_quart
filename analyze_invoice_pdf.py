import fitz

doc = fitz.open(r'C:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\20171634432_011_00002_00000056.pdf')
page = doc[0]

print(f"Page size: {page.rect.width} x {page.rect.height}")
print("=" * 100)

# Extract ALL text with position, sorted by Y then X
blocks = page.get_text('dict')['blocks']
all_items = []

for b in blocks:
    if 'lines' in b:
        for line in b['lines']:
            for span in line['spans']:
                text = span['text'].strip()
                if text:
                    bbox = span['bbox']
                    all_items.append({
                        'y': bbox[1],
                        'x': bbox[0],
                        'text': text,
                        'size': span['size'],
                        'font': span['font'],
                        'bold': 'Bold' in span['font'] or 'bold' in span['font'].lower()
                    })

# Sort by Y, then X
all_items.sort(key=lambda i: (round(i['y'], 0), i['x']))

prev_y = -1
for item in all_items:
    y_rounded = round(item['y'], 0)
    if y_rounded != prev_y:
        print(f"\n--- Y={item['y']:>6.1f} ---")
        prev_y = y_rounded
    bold_marker = "[B]" if item['bold'] else "   "
    print(f"  X={item['x']:>6.1f} | sz={item['size']:>5.1f} | {bold_marker} | {item['text']}")

print("\n\n" + "=" * 100)
print("DRAWING/LINES (rectangles and paths - for layout structure):")

# Extract rectangles (borders/boxes)
drawings = page.get_drawings()
for d in drawings:
    if d['rect']:
        r = d['rect']
        print(f"  RECT: x0={r.x0:.1f} y0={r.y0:.1f} x1={r.x1:.1f} y1={r.y1:.1f} | w={r.width:.1f} h={r.height:.1f}")
