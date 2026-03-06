import pandas as pd
import sys

# Force UTF-8 output
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

file_path = 'libros_enriquecidos_Empresa_1.xlsx'
df = pd.read_excel(file_path)

print(f"Total rows: {len(df)}")
if 'Ebook' in df.columns:
    print("Ebook distribution:")
    print(df['Ebook'].value_counts())
else:
    print("Columns:", df.columns.tolist())
