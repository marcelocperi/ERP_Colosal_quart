import zipfile
import xml.etree.ElementTree as ET
import sys
import os

def read_docx(file_path, output_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found")
        return
    text = []
    ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
    with zipfile.ZipFile(file_path) as docx:
        for file in docx.namelist():
            if file == 'word/document.xml':
                xml_content = docx.read(file)
                tree = ET.fromstring(xml_content)
                for p in tree.iter(f"{{{ns['w']}}}p"):
                    p_text = [node.text for node in p.iter(f"{{{ns['w']}}}t") if node.text]
                    if p_text:
                        text.append(''.join(p_text))
                    else:
                        text.append('')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(text))

if __name__ == '__main__':
    read_docx(sys.argv[1], sys.argv[2])
