
filename = r"c:\Users\andre\OneDrive\Escritorio\web_comparativas_v2\web_comparativas_v2\web_comparativas\main.py"
search_str = '/mercado-publico/seguimiento-usuarios'

with open(filename, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f, 1):
        if search_str in line:
            print(f"Found on line {i}: {line.strip()}")
