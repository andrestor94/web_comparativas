
import os

path = r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\templates\dashboard.html"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Define the broken chunks and replacements
replacements = [
    (
        "    const direct = {{ chart_prov|default ([]) | tojson\n  }};",
        "    const direct = {{ chart_prov|default([]) | tojson }};"
    ),
    (
        "    const L = {{ chart_suppliers_adj_labels|default ([]) | tojson\n  }};",
        "    const L = {{ chart_suppliers_adj_labels|default([]) | tojson }};"
    )
]

new_content = content
for old, new in replacements:
    new_content = new_content.replace(old, new)

if new_content != content:
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print("FIX_APPLIED")
else:
    print("NO_MATCH_FOUND")
