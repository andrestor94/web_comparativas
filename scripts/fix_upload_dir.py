
import os

path = r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\legacy_routes.py"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Replace definition
old_code = '    base_dir = Path("data/uploads")'
new_code = '    upload_dir = Path("data/uploads")'

if old_code in content:
    new_content = content.replace(old_code, new_code)
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print("FIX_APPLIED")
else:
    print("NO_MATCH_FOUND")
    # Debug
    print("Looking for:", repr(old_code))
    # Maybe indentation is tabs? Or spaces?
    # I see 4 spaces in view_file.
