
import os

path = r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\templates\base.html"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Define the broken chunks and replacements
replacements = [
    (
        "    display: { { (_display_name or 'Yo')| tojson } },",
        "    display: {{ (_display_name or 'Yo')| tojson }},"
    ),
    (
        "    email: { { (user and user.email or '')| tojson } }",
        "    email: {{ (user and user.email or '')| tojson }}"
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
    # Debug print surrounding lines
    idx = content.find("const APP_USER")
    if idx != -1:
        print("CONTEXT:", content[idx:idx+200])
