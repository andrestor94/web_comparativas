from pathlib import Path

file_path = Path(r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\templates\base.html")

content = file_path.read_text(encoding="utf-8")

# Pattern to search for (simplified to avoid whitespace issues)
# We look for the start of the object and the end
start_marker = "const APP_USER = {"
end_marker = "window.APP_USER = APP_USER;"

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

if start_idx != -1 and end_idx != -1:
    print(f"Found block from {start_idx} to {end_idx}")
    
    # Construct the clean block
    clean_block = """const APP_USER = {
        role: {{ ((user and (user.role or user.rol or ''))|lower)|tojson }},
        display: {{ (_display_name or 'Yo')|tojson }},
        email: {{ (user and user.email or '')|tojson }}
      };
    """
    
    # We want to replace everything between start_marker and before window.APP_USER
    # matches: const APP_USER = { ... };
    # The end_marker starts at window...
    # We need to include the }; before it.
    
    # Look for the last curly brace before end_idx?
    # Actually, let's just replace the specific broken lines.
    
    # The broken lines contain "{ {"
    if "{ {" in content:
        print("Found broken brackets with spaces")
        new_content = content.replace("{ {", "{{")
        new_content = new_content.replace("} }", "}}")
        
        # Also clean up the 'role' line which might be split
        # This is harder to regex replace safely without context.
        # Let's try to locate the whole block and replace it.
        
        pre_block = content[:start_idx]
        post_block = content[end_idx:]
        
        final_content = pre_block + clean_block + "    " + post_block
        # Note: indentation might be slightly off but better than syntax error.
        
        file_path.write_text(final_content, encoding="utf-8")
        print("File updated.")
    else:
        print("Did not find '{ {' spaces. Checking for duplicate declaration?")
        if content.count("const APP_USER = {") > 1:
            print("Found duplicate APP_USER declaration. Fixing...")
            # If duplicated, it looks like:
            # const APP_USER = {
            #   const APP_USER = {
            # ...
            # We can just remove one.
            new_content = content.replace("const APP_USER = {\n        const APP_USER = {", "const APP_USER = {")
            # Handle variable indentation/newlines
            import re
            new_content = re.sub(r"const APP_USER = \{\s+const APP_USER = \{", "const APP_USER = {", content)
            file_path.write_text(new_content, encoding="utf-8")
            print("Fixed duplicate.")
else:
    print("Could not locate APP_USER block.")
