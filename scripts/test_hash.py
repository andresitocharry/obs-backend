import bcrypt

password = "admin123"
# The hash from supabase_setup.sql
stored_hash = "$2b$12$HEhG9w.SgxM0gE.KRE0Cxeo5V/rM4zP4fT.7R8mQY6W5mI4nI5y2G"

try:
    # We must ensure we are using bytes
    result = bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
    print(f"Match: {result}")
except Exception as e:
    print(f"Error: {e}")

# Generate a new known hash just in case
new_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
print(f"New recommended hash for 'admin123': {new_hash}")
