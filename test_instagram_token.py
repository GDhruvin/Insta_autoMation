import os
import requests
from dotenv import load_dotenv

load_dotenv()

INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN")

print("Testing Instagram Token...")
print(f"Token (first 30 chars): {INSTAGRAM_ACCESS_TOKEN[:30]}...")

# Test 1: Get Instagram account info
test_url = f"https://graph.instagram.com/v21.0/me?fields=id,username&access_token={INSTAGRAM_ACCESS_TOKEN}"
response = requests.get(test_url)

if response.status_code == 200:
    data = response.json()
    print(f"✅ SUCCESS! Instagram Account:")
    print(f"   ID: {data.get('id')}")
    print(f"   Username: {data.get('username')}")
    print(f"   Account Type: {data.get('account_type', 'Not specified')}")
    
    # Test 2: Check permissions
    debug_url = f"https://graph.instagram.com/v21.0/debug_token?input_token={INSTAGRAM_ACCESS_TOKEN}&access_token={INSTAGRAM_ACCESS_TOKEN}"
    debug_response = requests.get(debug_url)
    
    if debug_response.status_code == 200:
        debug_data = debug_response.json()
        print(f"\n✅ Token Debug Info:")
        print(f"   Is Valid: {debug_data.get('data', {}).get('is_valid', False)}")
        print(f"   Scopes: {debug_data.get('data', {}).get('scopes', [])}")
        print(f"   Expires At: {debug_data.get('data', {}).get('expires_at', 0)}")
else:
    print(f"❌ ERROR: {response.text}")