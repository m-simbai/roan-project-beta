import requests
import json

# Test the search API directly
response = requests.get('http://127.0.0.1:5000/api/search?q=sapi')
print(f"Status Code: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")

# Test with different search terms
test_terms = ['Sapi', 'Safari', 'mid_zambezi', 'NAME', 'LANDTYPE']

for term in test_terms:
    response = requests.get(f'http://127.0.0.1:5000/api/search?q={term}')
    result = response.json()
    print(f"\nSearch for '{term}': {result['total_results']} results")
    if result['total_results'] > 0:
        for r in result['results']:
            print(f"  - Table: {r['table_name']}")
            if r['table_match']:
                print(f"    Table name match: True")
            if r['column_matches']:
                print(f"    Column matches: {r['column_matches']}")
            if r['total_data_matches'] > 0:
                print(f"    Data matches: {r['total_data_matches']}")
