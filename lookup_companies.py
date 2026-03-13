
import json, os, sys, requests, time

API_KEY_FILE = os.path.join(os.path.dirname(__file__), '.ch_api_key')

def load_api_key():
    with open(API_KEY_FILE) as f:
        for line in f:
            if 'COMPANIES_HOUSE_API_KEY=' in line:
                return line.strip().split('=',1)[1]
    return None

def search_company(name, api_key):
    url = 'https://api.company-information.service.gov.uk/search/companies'
    r = requests.get(url, params={'q': name, 'items_per_page': 5}, auth=(api_key, ''))
    if r.status_code \!= 200:
        print(f'  API error {r.status_code} for {name}')
        return None
    items = r.json().get('items', [])
    # Find exact match (case-insensitive)
    for item in items:
        if item.get('title','').upper().strip() == name.upper().strip():
            return item
    # Fall back to first result
    return items[0] if items else None

def main():
    names_str = os.environ.get('COMPANY_NAMES', '')
    if not names_str:
        print('No COMPANY_NAMES env var set')
        sys.exit(1)

    names = [n.strip() for n in names_str.split('|') if n.strip()]
    api_key = load_api_key()
    if not api_key:
        print('No API key found')
        sys.exit(1)

    companies = []
    for name in names:
        print(f'Looking up: {name}')
        result = search_company(name, api_key)
        time.sleep(0.5)
        if result:
            num = result.get('company_number','')
            print(f'  Found: {result.get("title")} ({num})')
            companies.append({
                'company_number': num,
                'company_name': result.get('title',''),
                'company_status': result.get('company_status','active'),
                'date_of_creation': result.get('date_of_creation',''),
                'registered_office_address': result.get('address',{}),
                'sic_codes': [],
                'source': 'manual_lookup',
            })
        else:
            print(f'  Not found')

    os.makedirs('output', exist_ok=True)
    with open('output/filtered_companies.json', 'w') as f:
        json.dump(companies, f, indent=2)
    print(f"
Saved {len(companies)} companies to output/filtered_companies.json")

if __name__ == "__main__":
    main()
