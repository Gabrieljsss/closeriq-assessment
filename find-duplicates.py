import pandas as pd
from fuzzywuzzy import fuzz
import json
import sys

def sanitize_url(url):
        if (url):
            return  url.replace('https://', '').replace('http://', '').replace('www.', '')
        return False
def sanitize_linkedin_url(linkedin_url):
        if (linkedin_url):
            return  sanitize_url(linkedin_url).replace('linkedin.com/company/',  '')
        return False

'''
Matches two rows.
Overall compatibility is given by the arithmetic mean of each field compatibility.
Fields are only compared when both rows provide them.
'''
def match_company_rows(first_row, second_row, threshold = 80):
    compatibility = []

    if (first_row['name'] and second_row['name']):
        compatibility.append(fuzz.partial_ratio(first_row['name'], second_row['name']))

    if (first_row['short_name'] and second_row['short_name']):
        compatibility.append(fuzz.partial_ratio(first_row['short_name'], second_row['short_name']))

    # Different companies cannot have the exact same website url
    if (first_row['website_url'] and second_row['website_url']):
        if (first_row['website_url'] == second_row['website_url']):
            return True
        compatibility.append(fuzz.partial_ratio(first_row['website_url'], second_row['website_url']))

    # Different companies cannot have the exact same linkedin url
    if (first_row['linkedin_url'] and second_row['linkedin_url']):
        if (first_row['linkedin_url'] == second_row['linkedin_url']):
            return True
        compatibility.append(fuzz.partial_ratio(first_row['linkedin_url'], second_row['linkedin_url']))

    probability = 0
    for property_compatibility in compatibility:
        probability += float(1 / len(compatibility)) * float(property_compatibility)

    return probability > threshold

def group_companies(df, record_match_function):
    duplicates = {}
    duplicated_elements_ids = {} # {'element_id': 'first_possible_entry_id'}

    for index, row in df.iterrows():
        # if know to be duplicated adds to duplicates and skipes iteration
        if row['Id'] in duplicated_elements_ids:
                duplicates[duplicated_elements_ids[row['Id']]].append(row.to_dict())
                continue
        for inner_index, inner_row in df.iloc[index:].iterrows():

            # skips current company
            if (inner_row['Id'] == row['Id']):
                continue

            # When the inner_row is has already a known duplicate, row
            # when be added to the same duplicates partition as they are
            # transitive duplicates.
            if (record_match_function(row, inner_row)):
                if inner_row['Id'] in duplicated_elements_ids:
                    duplicates[duplicated_elements_ids[inner_row['Id']]].append(row.to_dict())
                    duplicated_elements_ids[row['Id']] = duplicated_elements_ids[inner_row['Id']]
                    continue
                # When inner_row does not have a duplicate yet,
                # a new duplicates group is created
                duplicates[row['Id']] = [row.to_dict()]
                duplicated_elements_ids[inner_row['Id']] = row['Id']
    return duplicates

def dump_results(results):
    with open('./data.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

def main():
    dump_results(group_companies(df, match_company_rows))

if __name__=="__main__":
    if len(sys.argv) > 1: filename = sys.argv[1]
    else: filename = 'companies.csv'
    filename = f'./{filename}'
    df = pd.read_csv(
        filename
    ).fillna('')
    main()
