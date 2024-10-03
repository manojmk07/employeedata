import requests

# Set your Solr URL
SOLR_URL = "http://localhost:8983/solr"

# Function to create a collection in Solr
def createCollection(p_collection_name):
    url = f"{SOLR_URL}/admin/collections?action=CREATE&name={p_collection_name}&numShards=1&replicationFactor=1"
    response = requests.get(url)
    return response.json()

# Function to index data into the specified collection excluding a specified column
def indexData(p_collection_name, p_exclude_column):
    # Sample employee data
    employees = [
        {'id': 'E02001', 'name': 'Alice', 'gender': 'Female', 'department': 'IT', 'salary': 75000},
        {'id': 'E02002', 'name': 'Bob', 'gender': 'Male', 'department': 'HR', 'salary': 65000},
        {'id': 'E02003', 'name': 'Charlie', 'gender': 'Male', 'department': 'Engineering', 'salary': 80000},
        {'id': 'E02004', 'name': 'David', 'gender': 'Male', 'department': 'IT', 'salary': 72000},
    ]

    # Exclude specified column and prepare data for indexing
    docs_to_index = [
        {k: v for k, v in emp.items() if k != p_exclude_column} for emp in employees
    ]

    # Send data to Solr for indexing
    url = f"{SOLR_URL}/{p_collection_name}/update?commit=true"
    response = requests.post(url, json=docs_to_index)
    return response.json()

# Function to search within the specified collection
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    url = f"{SOLR_URL}/{p_collection_name}/select?q={p_column_name}:{p_column_value}&rows=10"
    response = requests.get(url)
    return response.json()

# Function to get the employee count in the specified collection
def getEmpCount(p_collection_name):
    url = f"{SOLR_URL}/{p_collection_name}/select?q=*:*&rows=0"
    response = requests.get(url)
    try:
        return response.json()['response']['numFound']
    except (ValueError, KeyError) as e:
        print(f"Error retrieving employee count for {p_collection_name}: {e}")
        print("Response text:", response.text)
        return None

# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    url = f"{SOLR_URL}/{p_collection_name}/update?commit=true"
    payload = f"<delete><id>{p_employee_id}</id></delete>"
    headers = {'Content-Type': 'text/xml'}
    response = requests.post(url, data=payload, headers=headers)
    return response.json()

# Function to get department facets
def getDepFacet(p_collection_name):
    url = f"{SOLR_URL}/{p_collection_name}/select?q=*:*&facet=true&facet.field=department&rows=0"
    response = requests.get(url)
    return response.json()

# Execution of the functions
if __name__ == "__main__":
    v_nameCollection = 'Hash_YourName'  # Replace with your actual name
    v_phoneCollection = 'Hash_1234'      # Replace with your last four digits

    print(f"Creating collection: {v_nameCollection}")
    print(createCollection(v_nameCollection))

    print(f"Creating collection: {v_phoneCollection}")
    print(createCollection(v_phoneCollection))

    print(f"Initial Employee count for {v_nameCollection}: {getEmpCount(v_nameCollection)}")

    print(f"Indexing data into {v_nameCollection} excluding 'Department':")
    print(indexData(v_nameCollection, 'Department'))

    print(f"Indexing data into {v_phoneCollection} excluding 'Gender':")
    print(indexData(v_phoneCollection, 'Gender'))

    print(f"Deleting employee 'E02003' from {v_nameCollection}:")
    print(delEmpById(v_nameCollection, 'E02003'))

    print(f"Employee count for {v_nameCollection} after deletion: {getEmpCount(v_nameCollection)}")

    print(f"Searching for employees in 'IT' department in {v_nameCollection}:")
    print(searchByColumn(v_nameCollection, 'department', 'IT'))

    print(f"Searching for 'Male' employees in {v_nameCollection}:")
    print(searchByColumn(v_nameCollection, 'gender', 'Male'))

    print(f"Searching for employees in 'IT' department in {v_phoneCollection}:")
    print(searchByColumn(v_phoneCollection, 'department', 'IT'))

    print(f"Getting department facet for {v_nameCollection}:")
    print(getDepFacet(v_nameCollection))

    print(f"Getting department facet for {v_phoneCollection}:")
    print(getDepFacet(v_phoneCollection))
