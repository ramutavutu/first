import requests

def call_cloud_function():
    # Replace with your Cloud Function URL
    function_url = 'https://us-east1-bkt-nonprod-dev-dwh-svc-00.cloudfunctions.net/clfn-fetch-jira-project-issues-statuses-to-gcs'

    # Make a GET or POST request, depending on your function's configuration
    response = requests.get(function_url)
    
    # Check the response status
    if response.status_code == 200:
        print("Cloud Function called successfully")
        print("Response:", response.text)
    else:
        print("Failed to call Cloud Function")
        print("Status Code:", response.status_code)
        print("Error Message:", response.text)

# Call the function
call_cloud_function()
