import requests

def test_upload_pdf():
    # Replace with your PDF file path
    pdf_path = "/Users/apple/Downloads/a-practical-guide-to-building-agents.pdf"
    
    # Prepare the files for upload
    files = {
        'files': ('test.pdf', open(pdf_path, 'rb'), 'application/pdf')
    }
    
    try:
        # Make the POST request
        response = requests.post(
            'http://localhost:8000/ingest/',
            files=files
        )
        
        # Print the response details
        print(f"Status Code: {response.status_code}")
        print("Response Headers:", response.headers)
        print("Raw Response Text:", response.text)
        
        # Try to parse JSON if possible
        try:
            print("JSON Response:", response.json())
        except requests.exceptions.JSONDecodeError:
            print("Response is not JSON format")
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
    finally:
        # Make sure to close the file
        files['files'][1].close()

if __name__ == "__main__":
    test_upload_pdf() 