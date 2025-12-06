import os, sys
import requests

MAIN_DIR = os.path.dirname((os.path.dirname(__file__)))
if __name__ == "__main__":
    sys.path.append(MAIN_DIR)

from pylcloud.storage import StorageServer


storage_api = StorageServer()

if "server" in sys.argv:
    storage_api.start()

else:

    test_file_path = os.path.join(os.path.dirname(__file__), "test_file.txt")
    with open(test_file_path, "w") as f:
        f.write("Hello Storage Server!")

    key = "test-bucket/test_upload.txt"
    upload_url = "http://localhost:5001/upload"
    download_url = "http://localhost:5001/download"
    list_url = "http://localhost:5001/list_buckets"
    list_files_url = "http://localhost:5001/list_bucket_files"

    # Upload to server
    with open(test_file_path, "rb") as f:
        response = requests.post(upload_url, files={"file": f}, params={"key": key})
        print("Upload response:", response.json())

    # Check list bucket
    list_response = requests.get(list_url)
    print("List buckets response:", list_response.json())

    # Check list bucket files
    list_response = requests.get(list_url, params={"bucket_name": "test-bucket"})
    print("List bucket files response:", list_response.json())

    # Download file
    response = requests.get(download_url, params={"key": key})
    if response.status_code == 200:
        with open("downloaded_test.txt", "wb") as out:
            out.write(response.content)
        print("Downloaded file saved as 'downloaded_test.txt'")
    else:
        print("Download failed:", response.json())
