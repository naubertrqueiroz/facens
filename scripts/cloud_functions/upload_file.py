import functions_framework
import requests
import json
from google.cloud import storage

@functions_framework.http
def upload_file(request):
    data = json.loads(request.data.decode("utf-8"))

    try:
        url = data['url']
        file_name = data['file_name']
    except Exception as error:
        return{
            "status_code": 400,
            "message": "Url or file_name is empty"
        }


    try:
        with requests.get(url, stream=True) as response:
            client = storage.Client()
            bucket = client.get_bucket('sinasc_bronze')
            blob = bucket.blob('source/'+ file_name)
            content_type = response.headers.get('Content-Type')

            if not blob.exists():
                blob.metadata = {'Content-Type': content_type}

                with blob.open(mode='wb',content_type=content_type) as f:
                    for batch in response.iter_content(1024 * 1024 * 24):
                        f.write(batch)

                blob.patch()

                return {"message": "Upload file with success"}
            else:
                return {"message": 'File already exists in google storage'}
    except Exception as error:
      return {"message":"Failed upload file, error: " + str(error)}
        