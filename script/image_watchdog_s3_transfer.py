import os
import time
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# AWS credentials and S3 bucket details
TODO: Allow a different way to supply the secrets below
aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = 'nbtr-production'

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Watchdog event handler
class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        file_path = event.src_path
        upload_to_s3(file_path)

# Function to upload file to S3
def upload_to_s3(file_path):
    file_name = os.path.basename(file_path)
    s3.upload_file(file_path, bucket_name, file_name)
    print(f'{file_name} uploaded to S3')

if __name__ == "__main__":
    folder_to_monitor = f"{os.environ['HOME']}/staging"
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_monitor, recursive=False)
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
