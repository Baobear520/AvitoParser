import io

from minio import Minio
from minio.error import S3Error


def create_image_key(record, counter=1):
    # Generate a unique key for the image
    user_id = record['user_id']
    object_id = record['id']

    # Construct the object key based on user and object
    image_key = f"user-{user_id}/object-{object_id}/image-{counter}.jpg"
    print(f"Image key: {image_key} has been created")
    return image_key


class MinioClient:
    def __init__(self, endpoint, root_user, password, secure=False):
        self.client = Minio(endpoint,
                            access_key=root_user,
                            secret_key=password,
                            secure=secure)

    def create_bucket(self, bucket_name):
        # Check if the bucket exists, create if it doesn't
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
        except S3Error as err:
            print(f"Error creating bucket: {err}")

    def delete_bucket(self, bucket_name):
        try:
            self.client.remove_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' deleted.")
        except S3Error as err:
            print(f"Error deleting bucket: {err}")

    def upload_image(self, bucket_name, image_key, image_data):
        try:
            file_data = io.BytesIO(image_data)
            # Upload image to the specified bucket
            self.client.put_object(
                bucket_name,
                image_key,
                file_data,
                len(image_data),
                'image/jpeg')
            print(f"Uploaded {image_key} to Minio.")
        except S3Error as err:
            print(f"Error uploading {image_key} to Minio: {err}")
        except Exception as err:
            print(f"{type(err).__name__} occurred uploading {image_key} to Minio: {err}")

    def get_image(self, bucket_name, image_key):
        try:
            image_data = self.client.get_object(bucket_name, image_key)
            return image_data.read()
        except S3Error as err:
            print(f"Error getting {image_key} from Minio: {err}")
        except Exception as err:
            print(f"{type(err).__name__} occurred getting {image_key} from Minio: {err}")
        return None
