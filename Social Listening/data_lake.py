from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import datetime
import json
import os


class DataLake:
    """Azure Data Lake Connection Class

    This class allows the user to manipulate blobs in Azure Data Lake.
    Once instantiated, the user will be able list, download, delete,
    and otherwise manipulate blobs in Azure Data Lake.

    Attributes
    ----------
    connect_str : str
        Azure storage connection string
    container : str
        Azure storage container (default: 'researchanalyticsinsights')
    storage_client : BlobServiceClient
        Azure BlobServiceClient object to interact with storage client
    container_client : ContainerClient
        Azure ContainerClient object to interact with storage container

    Methods
    -------
    list_storage_containers() -> list
        Returns list of all containers (string) in the storage account.
    list_all_blobs() -> list
        Returns list of all blobs (string) in the container.
    find_blobs(val: str) -> list
        Returns list of all blobs that contain the substring (val).
    upload_file(lake_path: str, file_path: str) -> boolean
        Uploads file to data lake and returns True if upload was successful.
    upload_folder(lake_path: str, folder_path: str) -> boolean
        Uploads folder of files to data lake and returns True if upload was successful.
    delete_file(lake_path: str) -> boolean
        Deletes file in data lake and returns True if delete was successful.
    delete_folder(lake_path: str, keep_folder=False) -> boolean
        Deletes folder of files in data lake and returns True if delete was successful.
    get_json_lines_as_df(blob: str) -> DataFrame
        Return a pandas dataframe from a json lines file.
    """

    def __init__(self, connect_str, container="researchanalyticsinsights"):
        """
        Parameters
        ----------
        connect_str : str
            Azure storage connection string
        container : str
            Azure storage container (default: 'researchanalyticsinsights')
        """
        self.connect_str = connect_str
        self.container = container
        self.storage_client = BlobServiceClient.from_connection_string(self.connect_str)
        self.container_client = self.storage_client.get_container_client(self.container)

    def list_storage_containers(self):
        """List the name of containers in the storage account.

        Returns
        -------
        list
            List of all containers (string) in the storage account.
        """

        return [x.name for x in self.storage_client.list_containers()]

    def list_all_blobs(self):
        """List all blobs in a storage container.

        Returns
        -------
        list
            List of all blobs (string) in the container.
        """

        return [x.name for x in self.container_client.list_blobs()]

    def find_blobs(self, val: str):
        """Find blobs that contain a substring.

        Parameters
        ----------
        val : str
            The substring value to look for.

        Returns
        -------
        list
            List of all blobs that contain the substring.
        """

        return [
            x.name for x in self.container_client.list_blobs() if x.name.find(val) != -1
        ]

    def upload_file(self, lake_path: str, file_path: str):
        """Uploads a specific blob at lake_path.

        Parameters
        ----------
        lake_path : str
            The data lake path/to/destination/file.txt.
        file_path : str
            The local path/to/file.txt that should be uploaded to the data lake.

        Returns
        -------
        boolean
            Returns True if file was uploaded, False otherwise.
        """

        try:
            with open(file_path, "rb") as f:
                blob = self.container_client.get_blob_client(blob=lake_path)
                blob.upload_blob(f)
            return True
        except Exception as e:
            print("There was an error uploading this blob.")
            print(f"Blob path: {lake_path}")
            print(f"File path: {file_path}")
            print(f"Error: {e}")
            return False

    def upload_folder(self, lake_path: str, folder_path: str):
        """Uploads a folder of files to the data lake.

        Parameters
        ----------
        lake_path : str
            The data lake path/to/destination/folder
        folder_path : str
            The local path/to/folder that should be uploaded to the data lake.

        Returns
        -------
        boolean
            Returns True if folder of files was uploaded, False otherwise.
        """

        try:
            if lake_path.endswith("/"):
                lake_path = lake_path[:-1]

            for i in os.listdir(folder_path):
                f = os.path.join(folder_path, i)
                blob_path = f"{lake_path}/{i}"
                self.upload_file(blob_path, f)
            return True
        except Exception as e:
            print("There was sn error uploading this folder.")
            print(f"Lake path: {lake_path}")
            print(f"Folder path: {folder_path}")
            print(f"Error: {e}")
            return False

    def delete_file(self, lake_path: str):
        """Deletes a specific blob at lake_path.

        Parameters
        ----------
        lake_path : str
            The blob/path to delete.

        Returns
        -------
        boolean
            Returns True if blob was deleted, False otherwise.
        """

        try:
            blob = self.container_client.get_blob_client(blob=lake_path)
            blob.delete_blob()
            return True
        except Exception as e:
            print("There was an error deleting this blob.")
            print(f"Blob: {lake_path}")
            print(f"Error: {e}")
            return False

    def delete_folder(self, lake_path: str, keep_folder=False):
        """Deletes a folder of blobs.

        Parameters
        ----------
        lake_path : str
            The blob path to delete.
        keep_folder : bool
            Keep the folder at the top of the files to delete? (default: False)

        Returns
        -------
        boolean
            Returns True if blob files were deleted, False otherwise.
        """

        try:
            if lake_path.endswith("/"):
                path = lake_path
                folder_blob = lake_path[:-1]
            else:
                path = lake_path + "/"
                folder_blob = lake_path
            blobs = [
                x.name for x in self.container_client.list_blobs(name_starts_with=path)
            ]
            folders = [x for x in blobs if len(x.split(".")) == 1][::-1]
            folders.remove(folder_blob)
            files = [x for x in blobs if x not in folders]
            for blob in files:
                self.delete_file(blob)
            for blob in folders:
                self.delete_file(blob)
            if keep_folder == False:
                self.delete_file(folder_blob)
            return True
        except Exception as e:
            print("There was an error deleting this folder.")
            print(f"Blob: {lake_path}")
            print(f"Error: {e}")
            return False

    def get_json_lines_as_df(self, blob):
        """Return a pandas dataframe from a json lines file.

        Parameters
        ----------
        blob : str
            The path/to/the/blob.json.

        Returns
        -------
        DataFrame
            Returns pandas dataframe.

        Raises
        ------
        Exception
            If there was any error reading the blob, exception details will be
            printed and None will be returned.
        """

        try:
            blob_client_instance = self.storage_client.get_blob_client(
                "researchanalyticsinsights", blob
            )
            streamdownloader = blob_client_instance.download_blob()
            str_contents = streamdownloader.readall()
            split_contents = str_contents.decode("utf-8-sig").split("\r\n")
            contents = [
                json.loads(content) for content in split_contents if content != ""
            ]
            return pd.DataFrame(contents)
        except Exception as e:
            print(f"There was an error getting {blob}")
            print(f"Exception: {e}")
            return None
