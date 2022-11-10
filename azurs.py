import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

#RDLAYbSpKBlgi6fLj1IGh1bGdV1M1g4vZFI04CoG2IvkLInMXtx964PYyRth99JH7l8NDb5XIzfO+AStb9Zrxw==
cstr = '''DefaultEndpointsProtocol=https;AccountName=azure1earning;AccountKey=RDLAYbSpKBlgi6fLj1IGh1bGdV1M1g4vZFI04CoG2IvkLInMXtx964PYyRth99JH7l8NDb5XIzfO+AStb9Zrxw==;EndpointSuffix=core.windows.net'''


# Create the BlobServiceClient object which will be used to create a container client
#blob_service_client = BlobServiceClient.from_connection_string(cstr)
from azure.identity import DefaultAzureCredential
default_credential = DefaultAzureCredential()

# Instantiate a BlobServiceClient using a token credential
from azure.storage.blob import BlobServiceClient
blob_service_client = BlobServiceClient(
    account_url='https://azure1earning.blob.core.windows.net',
    credential=default_credential
)


# Get account information for the Blob Service
account_info = blob_service_client.get_service_properties()
print(account_info)
# Create a unique name for the container
container_name = str(uuid.uuid4())

# Create the container
#container_client = blob_service_client.create_container(container_name)

print("\nListing blobs...")

 # Get a client to interact with a specific container - though it may not yet exist
container_client = blob_service_client.get_container_client("learning")


with open(r'C:\Work\DataValidationToolUsingSpark\output\config.calender\20220824172932\dataComparision_FAILED.csv', "rb") as data:
    blob_client = container_client.upload_blob(name="dataComparision_FAILED.csv", data=data)

properties = blob_client.get_blob_properties()

try:
    for blob in container_client.list_blobs():
        print("Found blob: ", blob.name)
except ResourceNotFoundError:
    print("Container not found.")
# [END bsc_get_container_client]



