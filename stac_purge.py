import requests

"""
Purges Sentinel STAC items (collections are protected from deletion otherwise)
Don't forget to set up .netrc file !!!
"""

# Constants
BASE_URL = "https://resto-test.cloud.cesnet.cz"
COLLECTIONS = ["SENTINEL-1", "SENTINEL-2", "SENTINEL-3", "SENTINEL-5P"]
ITEMS_PER_PAGE = 100
TOKEN = None


def get_token():
    """Get authentication token using credentials from .netrc file."""
    global TOKEN

    response = requests.get(f"{BASE_URL}/auth")
    response.raise_for_status()
    TOKEN = response.json().get('token')


def fetch_collection_items(collection_id):
    """Fetch all items from a collection using pagination."""
    items = []
    page = 1
    headers = {'Authorization': f'Bearer {TOKEN}'}
    while True:
        response = requests.get(
            f"{BASE_URL}/collections/{collection_id}/items",
            params={"limit": ITEMS_PER_PAGE, "page": page},
            headers=headers
        )

        if not response.ok:
            print(f"Error fetching items for collection {collection_id}: {response.status_code}")
            break

        data = response.json()
        if not data.get('features'):
            break

        items.extend(data['features'])
        page += 1
    return items


def delete_item(collection_id, item_id):
    """Delete a specific item from a collection."""
    headers = {'Authorization': f'Bearer {TOKEN}'}
    response = requests.delete(
        f"{BASE_URL}/collections/{collection_id}/items/{item_id}",
        headers=headers
    )
    return response.ok


def main():
    get_token()
    for collection in COLLECTIONS:
        print(f"Processing collection: {collection}")
        items = fetch_collection_items(collection)
        print(f"Found {len(items)} items in {collection}")

        for item in items:
            item_id = item['id']
            if delete_item(collection, item_id):
                print(f"Successfully deleted item {item_id}")
            else:
                print(f"Failed to delete item {item_id}")


if __name__ == "__main__":
    main()
