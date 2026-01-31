import requests
import json
import pandas as pd

def get_ghaziabad_localities():
    """
    Pulls a list of localities (villages, neighborhoods, etc.)
    from OpenStreetMap within Ghaziabad's bounding box.
    """
    # Define bounding box for Ghaziabad district (approx).
    bbox = "28.53,77.25,28.84,77.74"

    # Overpass query
    query = f"""
    [out:json][timeout:60];
    (
      node["place"~"hamlet|village|town|suburb|neighbourhood"]({bbox});
      way["place"~"hamlet|village|town|suburb|neighbourhood"]({bbox});
      relation["place"~"hamlet|village|town|suburb|neighbourhood"]({bbox});
    );
    out tags center;
    """

    overpass_url = "https://overpass-api.de/api/interpreter"
    response = requests.get(overpass_url, params={'data': query})

    if response.status_code != 200:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON response from the API.")
        return None

    localities = []

    for element in data.get("elements", []):
        tags = element.get("tags", {})
        if "name" in tags and "place" in tags:
            localities.append({
                "name": tags["name"],
                "type": tags["place"]
            })

    df = pd.DataFrame(localities).drop_duplicates().sort_values("name")
    return df

# Run it
localities_df = get_ghaziabad_localities()

if localities_df is not None:
    print("Localities in Ghaziabad:")
    print(localities_df.head(20))  # show first 20
    localities_df.to_csv("ghaziabad_localities.csv", index=False)
