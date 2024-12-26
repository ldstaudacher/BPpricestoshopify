import requests
import pandas as pd
import os
import time
import certifi
import psycopg2

def lambda_handler(event, context):
    #product_data_good = pd.read_csv('/Users/staudacherld/Downloads/compareatgood.csv')

####get bp ids for all products
    #product_data = get_all_product_ids()
    #product_data.to_csv('/Users/staudacherld/Downloads/bpcatalogall.csv', index=False)
    product_data = pd.read_csv('/Users/staudacherld/Downloads/bpcatalogall.csv')
    product_data = product_data[~product_data['name'].str.contains('SHIRT', case=False, na=False)]
####get shopify ids for all products
    num_products = all  # Set to 'all' or a fixed number
    shopify_df = fetch_products(limit=num_products)
    shopify_df['sku'] = shopify_df['sku'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    joined_df = pd.merge(shopify_df, product_data, on="sku", how="left")
####get wholsale (retail) pricing for all products
    joined_df = joined_df.sort_values(by="product_id")
    joined_df = joined_df.drop_duplicates()
    product_ids = list(set(joined_df['product_id'].dropna().astype(int).tolist()))
    joined_df = fetch_brightpearl_pricing(product_ids, price_list_id=2, joined_df=joined_df)
    joined_df = joined_df.drop_duplicates()
    product_data_good = joined_df[joined_df['price'].notnull()]
    product_data_bad = joined_df[joined_df['price'].isnull()]
    product_data_good.to_csv('/Users/staudacherld/Downloads/compareatgood.csv', index=False)
    product_data_bad.to_csv('/Users/staudacherld/Downloads/compareatbad.csv', index=False)

    update_compare_at_prices_batch(product_data_good)
    print("something")

def fetch_products(limit="all"):
    """
    Fetch product IDs, variant IDs, titles, and SKUs from Shopify's GraphQL API and return as a pandas DataFrame.

    :param shopify_domain: Your Shopify store domain (e.g., "yourstore.myshopify.com").
    :param access_token: Your Shopify private app access token.
    :param limit: "all" to fetch all products or an integer for a sample size.
    :return: pandas DataFrame with product IDs, variant IDs, titles, and SKUs.
    """
    shopify_domain = "onehydraulic.myshopify.com"  # Correct format for the domain
    access_token = os.getenv('token')
    url = f"https://{shopify_domain}/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    query = """
    query fetchProducts($first: Int, $after: String) {
      products(first: $first, after: $after) {
        edges {
          node {
            id
            title
            variants(first: 250) {
              edges {
                node {
                  id
                  sku
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    products = []
    variables = {"first": 250, "after": None}
    count = 0

    while True:
        response = requests.post(
            url,
            headers=headers,
            json={"query": query, "variables": variables},
            verify=False
        )
        if response.status_code != 200:
            raise Exception(f"GraphQL query failed: {response.status_code} {response.text}")

        data = response.json()
        product_edges = data["data"]["products"]["edges"]
        for edge in product_edges:
            product_id = edge["node"]["id"]
            product_title = edge["node"]["title"]
            for variant in edge["node"]["variants"]["edges"]:
                variant_id = variant["node"]["id"]
                sku = variant["node"]["sku"]
                products.append({
                    "shop_product_id": product_id,
                    "variant_id": variant_id,
                    "title": product_title,
                    "sku": sku
                })
                count += 1
                if isinstance(limit, int) and count >= limit:
                    return pd.DataFrame(products)

        page_info = data["data"]["products"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break

        variables["after"] = page_info["endCursor"]

    return pd.DataFrame(products)


def get_all_product_ids():
    """Retrieve all product IDs and SKUs from Brightpearl using paginated search."""
    products = []
    brightpearl_url = "https://ws-use.brightpearl.com/public-api/onehydraulics"

    headers = {
        'brightpearl-app-ref': os.environ['ref'],
        'brightpearl-account-token': os.environ['bp_token'],
        "Content-Type": "application/json"
    }
    endpoint = f"{brightpearl_url}/product-service/product-search"

    # Start from the first page
    page = 1
    page_size = 100  # Max page size allowed by Brightpearl API

    while True:
        params = {
            "pageSize": page_size,
            "firstResult": ((page - 1) * page_size) + 1,
            #"stockTracked": True,  # Filter for stockTracked = True
            "productStatus": "LIVE"  # Filter for productStatus = 'LIVE'
        }

        response = requests.get(endpoint, headers=headers, params=params, verify=False)
        response.raise_for_status()  # Raise an error for bad HTTP status codes

        data = response.json()
        results = data.get("response", {}).get("results", [])

        # Collect product IDs and SKUs (assuming product ID is at index 0 and SKU at index 2)
        for result in results:
            product_id = result[0]  # Assuming product ID is the first element
            name = result[1]
            sku = result[2]  # Assuming SKU is the third element
            products.append({'product_id': product_id, 'sku': sku, 'name': name})

        # Check if there are more results
        if len(results) < page_size:
            break  # No more pages to fetch
        page += 1

    products = pd.DataFrame(products)
    return products


def fetch_brightpearl_pricing(product_ids, price_list_id, joined_df):
    """
    Fetch pricing for a list of product IDs from Brightpearl REST API in batches of 150 for a specific price list ID,
    and return the joined DataFrame with the price column added.

    :param product_ids: List of product IDs to fetch pricing for.
    :param price_list_id: Price list ID to fetch pricing from.
    :param joined_df: DataFrame to join the pricing data with.
    :return: pandas DataFrame with product_id, existing data, and price column added.
    """
    brightpearl_url = "https://ws-use.brightpearl.com/public-api/onehydraulics"

    headers = {
        'brightpearl-app-ref': os.environ['ref'],
        'brightpearl-account-token': os.environ['bp_token'],
    }

    pricing_data = []
    product_ids = sorted(product_ids)
    # Batch product IDs in groups of 150
    batch_size = 150
    for i in range(0, len(product_ids), batch_size):
        #time.sleep(.3)
        batch = product_ids[i:i + batch_size]
        batch_str = ','.join(map(str, batch))  # Convert each element to a string and join with commas

        # Make the API call for the batch
        endpoint = f"{brightpearl_url}/product-service/product-price/{batch_str}/price-list/{price_list_id}"
        response = requests.get(endpoint, headers=headers, verify=False)
        if response.status_code == 200:
            batch_prices = response.json().get("response", [])
            for product in batch_prices:
                product_id = product["productId"]
                price_list = next(
                    (pl for pl in product.get("priceLists", []) if pl["priceListId"] == price_list_id),
                    None
                )
                if price_list:
                    price = price_list["quantityPrice"].get("1")  # Assuming quantity 1 is the desired price
                    pricing_data.append({
                        "product_id": product_id,
                        "price": price
                    })
                else:
                    pricing_data.append({
                        "product_id": product_id,
                        "price": None
                    })
        else:
            # Log or handle batch errors
            print(response.status_code)
            print(response.text)
            for product_id in batch:
                pricing_data.append({"product_id": product_id, "price": None})

    # Convert pricing data to DataFrame
    pricing_df = pd.DataFrame(pricing_data)

    # Join the pricing DataFrame with the existing joined_df
    joined_df["product_id"] = joined_df["product_id"].astype("Int64")
    pricing_df["product_id"] = pricing_df["product_id"].astype("Int64")
    joined_df = pd.merge(joined_df, pricing_df, on="product_id", how="left")
    return joined_df


def update_compare_at_prices_batch(df, batch_size=10):
    """
    Update the compareAtPrice for products in Shopify using batched GraphQL mutations.

    :param df: DataFrame containing columns: id (Shopify variant ID), price (compare at price to set).
    :param batch_size: Number of updates to include in each batch.
    """
    shopify_domain = "onehydraulic.myshopify.com"
    access_token = os.getenv('token')  # Shopify API token
    url = f"https://{shopify_domain}/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    # Iterate over the DataFrame in batches
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]

        # Construct the batched GraphQL query
        mutations = []
        for _, row in batch.iterrows():
            variant_id = row["variant_id"]  # Shopify variant ID
            compare_at_price = row["price"]  # Brightpearl price to set as compareAtPrice

            mutations.append(f"""
                mutation_{variant_id.replace("gid://shopify/ProductVariant/", "")}: productVariantUpdate(input: {{
                    id: "{variant_id}",
                    compareAtPrice: "{compare_at_price}"
                }}) {{
                    productVariant {{
                        id
                        compareAtPrice
                    }}
                    userErrors {{
                        field
                        message
                    }}
                }}
            """)

        # Combine all mutations into a single query
        query = "mutation {" + " ".join(mutations) + "}"

        # Send the batched request
        response = requests.post(
            url,
            headers=headers,
            json={"query": query},
            verify=False  # Added verify=False
        )

        # Handle the response
        if response.status_code == 200:
            result = response.json()
            for key, value in result.get("data", {}).items():
                if value.get("userErrors"):
                    print(f"Errors for mutation {key}: {value['userErrors']}")
        else:
            print(f"Error: {response.status_code} - {response.text}")




lambda_handler('event', 'context')
