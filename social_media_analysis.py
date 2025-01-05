import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI
import json
from astrapy import DataAPIClient

# Replace "YOUR_TOKEN" with your Astra DB Token
client = DataAPIClient("AstraCS:JNjCteHZxbeZjFnugZOzzjsa:8c970ac9c7128742aa604fa8b8c3e89d540e55de8000553c4e9a8b57208d9c55")
db = client.get_database_by_api_endpoint("https://440bff7a-4128-4874-8263-bb5ac606ab7c-us-east-2.apps.astra.datastax.com")

print(f"Connected to Astra DB: {db.list_collection_names()}")

# --- Insert Data into Astra DB ---
def insert_mock_data():
    collection = db.get_collection("social_engagement")  # Replace with your collection name
    mock_data = [
        {"post_id": "1", "post_type": "carousel", "likes": 120, "shares": 30, "comments": 20},
        {"post_id": "2", "post_type": "reels", "likes": 200, "shares": 50, "comments": 100},
        {"post_id": "3", "post_type": "static", "likes": 80, "shares": 10, "comments": 5},
    ]
    for post in mock_data:
        collection.create(post)
    print("Mock data inserted.")

# --- Fetch Data from Astra DB ---
def fetch_engagement_data(post_type):
    collection = db.get_collection("social_engagement")
    data = collection.find({"post_type": post_type})
    return list(data)
# Configure Astra DB connection
auth_provider = PlainTextAuthProvider(ASTRA_DB_CLIENT_ID, ASTRA_DB_CLIENT_SECRET)
cluster = Cluster(cloud={"secure_connect_bundle": ASTRA_DB_SECURE_CONNECT}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(ASTRA_DB_KEYSPACE)

# --- Create table ---
session.execute("""
CREATE TABLE IF NOT EXISTS social_engagement (
    post_id UUID PRIMARY KEY,
    post_type TEXT,
    likes INT,
    shares INT,
    comments INT
);
""")

# --- Insert mock data ---
mock_data = [
    {"post_id": "1", "post_type": "carousel", "likes": 120, "shares": 30, "comments": 20},
    {"post_id": "2", "post_type": "reels", "likes": 200, "shares": 50, "comments": 100},
    {"post_id": "3", "post_type": "static", "likes": 80, "shares": 10, "comments": 5},
]

for post in mock_data:
    session.execute("""
    INSERT INTO social_engagement (post_id, post_type, likes, shares, comments)
    VALUES (%s, %s, %s, %s, %s)
    """, (post["post_id"], post["post_type"], post["likes"], post["shares"], post["comments"]))

# --- Query data ---
def fetch_engagement_data(post_type):
    query = "SELECT * FROM social_engagement WHERE post_type = %s ALLOW FILTERING"
    rows = session.execute(query, (post_type,))
    return [{"likes": row.likes, "shares": row.shares, "comments": row.comments} for row in rows]

# --- Analyze data ---
def calculate_average_metrics(data):
    if not data:
        return {"likes": 0, "shares": 0, "comments": 0}
    likes = sum(d["likes"] for d in data) / len(data)
    shares = sum(d["shares"] for d in data) / len(data)
    comments = sum(d["comments"] for d in data) / len(data)
    return {"likes": likes, "shares": shares, "comments": comments}

# --- Generate insights ---
def generate_insights(post_type, metrics):
    llm = OpenAI(api_key="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")  # Add your OpenAI API key
    prompt = PromptTemplate(
        input_variables=["post_type", "likes", "shares", "comments"],
        template="""
        Analyze the performance of {post_type} posts. The average engagement metrics are:
        Likes: {likes}, Shares: {shares}, Comments: {comments}.
        Provide simple insights and suggestions for improving engagement.
        """
    )
    chain = LLMChain(llm=llm, prompt=prompt)
    return chain.run(post_type=post_type, likes=metrics["likes"], shares=metrics["shares"], comments=metrics["comments"])

# --- Main Function ---
if __name__ == "__main__":
    print("Fetching engagement data...")
    post_type = input("Enter post type (carousel/reels/static): ").strip().lower()
    data = fetch_engagement_data(post_type)

    if not data:
        print(f"No data found for post type: {post_type}")
    else:
        print(f"Data fetched for {post_type}: {data}")
        metrics = calculate_average_metrics(data)
        print(f"Average Metrics: {metrics}")
        insights = generate_insights(post_type, metrics)
        print(f"Generated Insights: {insights}")