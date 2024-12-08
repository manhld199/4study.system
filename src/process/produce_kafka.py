from datetime import datetime, timedelta, timezone
import json
import time
from kafka import KafkaProducer
from pymongo import MongoClient
from kafka.admin import KafkaAdminClient, NewTopic
from bson import ObjectId

def get_enrolled_courses(days=None):
  try:
    # Replace with your MongoDB Atlas connection string
    connection_string = "mongodb+srv://admin:admin@is353.nvhsc.mongodb.net/4study?retryWrites=true&w=majority&appName=is353"
    client = MongoClient(connection_string)

    # Access the database and collection
    db = client["4study"]
    user_collection = db["users"]

    # Calculate the start date if `days` is provided
    now = datetime.now(timezone.utc)  # Current UTC datetime with timezone info
    start_date = now - timedelta(days=days) if days else None

    # Aggregation pipeline
    pipeline = [
      {"$unwind": "$list_courses"},
      {
        "$match": {
          "list_courses.enroll_time": {
            "$lte": now,  # Use timezone-aware datetime
            **({"$gte": start_date} if start_date else {})
          }
        }
      },
      {"$sort": {"list_courses.enroll_time": -1}},
      {
        "$group": {
          "_id": "$_id",
          "list_courses": {
            "$push": {
              "course_id": "$list_courses.course_id",
              "enroll_time": "$list_courses.enroll_time"
            }
          }
        }
      },
      {
        "$project": {
          "_id": 0,
          "user_id": "$_id",
          "list_courses": 1
        }
      }
    ]

    # Execute the aggregation pipeline
    results = list(user_collection.aggregate(pipeline))
    return results

  except Exception as e:
    print("An error occurred:", e)
    return []

def create_topic_if_not_exists(topic_name, num_partitions=1, replication_factor=1):
  """
  Create a Kafka topic if it doesn't exist.
  :param topic_name: The name of the topic to create.
  :param num_partitions: Number of partitions for the topic.
  :param replication_factor: Replication factor for the topic.
  """
  admin_client = None  # Khởi tạo admin_client là None
  
  try:
    # Khởi tạo Kafka Admin Client
    admin_client = KafkaAdminClient(
        bootstrap_servers="kafka:29092",  # Sử dụng hostname của container Kafka
        client_id="kafka_admin"
    )

    # Lấy danh sách các topic đã tồn tại
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        print(f"Topic '{topic_name}' does not exist. Creating...")
        # Tạo topic mới
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully!")
    else:
        print(f"Topic '{topic_name}' already exists.")

    admin_client.close()
  except Exception as e:
    print(f"Error creating topic: {e}")

def custom_serializer(obj):
  if isinstance(obj, ObjectId):
    return str(obj)  # Chuyển ObjectId thành chuỗi
  elif isinstance(obj, datetime):
    return obj.isoformat()  # Chuyển datetime thành chuỗi ISO 8601
  raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def prepare_data(data):
  for record in data:
    for key, value in record.items():
      if isinstance(value, ObjectId):
        record[key] = str(value)  # Chuyển ObjectId thành chuỗi
      elif isinstance(value, datetime):
        record[key] = value.isoformat()  # Chuyển datetime thành chuỗi ISO 8601
  return data

def produce_data():
  """
  Fetch users with their courses and produce 10 users to Kafka every second.
  """

  topic_name = "enrolled_users"
  create_topic_if_not_exists(topic_name)

  producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v, default=custom_serializer).encode('utf-8')
  )
  print("----------- DONE: Connected to Kafka", producer.bootstrap_connected())

  users = prepare_data(get_enrolled_courses())
  print("----------- DONE: Get data from mongodb")

  for i in range(0, len(users), 10):
    batch = users[i:i + 10]
    
    for user in batch:
      producer.send(topic_name, user).get(timeout=30)
    print(f"Sent batch {i} of {len(batch)} users to Kafka.")

    time.sleep(1)

  producer.close()
  print("----------- DONE: Finished sending data.")

# produce_data()