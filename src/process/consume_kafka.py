from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json

def consume_data():
  """
  Lắng nghe và xử lý dữ liệu từ Kafka topic "enrolled_users"
  """
  topic_name = "enrolled_users"

  # Khởi tạo Kafka consumer
  consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='kafka:29092',  # Địa chỉ Kafka
    # group_id='consumer-group-1',  # Đặt group ID cho consumer
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize từ JSON
  )

  print(f"----------- DONE: Subscribed to Kafka topic {topic_name}", consumer.bootstrap_connected())

  # Tiếp tục lắng nghe các message mới
  for message in consumer:
    # Mỗi message sẽ được là một dict sau khi được deserialized từ JSON
    print(f"Received message: {message.value}")

    # Bạn có thể xử lý message tại đây
    # Ví dụ: in thông tin user
    user = message.value
    print(f"User info: {user}")

  consumer.close()
  print("----------- DONE: Closed Kafka consumer.")

def delete_topic(topic_name):
  try:
    # Khởi tạo KafkaAdminClient
    admin_client = KafkaAdminClient(
      bootstrap_servers='kafka:29092',  # Thay bằng địa chỉ broker của bạn
      client_id='kafka_admin'
    )

    # Xóa topic
    admin_client.delete_topics([topic_name])
    print(f"Topic '{topic_name}' has been deleted successfully.")
    
    # Đóng client sau khi xong
    admin_client.close()

  except Exception as e:
    print(f"Error deleting topic '{topic_name}': {e}")

# consume_data()
