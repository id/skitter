import os

from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

TOPIC_INBOUND = "skitter/inbound/{chat_id}"
TOPIC_OUTBOUND = "skitter/outbound/{chat_id}"
TOPIC_JOBS = "skitter/jobs/{chat_id}"
TOPIC_TASKS = "skitter/tasks/{agent}/{chat_id}/{task_id}"
TOPIC_RESULTS = "skitter/results/{chat_id}/{task_id}"
TOPIC_WORKER_STATUS = "skitter/workers/{chat_id}/{task_id}/status"
TOPIC_USAGE = "skitter/usage/{chat_id}/{task_id}"
TOPIC_STREAM = "skitter/stream/{chat_id}/{task_id}"
TOPIC_STREAM_SNAPSHOT = "skitter/stream/{chat_id}/{task_id}/snapshot"
TOPIC_FEEDBACK = "skitter/feedback/{chat_id}/{task_id}"
TOPIC_CANCEL = "skitter/cancel/{chat_id}/{task_id}"
