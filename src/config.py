from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    # RabbitMQ
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
    RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
    RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
    
    # Redis
    REDIS_HOST = os.getenv('REDIS_HOST')
    REDIS_PORT = int(os.getenv('REDIS_PORT'))
    REDIS_DB = int(os.getenv('REDIS_DB'))
    REDIS_TTL = int(os.getenv('REDIS_TTL'))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')  # Adaugă parola
    
    # AWS
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
    AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')