import pika
import time
import logging
import json
from typing import Callable
from src.config import Config
from src.utils.logger import setup_logger

class RabbitMQService:
    def __init__(self):
        self.logger = setup_logger('RabbitMQService')
        self.config = Config()
        self.connection = None
        self.channel = None
        self.should_reconnect = True
        self.reconnect_delay = 5
        self.max_reconnect_delay = 30

    def publish(self, queue_name: str, message: dict, persistent: bool = True) -> bool:
        """Publishes a message to a queue"""
        try:
            # Ensure we have a connection
            if not self.connect():
                return False

            # Convert message to JSON
            message_body = json.dumps(message)

            # Declare queue to ensure it exists
            self.channel.queue_declare(
                queue=queue_name, 
                durable=True
            )

            # Publish the message
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2 if persistent else 1  # 2 = persistent
                )
            )

            self.logger.debug(f"Published message to {queue_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to publish message: {str(e)}", exc_info=True)
            # Try to cleanup and reconnect
            self.cleanup()
            return False

    def connect(self):
        """Establishes connection to RabbitMQ server with retry logic"""
        while self.should_reconnect:
            try:
                if self.connection is None or self.connection.is_closed:
                    self.logger.info(f"Attempting to connect to RabbitMQ at {self.config.RABBITMQ_HOST}:{self.config.RABBITMQ_PORT}")
                    self.connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=self.config.RABBITMQ_HOST,
                            port=self.config.RABBITMQ_PORT,
                            heartbeat=600,
                            blocked_connection_timeout=300,
                            connection_attempts=3,
                            retry_delay=5
                        )
                    )
                    
                if self.channel is None or self.channel.is_closed:
                    self.channel = self.connection.channel()
                    self.channel.queue_declare(queue=self.config.RABBITMQ_QUEUE, durable=True)
                    self.channel.basic_qos(prefetch_count=1)
                    self.logger.info("Successfully connected to RabbitMQ")
                    self.reconnect_delay = 5
                    return True
                    
            except Exception as e:
                self.logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
                time.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                continue
                
        return False

    def start_consuming(self, callback: Callable):
        """Starts consuming messages with automatic reconnection"""
        while self.should_reconnect:
            try:
                if not self.connect():
                    continue

                self.channel.basic_consume(
                    queue=self.config.RABBITMQ_QUEUE,
                    on_message_callback=callback
                )
                
                self.logger.info("Starting to consume messages...")
                self.channel.start_consuming()
                
            except (pika.exceptions.ConnectionClosedByBroker,
                    pika.exceptions.AMQPChannelError,
                    pika.exceptions.AMQPConnectionError) as e:
                self.logger.warning(f"Connection or channel closed: {str(e)}")
                self.cleanup()
                continue
                
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                self.cleanup()
                continue

            time.sleep(self.reconnect_delay)

    def cleanup(self):
        """Clean up connection and channel"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
        except Exception:
            pass
            
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception:
            pass
            
        self.channel = None
        self.connection = None

    def close(self):
        """Gracefully close the connection"""
        self.should_reconnect = False
        self.cleanup()
        self.logger.info("RabbitMQ service shut down")

    def __del__(self):
        """Ensure resources are cleaned up"""
        self.close()