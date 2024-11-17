import json
from src.services.image_processor import ImageProcessor
from src.services.s3_service import S3Service
from src.services.redis_service import RedisService
from src.services.rabbitmq_service import RabbitMQService
from src.utils.logger import setup_logger
import pika
import time


class ImageConsumer:
    def __init__(self):
        self.logger = setup_logger('ImageConsumer')
        self.logger.info("Initializing ImageConsumer...")
        self.BATCH_SIZE = 10
        
        try:
            self.redis_service = RedisService()
            self.s3_service = S3Service()
            self.image_processor = ImageProcessor(self.s3_service)
            self.rabbitmq_service = RabbitMQService()
            
            if not self.redis_service.test_connection():
                raise Exception("Could not connect to Redis")
                
        except Exception as e:
            self.logger.error(f"Error during initialization: {str(e)}", exc_info=True)
            raise
    def process_all_remaining(self):
        """Process all remaining exercises"""
        try:
            while True:
                status = self.get_processing_status()
                if status['remaining'] == 0:
                    self.logger.info("All exercises have been processed")
                    break

                exercises = self.redis_service.get_exercises_without_thumbnails(self.BATCH_SIZE)
                if not exercises:
                    self.logger.info("No more exercises to process")
                    break

                self.logger.info(f"Processing next batch of {len(exercises)} exercises")
                if not self.process_batch(exercises):
                    self.logger.error("Failed to process batch, will retry")
                    time.sleep(1)  # Small delay before retry
                    continue

                # Small delay between batches to prevent overload
                time.sleep(0.1)

        except Exception as e:
            self.logger.error(f"Error in process_all_remaining: {str(e)}", exc_info=True)
            raise

    def get_processing_status(self):
        """Get current processing status of all exercises"""
        try:
            all_exercises = self.redis_service.get_all_exercises()
            if not all_exercises:
                return {'total': 0, 'processed': 0, 'remaining': 0}

            total = len(all_exercises)
            needs_processing = sum(1 for exercise in all_exercises 
                                 if 'image' in exercise 
                                 and exercise.get('image', {}).get('uri')
                                 and exercise.get('image', {}).get('thumbnail') is None)
            
            return {
                'total': total,
                'processed': total - needs_processing,
                'remaining': needs_processing
            }
        except Exception as e:
            self.logger.error(f"Error getting processing status: {str(e)}")
            return {'total': 0, 'processed': 0, 'remaining': 0}

    def process_batch(self, exercises):
        """Process a batch of exercises"""
        success_count = 0
        try:
            total_exercises = len(exercises)
            for i, exercise in enumerate(exercises, 1):
                try:
                    exercise_id = exercise['id']
                    image_url = exercise.get('image', {}).get('uri')
                    
                    if not image_url:
                        self.logger.warning(f"No image URL for exercise {exercise_id}")
                        continue
                    
                    self.logger.info(f"Processing exercise {i}/{total_exercises} - ID: {exercise_id}")
                    
                    if 'proveit-exercises-directories.s3' in image_url:
                        path_parts = image_url.split('?')[0].split('proveit-exercises-directories.s3.amazonaws.com/')[1]
                        self.logger.debug(f"Processing path: {path_parts}")
                        
                        if thumbnail_uri := self.image_processor.process_image(exercise_id=exercise_id, image_url=path_parts):
                            if self.redis_service.update_exercise_thumbnail(exercise_id, thumbnail_uri):
                                success_count += 1
                                self.logger.info(f"Successfully processed {exercise_id} ({success_count}/{total_exercises})")
                            else:
                                self.logger.error(f"Failed to update Redis for {exercise_id}")
                        else:
                            self.logger.error(f"Failed to process image for {exercise_id}")
                    else:
                        self.logger.warning(f"Skipping {exercise_id} - Invalid S3 URL")
                            
                except Exception as e:
                    self.logger.error(f"Error processing exercise {exercise.get('id', 'unknown')}: {str(e)}", exc_info=True)
                    continue
                    
            status = self.get_processing_status()
            self.logger.info(
                f"Batch complete - Processed: {success_count}/{total_exercises} exercises. "
                f"Overall progress: {status['processed']}/{status['total']} "
                f"({status['remaining']} remaining)"
            )
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Batch processing error: {str(e)}", exc_info=True)
            return False

    def callback(self, ch, method, properties, body):
        try:
            if self.processing:
                # If already processing, just acknowledge and return
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            data = json.loads(body)
            
            if data.get('action') == 'process_exercises':
                self.processing = True
                try:
                    # Process all remaining exercises
                    self.process_all_remaining()
                finally:
                    self.processing = False
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self.logger.warning(f"Unknown action: {data.get('action')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            self.logger.error(f"Callback error: {str(e)}", exc_info=True)
            if not ch.is_closed:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.processing = False
            raise


    def run(self):
        self.logger.info("Starting consumer...")
        while True:
            try:
                self.rabbitmq_service.start_consuming(self.callback)
            except KeyboardInterrupt:
                self.logger.info("Shutting down...")
                self.rabbitmq_service.close()
                break
            except (pika.exceptions.ConnectionClosedByBroker,
                    pika.exceptions.AMQPChannelError,
                    pika.exceptions.AMQPConnectionError) as e:
                self.logger.warning(f"Connection lost, reconnecting... Error: {str(e)}")
                self.processing = False  # Reset processing flag on connection error
                continue
            except Exception as e:
                self.logger.error(f"Fatal error: {str(e)}")
                self.processing = False  # Reset processing flag on error
                raise