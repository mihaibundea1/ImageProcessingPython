import redis
import json
import logging
from typing import Optional, List, Dict
from src.config import Config

logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self):
        self.config = Config()
        self.redis = redis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            db=self.config.REDIS_DB,
            password=self.config.REDIS_PASSWORD,
            decode_responses=True
        )
        self.image_prefix = "exercise:image:"

    def test_connection(self) -> bool:
        """Testează conexiunea la Redis"""
        try:
            return self.redis.ping()
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")
            return False
        
    def get_all_exercises(self) -> Optional[List[Dict]]:
        """Get all exercises from Redis"""
        try:
            exercises_data = self.redis.get("exercises:all")
            if exercises_data:
                return json.loads(exercises_data)
            return None
        except Exception as e:
            logger.error(f"Error getting all exercises: {e}")
            return None

    def get_exercises_without_thumbnails(self, limit: int = 50) -> List[Dict]:
        """Gets exercises needing thumbnail processing"""
        try:
            raw_data = self.redis.get("exercises:all")
            if not raw_data:
                return []

            all_exercises = json.loads(raw_data)
            needs_processing = []

            for exercise in all_exercises:
                if (
                    'image' in exercise 
                    and exercise.get('image', {}).get('uri')
                    and exercise.get('image', {}).get('thumbnail') is None
                ):
                    needs_processing.append(exercise)
                    if len(needs_processing) >= limit:
                        break

            return needs_processing

        except Exception as e:
            logger.error(f"Error getting exercises without thumbnails: {e}")
            return []
        
    def update_exercise_thumbnail(self, exercise_id: str, thumbnail_uri: str) -> bool:
        """Updates exercise thumbnail in Redis"""
        try:
            # Get current data
            raw_data = self.redis.get("exercises:all")
            if not raw_data:
                logger.error("No exercises found in Redis")
                return False

            exercises = json.loads(raw_data)
            updated = False

            # Update the specific exercise
            for exercise in exercises:
                if exercise['id'] == exercise_id and 'image' in exercise:
                    exercise['image']['thumbnail'] = thumbnail_uri
                    updated = True
                    break

            if updated:
                # Save back to Redis
                success = self.redis.set("exercises:all", json.dumps(exercises))
                if success:
                    logger.info(f"Successfully updated thumbnail for exercise {exercise_id}")
                    # Also save individual thumbnail
                    self.redis.set(f"{self.image_prefix}{exercise_id}", thumbnail_uri)
                    return True
                else:
                    logger.error(f"Failed to save updated exercises to Redis")
                    return False
            else:
                logger.warning(f"Exercise {exercise_id} not found in Redis")
                return False

        except Exception as e:
            logger.error(f"Error updating exercise thumbnail: {e}", exc_info=True)
            return False

    def get_thumbnail(self, exercise_id: str) -> Optional[str]:
        """Obține thumbnail pentru un exercițiu"""
        try:
            return self.redis.get(f"{self.image_prefix}{exercise_id}")
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return None
            
    def save_thumbnail(self, exercise_id: str, thumbnail_uri: str) -> bool:
        """Salvează thumbnail pentru un exercițiu"""
        try:
            key = f"{self.image_prefix}{exercise_id}"
            return self.redis.set(key, thumbnail_uri, ex=self.config.REDIS_TTL)
        except Exception as e:
            logger.error(f"Redis save error: {e}")
            return False

    def update_exercise_thumbnail(self, exercise_id: str, thumbnail_uri: str) -> bool:
        """Actualizează thumbnail-ul în lista completă de exerciții"""
        try:
            # Obține lista completă
            exercises_data = self.redis.get("exercises:all")
            if not exercises_data:
                logger.error("No exercises found in Redis")
                return False
                
            exercises = json.loads(exercises_data)
            updated = False
            
            # Actualizează thumbnail-ul
            for exercise in exercises:
                if exercise['id'] == exercise_id:
                    if 'image' not in exercise:
                        exercise['image'] = {}
                    exercise['image']['thumbnail'] = thumbnail_uri
                    updated = True
                    break
                    
            if updated:
                # Salvează înapoi în Redis
                self.redis.set("exercises:all", json.dumps(exercises))
                # Salvează și în cache-ul individual
                self.save_thumbnail(exercise_id, thumbnail_uri)
                logger.info(f"Updated thumbnail for exercise {exercise_id}")
                return True
                
            logger.warning(f"Exercise {exercise_id} not found in list")
            return False
            
        except Exception as e:
            logger.error(f"Error updating exercise thumbnail: {e}")
            return False

    def get(self, key: str) -> Optional[str]:
        """Metodă generală pentru a obține valori din Redis"""
        try:
            return self.redis.get(key)
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return None

    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Metodă generală pentru a seta valori în Redis"""
        try:
            return self.redis.set(key, value, ex=ex)
        except Exception as e:
            logger.error(f"Error setting key {key}: {e}")
            return False