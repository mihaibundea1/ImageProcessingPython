from PIL import Image
import io
import base64
import logging
from typing import Optional
from src.services.s3_service import S3Service

logger = logging.getLogger(__name__)

class ImageProcessor:
    def __init__(self, s3_service: S3Service):
        self.s3_service = s3_service
        self.thumbnail_size = (128, 128)
        
    def process_image(self, exercise_id: str, image_url: str) -> Optional[str]:
        try:
            image_data = self.s3_service.get_image(image_url)
            if not image_data:
                return None
                
            with Image.open(io.BytesIO(image_data)) as img:
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                    
                img.thumbnail(self.thumbnail_size, Image.Resampling.LANCZOS)
                
                buffer = io.BytesIO()
                img.save(buffer, format='JPEG', quality=85, optimize=True)
                image_data = buffer.getvalue()
                
                image_base64 = base64.b64encode(image_data).decode('utf-8')
                return f"data:image/jpeg;base64,{image_base64}"
                
        except Exception as e:
            logger.error(f"Image processing error: {e}")
            return None
