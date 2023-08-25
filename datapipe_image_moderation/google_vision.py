from typing import List

from google.cloud import vision
from google.oauth2 import service_account

from datapipe_image_moderation.utils import get_bytes_image


class ImageModerationGoogle:
    """
    Класс клиента модерации изображений в Google Cloud Vision gRPC.
    """

    def __init__(self, google_credentials: service_account.Credentials) -> None:
        self._google_vision_client = vision.ImageAnnotatorClient(credentials=google_credentials)

    def moderate_batch(self, images: List[str]) -> List:
        """
        Метод массовой модерации изображений с помощью Google Cloud Vision gRPC.

        :param images: список изображений в виде URL или Bytes.
        :return: результат модерации.
        """

        # Проверяем количество переданных изображений на соответствие лимитам Google Cloud Vision gRPC.
        if len(images) > 15:
            raise ValueError("Количество изображений должно быть меньше или равно 15!")

        # Получаем список изображений в формате base64.
        bytes_images = [get_bytes_image(image_url=image) for image in images]

        # Формируем тип модерации в Google Cloud Vision gRPC.
        features = [{"type_": vision.Feature.Type.SAFE_SEARCH_DETECTION}]

        # Формируем список изображений в формате "vision.AnnotateImageRequest".
        vision_images = [{"image": vision.Image({"content": image}), "features": features} for image in bytes_images]

        # Формируем запрос для получения модерации изображений в Google Cloud Vision gRPC.
        request = vision.BatchAnnotateImagesRequest({"requests": vision_images})

        # Получаем результат batch-модерации изображений в Google Cloud Vision gRPC.
        response = self._google_vision_client.batch_annotate_images(request=request)

        # Формируем результаты модерации.
        details = []
        for resp in response.responses:
            details.append(
                {
                    "adult": resp.safe_search_annotation.adult.name,
                    "spoof": resp.safe_search_annotation.spoof.name,
                    "medical": resp.safe_search_annotation.medical.name,
                    "violence": resp.safe_search_annotation.violence.name,
                    "racy": resp.safe_search_annotation.racy.name,
                }
            )

        return details
