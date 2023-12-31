from typing import List, Optional

from google.cloud import vision
from google.oauth2 import service_account

from datapipe_image_moderation.utils import get_bytes_images


class ImageModerationGoogle:
    """
    Класс клиента модерации изображений в Google Cloud Vision gRPC.
    """

    def __init__(self, google_credentials_path: Optional[str] = None) -> None:
        if google_credentials_path is None:
            self._google_vision_client = vision.ImageAnnotatorClient()
        else:
            google_credentials = service_account.Credentials.from_service_account_file(google_credentials_path)
            self._google_vision_client = vision.ImageAnnotatorClient(credentials=google_credentials)

    def moderate_batch(
        self,
        images: List[str],
        file_system_name: str,
        file_system_creds_path: Optional[str] = None,
    ) -> List:
        """
        Метод массовой модерации изображений с помощью Google Cloud Vision gRPC.

        :param images: список изображений в виде URL или Bytes.
        :param file_system_name: файловая система, где находится изображение.
        :param file_system_creds_path: путь к credentials для файловой системы (опционально).
        :return: результат модерации.
        """

        # Проверяем количество переданных изображений на соответствие лимитам Google Cloud Vision gRPC.
        if len(images) > 15:
            raise ValueError("Количество изображений должно быть меньше или равно 15!")

        # Получаем список изображений в формате base64.
        bytes_images = get_bytes_images(
            image_url_list=images,
            file_system_name=file_system_name,
            file_system_creds_path=file_system_creds_path,
        )

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
