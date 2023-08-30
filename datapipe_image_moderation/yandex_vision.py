from typing import List, Optional

import yandexcloud
from yandex.cloud.ai.vision.v1.vision_service_pb2 import (
    AnalyzeSpec,
    BatchAnalyzeRequest,
    Feature,
    FeatureClassificationConfig,
)
from yandex.cloud.ai.vision.v1.vision_service_pb2_grpc import VisionServiceStub

from datapipe_image_moderation.utils import get_bytes_images


class ImageModerationYandex:
    """
    Класс клиента модерации изображений в Yandex Cloud Vision gRPC.
    """

    def __init__(self, oauth_token: str, folder_id: str) -> None:
        """
        Метод инициализации класса ImageModerationYandex.

        :param oauth_token: Yandex Passport OAuth Token пользовательского аккаунта.
        :param folder_id: Идентификатор каталога, к которому у вас есть доступ.
        Требуется для авторизации с пользовательским аккаунтом.
        """

        self._yandex_vision_client = yandexcloud.SDK(token=oauth_token).client(VisionServiceStub)
        self._folder_id = folder_id

    @staticmethod
    def _get_analyze_specs(bytes_images: List[bytes]) -> List[AnalyzeSpec]:
        """
        Метод для получения массива модерируемых изображений в формате AnalyzeSpec.

        :param bytes_images: список изображений в формате bytes.
        :return: List[AnalyzeSpec].
        """

        analyze_specs: List[AnalyzeSpec] = []

        for bytes_image in bytes_images:
            analyze_spec = AnalyzeSpec(
                content=bytes_image,
                features=[
                    Feature(
                        type=2,
                        classification_config=FeatureClassificationConfig(model="moderation"),
                    )
                ],
            )
            analyze_specs.append(analyze_spec)

        return analyze_specs

    def moderate_batch(
        self,
        images: List[str],
        file_system_name: str,
        file_system_creds_path: Optional[str] = None,
    ) -> List:
        """
        Метод массовой модерации изображений с помощью Yandex Cloud Vision gRPC.

        :param images: список изображений в виде URL или Bytes.
        :param file_system_name: файловая система, где находится изображение.
        :param file_system_creds_path: путь к credentials для файловой системы (опционально).
        :return: результат модерации.
        """

        # Проверяем количество переданных изображений на соответствие лимитам Yandex Cloud Vision gRPC.
        if len(images) > 5:
            raise ValueError("Количество изображений должно быть меньше или равно 5!")

        # Получаем список изображений в формате base64.
        bytes_images = get_bytes_images(
            image_url_list=images,
            file_system_name=file_system_name,
            file_system_creds_path=file_system_creds_path,
        )

        # Формируем запрос для Yandex Cloud Vision gRPC.
        request = BatchAnalyzeRequest(
            folder_id=self._folder_id,
            analyze_specs=self._get_analyze_specs(bytes_images=bytes_images),
        )

        # Получаем результат batch-модерации изображений в Yandex Cloud Vision API.
        response = self._yandex_vision_client.BatchAnalyze(request)

        # Формируем результаты модерации (заполняем default значения на случай ошибки в Yandex Cloud Vision).
        details = [{"adult": 0, "gruesome": 0, "text": 0, "watermarks": 0} for _ in range(len(images))]

        # Заполняем данные из Yandex Vision gRPC API.
        for i, result in enumerate(response.results):
            properties = result.results[0].classification.properties

            for prop in properties:
                details[i][prop.name] = prop.probability

        return details
