# Datapipe Image Moderation

Python-package для модерации изображений в сервисах Google Cloud Vision & Yandex Cloud Vision.

## Содержание:

- [Использование](#использование)
  - [Установка](#установка)
  - [Запуск](#запуск)

## Использование

### Установка

```
poetry add git+ssh://git@github.com:epoch8/datapipe-image-moderation.git
```

### Запуск
Для модерации с помощью Google Cloud Vision:

```
from datapipe_image_moderation.pipeline import GoogleImageClassificationStep

image_urls: List[str] = []

pipeline = (
   [
      GoogleImageClassificationStep(
         input="название входной таблицы",
         output="название выходной таблицы",
         dbconn=коннектор к базе,
         credentials_path="путь к json-credentials файлу",
         image_field="название колонки с url изображений",
         details_field="желаемое название колонки с результатом классификации",
         step_name="название шага",
      )
   ]
)
```

Для модерации с помощью Yandex Cloud Vision:

```
from datapipe_image_moderation.pipeline import YandexImageClassificationStep

image_urls: List[str] = []

pipeline = (
   [
      YandexImageClassificationStep(
         input="название входной таблицы",
         output="название выходной таблицы",
         dbconn=коннектор к базе,
         folder_id="ID папки в Yandex Cloud",
         yandex_oauth_token="OAuth Token пользовательского аккаунта",
         image_field="название колонки с url изображений",
         details_field="желаемое название колонки с результатом классификации",
         step_name="название шага",
      )
   ]
)
```