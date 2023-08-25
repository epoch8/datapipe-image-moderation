import requests


def get_bytes_image(image_url: str) -> bytes:
    """
    Метод для получения изображения из URL в bytes.

    :param image_url: ссылка на изображение.
    :return: изображение в bytes.
    """

    image_bytes = requests.request(method="GET", url=image_url, timeout=300).content
    return image_bytes
