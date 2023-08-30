from typing import List, Optional

import fsspec


def get_bytes_images(
    image_url_list: List[str],
    file_system_name: str,
    file_system_creds_path: Optional[str] = None,
) -> List[bytes]:
    """
    Метод для получения изображения из URL в bytes.

    :param image_url_list: Список ссылок на изображения.
    :param file_system_name: файловая система, где находится изображение.
    :param file_system_creds_path: путь к credentials для файловой системы (опционально).
    :return: изображение в bytes.
    """

    if file_system_creds_path is not None:
        file_system = fsspec.filesystem(file_system_name, token=file_system_creds_path)
    else:
        file_system = fsspec.filesystem(file_system_name)

    image_bytes_list: List[bytes] = []

    for image_url in image_url_list:
        with file_system.open(image_url, "rb") as image_file:
            image_bytes = image_file.read()
            image_bytes_list.append(image_bytes)

    return image_bytes_list
