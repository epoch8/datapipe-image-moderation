import uuid
from typing import List

import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON, String

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.store.database import TableStoreDB
from datapipe_image_moderation.google_vision import ImageModerationGoogle
from datapipe_image_moderation.pipeline import GoogleImageClassificationStep
from tests.fixtures.images import IMAGE_URLS
from tests.utils import assert_datatable_equal

CREDENTIALS_PATH = "tests/test_google_creds.json"

TEST_IMAGE_URLS_INPUT = pd.DataFrame(
    {
        "image_id": [str(uuid.uuid4()) for _ in IMAGE_URLS],
        "image_url": IMAGE_URLS,
    }
)


def image_classification_google(
    input_df: pd.DataFrame,
    input_primary_keys: List[str],
    details_field: str,
    image_field: str,
) -> pd.DataFrame:
    image_moderation_google_service = ImageModerationGoogle(google_credentials_path=CREDENTIALS_PATH)

    output_df = input_df[input_primary_keys].copy()
    output_df[details_field] = image_moderation_google_service.moderate_batch(images=input_df[image_field].tolist())
    return output_df


def test_google_step_with_urls(dbconn) -> None:
    """
    Тест для проверки шага классификации изображений в Google Vision API gRPC по их URL.

    :param dbconn: коннектор к БД (conftest.py)
    :return: None.
    """

    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "test_image_urls_input": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="test_image_urls_input",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("image_url", JSON, nullable=False),
                    ],
                    create_table=True,
                )
            ),
        }
    )

    google_step = GoogleImageClassificationStep(
        input="test_image_urls_input",
        output="test_image_urls_output",
        dbconn=dbconn,
        credentials_path=CREDENTIALS_PATH,
        image_field="image_url",
        details_field="details",
        step_name="test_image_classification_google_by_url",
    )

    pipeline = Pipeline([google_step])
    steps = build_compute(ds, catalog, pipeline)
    ds.get_table("test_image_urls_input").store_chunk(TEST_IMAGE_URLS_INPUT)

    test_result = image_classification_google(
        input_df=TEST_IMAGE_URLS_INPUT,
        input_primary_keys=["image_id"],
        image_field="image_url",
        details_field="details",
    )
    run_steps(ds, steps)
    print(f'TABLE DATA: {ds.get_table("test_image_urls_output").get_data()}')
    print(f"TEST DATA: {test_result}")
    assert_datatable_equal(ds.get_table("test_image_urls_output"), test_result)
