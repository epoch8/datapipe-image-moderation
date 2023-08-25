from dataclasses import dataclass
from typing import List, Optional, Union

import pandas as pd
import sqlalchemy as sa

from datapipe.compute import Catalog, ComputeStep, DataStore, ExecutorConfig, Labels, PipelineStep, Table
from datapipe.core_steps import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe_image_moderation.google_vision import ImageModerationGoogle
from datapipe_image_moderation.yandex_vision import ImageModerationYandex


@dataclass
class YandexImageClassificationStep(PipelineStep):
    """
    Шаг пайплайна для классификации изображений через Yandex Cloud Vision gRPC.
    """

    input: str  # Input Table name.
    output: str  # Output Table name.
    dbconn: Union[DBConn, str]  # Database Connection.

    folder_id: str  # Folder ID with access in Yandex Cloud.
    yandex_oauth_token: str  # Yandex Passport OAuth Token of user account.

    image_field: str = "image_url"  # Name of Field with Image URL or Image Bytes.
    details_field: str = "details"  # Name of Field for write classification result.
    step_name: str = "image_classification_yandex"  # Name of Step.

    create_table: bool = True
    executor_config: Optional[ExecutorConfig] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dt = catalog.get_datatable(ds, self.input)

        output_dt = ds.get_or_create_table(
            self.output,
            TableStoreDB(
                dbconn=self.dbconn,
                name=self.output,
                data_sql_schema=input_dt.primary_schema + [sa.Column("details", sa.JSON)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(self.output, Table(output_dt.table_store))

        def image_classification_yandex(input_df: pd.DataFrame) -> pd.DataFrame:
            image_moderation_yandex_service = ImageModerationYandex(
                oauth_token=self.yandex_oauth_token,
                folder_id=self.folder_id,
            )

            output_df = input_df[input_dt.primary_keys].copy()
            output_df[self.details_field] = image_moderation_yandex_service.moderate_batch(
                images=input_df[self.image_field].tolist(),
            )
            return output_df

        return [
            BatchTransformStep(
                ds=ds,
                name=self.step_name,
                input_dts=[input_dt],
                output_dts=[output_dt],
                func=image_classification_yandex,
                chunk_size=5,
                labels=self.labels,
                executor_config=self.executor_config,
            )
        ]


@dataclass
class GoogleImageClassificationStep(PipelineStep):
    """
    Шаг пайплайна для классификации изображений через Google Cloud Vision gRPC.
    """

    input: str  # Input Table name.
    output: str  # Output Table name.
    dbconn: Union[DBConn, str]  # Database Connection.

    credentials_path: Optional[str] = None  # Folder ID with access in Yandex Cloud.
    image_field: str = "image_url"  # Name of Field with Image URL or Image Bytes.
    details_field: str = "details"  # Name of Field for write classification result.
    step_name: str = "image_classification_google"  # Name of Step.

    create_table: bool = True
    executor_config: Optional[ExecutorConfig] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dt = catalog.get_datatable(ds, self.input)

        output_dt = ds.get_or_create_table(
            self.output,
            TableStoreDB(
                dbconn=self.dbconn,
                name=self.output,
                data_sql_schema=input_dt.primary_schema + [sa.Column("details", sa.JSON)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(self.output, Table(output_dt.table_store))

        def image_classification_google(input_df: pd.DataFrame) -> pd.DataFrame:
            image_moderation_google_service = ImageModerationGoogle(google_credentials_path=self.credentials_path)

            output_df = input_df[input_dt.primary_keys].copy()
            output_df[self.details_field] = image_moderation_google_service.moderate_batch(
                images=input_df[self.image_field].tolist(),
            )
            return output_df

        return [
            BatchTransformStep(
                ds=ds,
                name=self.step_name,
                input_dts=[input_dt],
                output_dts=[output_dt],
                func=image_classification_google,
                chunk_size=15,
                labels=self.labels,
                executor_config=self.executor_config,
            )
        ]
