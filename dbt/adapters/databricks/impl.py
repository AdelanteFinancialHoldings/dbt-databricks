from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from agate import Row, Table

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.spark.impl import (
    SparkAdapter,
    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
    KEY_TABLE_OWNER,
    KEY_TABLE_STATISTICS,
    LIST_RELATIONS_MACRO_NAME,
    LIST_SCHEMAS_MACRO_NAME,
    TABLE_OR_VIEW_NOT_FOUND_MESSAGES,
)
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import RelationType
import dbt.exceptions
from dbt.events import AdapterLogger
from dbt.utils import executor

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.utils import undefined_proof


logger = AdapterLogger("Databricks")

CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
LIST_TABLES_MACRO_NAME = 'databricks__list_tables_without_caching'
LIST_VIEWS_MACRO_NAME = 'databricks__list_views_without_caching'

@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    tblproperties: Optional[Dict[str, str]] = None


@undefined_proof
class DatabricksAdapter(SparkAdapter):

    Relation = DatabricksRelation
    Column = DatabricksColumn

    ConnectionManager = DatabricksConnectionManager
    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    def list_schemas(self, database: Optional[str]) -> List[str]:
        """
        Get a list of existing schemas in database.

        If `database` is `None`, fallback to executing `show databases` because
        `list_schemas` tries to collect schemas from all catalogs when `database` is `None`.
        """
        if database is not None:
            results = self.connections.list_schemas(database=database)
        else:
            results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})
        return [row[0] for row in results]

    def check_schema_exists(self, database: Optional[str], schema: str) -> bool:
        """Check if a schema exists."""
        return schema.lower() in set(s.lower() for s in self.list_schemas(database=database))

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        *,
        staging_table: Optional[BaseRelation] = None,
    ) -> Tuple[AdapterResponse, Table]:
        try:
            return super().execute(sql=sql, auto_begin=auto_begin, fetch=fetch)
        finally:
            if staging_table is not None:
                self.drop_relation(staging_table)

    def list_relations_without_caching(
        self, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        kwargs = {'relation': schema_relation}
        try:
            # The catalog for `show table extended` needs to match the current catalog.
            tables = self.execute_macro(
                LIST_TABLES_MACRO_NAME,
                kwargs=kwargs
            )
            views = self.execute_macro(
                LIST_VIEWS_MACRO_NAME,
                 kwargs=kwargs
            )
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for tbl in tables:
            rel_type = ('view' if tbl['tableName'] in views.columns["viewName"].values() else 'table')
            relation = self.Relation.create(
                schema=tbl['database'],
                identifier=tbl['tableName'],
                type=rel_type,
            )
            relations.append(relation)
        #for row in results:
        #    if len(row) != 4:
        #        raise dbt.exceptions.RuntimeException(
        #            f'Invalid value from "show table extended ...", '
        #            f"got {len(row)} values, expected 4"
        #        )
        #    _schema, name, _, information = row
        #    rel_type = RelationType.View if "Type: VIEW" in information else RelationType.Table
        #    is_delta = "Provider: delta" in information
        #    is_hudi = "Provider: hudi" in information
        #    relation = self.Relation.create(
        #        database=schema_relation.database,
        #        schema=_schema,
        #        identifier=name,
        #        type=rel_type,
        #        information=information,
        #        is_delta=is_delta,
        #        is_hudi=is_hudi,
        #    )
        #    relations.append(relation)

        return relations
    
    def get_relation(
        self,
        database: Optional[str],
        schema: str,
        identifier: str,
        *,
        needs_information: bool = False,
    ) -> Optional[DatabricksRelation]:
        cached: Optional[DatabricksRelation] = super(SparkAdapter, self).get_relation(
            database=database, schema=schema, identifier=identifier
        )

        if not needs_information:
            return cached

        return self._set_relation_information(cached) if cached else None

    def parse_describe_extended(
        self, relation: DatabricksRelation, raw_rows: List[Row]
    ) -> List[DatabricksColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)
        return [
            DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    def get_columns_in_relation(  # type: ignore[override]
        self, relation: DatabricksRelation
    ) -> List[DatabricksColumn]:
        return self._get_updated_relation(relation)[1]

    def _get_updated_relation(
        self, relation: DatabricksRelation
    ) -> Tuple[DatabricksRelation, List[DatabricksColumn]]:
        try:
            rows = self.execute_macro(
                GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME, kwargs={"relation": relation}
            )
            metadata, columns = self.parse_describe_extended(relation, rows)
        except dbt.exceptions.RuntimeException as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                metadata = None
                columns = []
            else:
                raise e

        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]

        return (
            self.Relation.create(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier,
                type=relation.type,
                metadata=metadata,
            ),
            columns,
        )

    def _set_relation_information(self, relation: DatabricksRelation) -> DatabricksRelation:
        """Update the information of the relation, or return it if it already exists."""
        if relation.has_information():
            return relation

        return self._get_updated_relation(relation)[0]

    def parse_columns_from_information(
        self, relation: DatabricksRelation
    ) -> List[DatabricksColumn]:
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, relation.information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, relation.information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, relation.information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def get_catalog(self, manifest: Manifest) -> Tuple[Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(manifest)

        with executor(self.config) as tpe:
            futures: List[Future[Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self, schema, self._get_one_catalog, info, [schema], manifest
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_columns_for_catalog(self, relation: DatabricksRelation) -> Iterable[Dict[str, Any]]:
        columns = self.parse_columns_from_information(relation)

        for column in columns:
            # convert DatabricksRelation into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            yield as_dict

    @contextmanager
    def _catalog(self, catalog: Optional[str]) -> Iterator[None]:
        """
        A context manager to make the operation work in the specified catalog,
        and move back to the current catalog after the operation.

        If `catalog` is None, the operation works in the current catalog.
        """
        current_catalog: Optional[str] = None
        try:
            if catalog is not None:
                current_catalog = self.execute_macro(CURRENT_CATALOG_MACRO_NAME)[0][0]
                if current_catalog is not None:
                    if current_catalog != catalog:
                        self.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=catalog))
                    else:
                        current_catalog = None
            yield
        finally:
            if current_catalog is not None:
                self.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=current_catalog))