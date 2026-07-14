"""Table access via Databricks SQL Warehouse (SDK StatementExecution API).

Clone of the ground-truth app's SqlClient. Auth: DEFAULT profile or
DATABRICKS_HOST + DATABRICKS_TOKEN env vars for local runs.
"""
from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from .config import config


class SqlClient:
    def __init__(self):
        self._w = WorkspaceClient()
        if not config.WAREHOUSE_ID:
            raise RuntimeError(
                "DATABRICKS_WAREHOUSE_ID is not set — required for table access."
            )
        self.warehouse_id = config.WAREHOUSE_ID

    @property
    def workspace(self) -> WorkspaceClient:
        return self._w

    # ── core executor ──────────────────────────────────────────────────────
    def execute(self, statement: str, parameters: list | None = None) -> list[dict]:
        """Run SQL and return rows as list[dict]. Empty list for non-SELECT.

        `parameters` is a list of StatementParameterListItem for parameterized
        queries (prevents SQL injection on user-supplied values).
        """
        resp = self._w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            parameters=parameters,
            wait_timeout="50s",
        )
        while resp.status and resp.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            resp = self._w.statement_execution.get_statement(resp.statement_id)

        if resp.status and resp.status.state != StatementState.SUCCEEDED:
            err = resp.status.error
            msg = err.message if err else "unknown error"
            raise RuntimeError(f"SQL failed ({resp.status.state}): {msg}")

        return self._rows_to_dicts(resp)

    @staticmethod
    def _rows_to_dicts(resp) -> list[dict]:
        result = resp.result
        manifest = resp.manifest
        if not result or not result.data_array:
            return []
        cols = [c.name for c in manifest.schema.columns]
        return [dict(zip(cols, row)) for row in result.data_array]

    # ── helper for named string params ─────────────────────────────────────
    @staticmethod
    def str_param(name: str, value: str):
        from databricks.sdk.service.sql import StatementParameterListItem

        return StatementParameterListItem(name=name, value=value, type="STRING")


_client: SqlClient | None = None


def get_sql() -> SqlClient:
    global _client
    if _client is None:
        _client = SqlClient()
    return _client
