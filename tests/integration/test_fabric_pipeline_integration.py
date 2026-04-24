"""
Integration tests for Fabric pipeline operations.

These tests require a live Fabric workspace and valid Azure credentials.
They are skipped automatically in CI unless RUN_INTEGRATION_TESTS=true is set.

Run manually:
    RUN_INTEGRATION_TESTS=true pytest tests/integration/ -v
"""

import os
import pytest

# Skip entire module unless explicitly opted in
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS", "false").lower() != "true",
    reason="Integration tests skipped. Set RUN_INTEGRATION_TESTS=true to run."
)

from src.utils.fabric_client import FabricClient, FabricConfig


@pytest.fixture(scope="module")
def fabric_client() -> FabricClient:
    """Real FabricClient pointing at the test workspace."""
    config = FabricConfig.from_env()
    return FabricClient(config)


class TestFabricWorkspaceConnectivity:

    def test_get_workspace_returns_id(self, fabric_client):
        workspace = fabric_client.get_workspace()
        assert "id" in workspace
        assert workspace["id"] == os.getenv("FABRIC_WORKSPACE_ID")

    def test_list_items_returns_list(self, fabric_client):
        items = fabric_client.list_items()
        assert isinstance(items, list)

    def test_list_pipeline_items(self, fabric_client):
        pipelines = fabric_client.list_items(item_type="DataPipeline")
        assert isinstance(pipelines, list)
        # In the test workspace we expect at least the ingest pipeline
        pipeline_names = [p.get("displayName", "") for p in pipelines]
        assert any("ingest" in name.lower() for name in pipeline_names), \
            f"Expected at least one 'ingest' pipeline, found: {pipeline_names}"


class TestFabricLakehouseConnectivity:

    def test_get_lakehouse_tables(self, fabric_client):
        lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID", "")
        if not lakehouse_id:
            pytest.skip("FABRIC_LAKEHOUSE_ID not set")

        tables = fabric_client.get_lakehouse_tables(lakehouse_id)
        assert isinstance(tables, list)
        table_names = [t.get("name", "") for t in tables]
        assert "sales_orders_raw" in table_names, \
            f"Expected 'sales_orders_raw' table in lakehouse. Found: {table_names}"
