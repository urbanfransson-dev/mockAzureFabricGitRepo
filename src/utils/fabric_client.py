"""
fabric_client.py
----------------
Utility wrapper for the Microsoft Fabric REST API.
Provides helpers for workspace, lakehouse, and pipeline operations.

Reference: https://learn.microsoft.com/en-us/rest/api/fabric/
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
TOKEN_SCOPE     = "https://api.fabric.microsoft.com/.default"


@dataclass
class FabricConfig:
    workspace_id:    str
    tenant_id:       str
    client_id:       str
    api_base:        str = FABRIC_API_BASE
    request_timeout: int = 60

    @classmethod
    def from_env(cls) -> "FabricConfig":
        return cls(
            workspace_id=  _require_env("FABRIC_WORKSPACE_ID"),
            tenant_id=     _require_env("AZURE_TENANT_ID"),
            client_id=     _require_env("AZURE_CLIENT_ID"),
            api_base=      os.getenv("FABRIC_API_BASE", FABRIC_API_BASE),
            request_timeout=int(os.getenv("FABRIC_API_TIMEOUT", "60")),
        )


class FabricClient:
    """Thin wrapper around the Microsoft Fabric REST API."""

    def __init__(self, config: FabricConfig) -> None:
        self.config = config
        self._credential = DefaultAzureCredential()
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })
        logger.info("FabricClient initialised for workspace %s", config.workspace_id)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        token = self._credential.get_token(TOKEN_SCOPE)
        return token.token

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ------------------------------------------------------------------
    # Generic HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.config.api_base}{path}"
        resp = self._session.get(
            url, headers=self._auth_headers(),
            timeout=self.config.request_timeout, **kwargs
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.config.api_base}{path}"
        resp = self._session.post(
            url, json=body, headers=self._auth_headers(),
            timeout=self.config.request_timeout
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    # ------------------------------------------------------------------
    # Workspace operations
    # ------------------------------------------------------------------

    def get_workspace(self) -> Dict[str, Any]:
        """Retrieve workspace metadata."""
        return self._get(f"/workspaces/{self.config.workspace_id}")

    def list_items(self, item_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """List items in the workspace, optionally filtered by type."""
        params = {}
        if item_type:
            params["type"] = item_type
        result = self._get(f"/workspaces/{self.config.workspace_id}/items", params=params)
        return result.get("value", [])

    # ------------------------------------------------------------------
    # Pipeline operations
    # ------------------------------------------------------------------

    def run_pipeline(self, pipeline_id: str, parameters: Optional[Dict] = None) -> str:
        """Trigger a Fabric data pipeline run.

        Args:
            pipeline_id: The item ID of the pipeline.
            parameters:  Runtime parameters to pass to the pipeline.

        Returns:
            The run ID (operation ID) of the triggered pipeline run.
        """
        body: Dict[str, Any] = {}
        if parameters:
            body["parameters"] = parameters

        resp = self._post(
            f"/workspaces/{self.config.workspace_id}/dataPipelines/{pipeline_id}/jobs/instances?jobType=Pipeline",
            body=body,
        )
        run_id = resp.get("id", "unknown")
        logger.info("Pipeline %s triggered — run_id=%s", pipeline_id, run_id)
        return run_id

    def get_pipeline_run_status(self, pipeline_id: str, run_id: str) -> Dict[str, Any]:
        """Get the status of a pipeline run."""
        return self._get(
            f"/workspaces/{self.config.workspace_id}/dataPipelines/{pipeline_id}/jobs/instances/{run_id}"
        )

    def wait_for_pipeline(
        self,
        pipeline_id: str,
        run_id: str,
        poll_interval_s: int = 30,
        timeout_s: int = 7200,
    ) -> str:
        """Poll a pipeline run until it completes or times out.

        Args:
            pipeline_id:     The pipeline item ID.
            run_id:          The run instance ID returned by run_pipeline().
            poll_interval_s: Seconds between status polls.
            timeout_s:       Maximum wait time in seconds.

        Returns:
            Final status string (e.g. "Succeeded", "Failed", "Cancelled").

        Raises:
            TimeoutError: If the pipeline does not finish within timeout_s.
        """
        terminal_statuses = {"Succeeded", "Failed", "Cancelled", "Deduped"}
        elapsed = 0

        while elapsed < timeout_s:
            status_resp = self.get_pipeline_run_status(pipeline_id, run_id)
            status = status_resp.get("status", "Unknown")
            logger.info("Pipeline %s run %s — status=%s (elapsed=%ds)", pipeline_id, run_id, status, elapsed)

            if status in terminal_statuses:
                return status

            time.sleep(poll_interval_s)
            elapsed += poll_interval_s

        raise TimeoutError(
            f"Pipeline {pipeline_id} run {run_id} did not finish within {timeout_s}s. "
            f"Last status: {status}"
        )

    # ------------------------------------------------------------------
    # Lakehouse operations
    # ------------------------------------------------------------------

    def get_lakehouse_tables(self, lakehouse_id: str) -> List[Dict[str, Any]]:
        """List tables registered in a lakehouse."""
        result = self._get(
            f"/workspaces/{self.config.workspace_id}/lakehouses/{lakehouse_id}/tables"
        )
        return result.get("data", [])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            f"See .env.dev.example for configuration."
        )
    return value
