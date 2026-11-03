# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Describe how shell drivers address managed engine resources in Kubernetes.

Each engine declares resource names, status paths, selectors, and API details
so drivers can share lifecycle operations.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

# Placeholder replaced with the resource name by the caller.
NAME = "<name>"


@dataclass(frozen=True)
class CompactorKubernetes:
    """Describe how a managed compactor's admitted pod fleet is identified."""

    fleet_selector: str
    pod_role_label: str
    role_map: tuple[tuple[str, str], ...]

    def texts(self) -> dict[str, str]:
        """Serialize fields consumed by shell drivers."""
        return {"fleet_selector": self.fleet_selector}


COMPACTOR_FIELDS: tuple[str, ...] = ("fleet_selector",)


@dataclass(frozen=True)
class EngineKubernetes:
    """Resource names and status fields used to manage an engine on Kubernetes.

    Replace ``NAME`` in targets and selectors with the run's object name. An empty
    ``pods_selector`` means verification needs no pod list.
    ``fleet_selector`` selects all engine pods for resource accounting.
    ``pod_role_label`` identifies their roles in the Python resource reader.

    Use ``error_jsonpath`` with ``lifecycle_jsonpath`` to distinguish a rejected
    resource from a reconciliation the operator will retry. Compare lifecycle
    and application state against ``failed_states``. The paths may be identical.

    Staging checks ``max_object_name_length`` before creating resources. ``None``
    means the operator declares no additional limit.
    """

    kind: str
    running_state: str
    failed_states: tuple[str, ...]
    state_jsonpath: str
    error_jsonpath: str
    lifecycle_jsonpath: str
    rest_service_suffix: str
    rest_port: int
    log_target: str
    provenance_selector: str
    pods_selector: str
    fleet_selector: str
    pod_role_label: str
    document_file: str
    configmap_file: str
    max_object_name_length: int | None = None

    def texts(self) -> dict[str, str]:
        """Serialize descriptor fields for shell readers, with comma-separated states."""
        return {
            "kind": self.kind,
            "running_state": self.running_state,
            "failed_states": ",".join(self.failed_states),
            "state_jsonpath": self.state_jsonpath,
            "error_jsonpath": self.error_jsonpath,
            "lifecycle_jsonpath": self.lifecycle_jsonpath,
            "rest_service_suffix": self.rest_service_suffix,
            "rest_port": str(self.rest_port),
            "log_target": self.log_target,
            "provenance_selector": self.provenance_selector,
            "pods_selector": self.pods_selector,
            "fleet_selector": self.fleet_selector,
            "document_file": self.document_file,
            "configmap_file": self.configmap_file,
        }


# Exclude Python-only fields explicitly so descriptor checks still
# catch newly added fields that lack a shell representation.
_UNPRINTED = frozenset({"max_object_name_length", "pod_role_label"})

# The field names a driver reads, in declaration order, which is the order
# `engine-k8s` prints them in.
FIELDS: tuple[str, ...] = tuple(field.name for field in fields(EngineKubernetes) if field.name not in _UNPRINTED)


def for_name(text: str, name: str) -> str:
    """``text`` with `NAME` replaced by a run's object name."""
    return text.replace(NAME, name)


def object_name(run_id: str) -> str:
    """Lowercase the run ID for a Kubernetes resource name.

    Keep the original ID for topics, tables, and run directories. Shell drivers
    apply the same conversion in ``k8s_object_name``.
    """
    return run_id.lower()
