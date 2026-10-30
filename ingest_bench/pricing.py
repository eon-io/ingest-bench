# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Price requested resources using each machine's hourly price and capacity."""

from typing import NotRequired, TypedDict


class MachinePrice(TypedDict):
    vcpu: float
    gib: float
    usd_per_hour: float


class Pricing(TypedDict):
    machine_types: NotRequired[dict[str, MachinePrice]]
    # Retain the explicit flat-rate format for existing sites and results.
    vcpu_hour_usd: NotRequired[float]
    gib_hour_usd: NotRequired[float]


def resource_cost(vcpu: float, gib: float, machine_type: str, pricing: Pricing) -> float:
    """Split a node's price equally between CPU and memory, then price the requested share."""
    if "machine_types" in pricing:
        try:
            node = pricing["machine_types"][machine_type]
        except KeyError as err:
            raise ValueError(f"site_pricing.machine_types: missing price for {machine_type!r}") from err
        return node["usd_per_hour"] * (vcpu / node["vcpu"] + gib / node["gib"]) / 2
    return vcpu * pricing["vcpu_hour_usd"] + gib * pricing["gib_hour_usd"]
