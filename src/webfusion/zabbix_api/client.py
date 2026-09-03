"""Dependency-free client for shared RF.Fusion Zabbix API operations.

The client limits configuration operations to RF.Fusion profiles and hosts.
It never logs the token or requests secret or vault-backed macro values.
"""

from __future__ import annotations

import json
import re
from collections import deque
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


API_METHOD_HOST_GET = "host.get"
API_METHOD_PROBLEM_GET = "problem.get"
API_METHOD_TEMPLATE_GET = "template.get"
API_METHOD_USER_MACRO_CREATE = "usermacro.create"
API_METHOD_USER_MACRO_DELETE = "usermacro.delete"
API_METHOD_USER_MACRO_GET = "usermacro.get"
API_METHOD_USER_MACRO_UPDATE = "usermacro.update"

MACRO_TYPE_TEXT = "0"
MACRO_TYPE_SECRET = "1"
MACRO_TYPE_VAULT = "2"

# This macro carries the Zabbix primary key used by appCataloga. It must not
# become a host-level override through the WebFusion configuration screen.
PROTECTED_MACRO_NAMES = frozenset(
    {
        "{$HOST_ID}",
        "{$HOST.ID}",
        "{HOST.ID}",
        "{$INTERFACE_ID}",
        "{$SNMP_COMMUNITY}",
        "{$DIGI_PORT}",
        "{$DIGI_TIMEOUT}",
        "{$ELSYS_PASS}",
        "{$ELSYS_PORT}",
        "{$ELSYS_USER}",
        "{$WUSER}"
    }
)

RF_FUSION_TEMPLATE_HOSTS = (
    "appCataloga",
    "RFEye_Nodes",
    "CW RMU",
    "ERMxAppColeta",
    "UMS300",
)

APPCATALOGA_CONTEXT_TAG = "Contex"
APPCATALOGA_CONTEXT_VALUE = "appCataloga"
TAG_OPERATOR_CONTAINS = 0

MACRO_NAME_PATTERN = re.compile(r"^\{\$[A-Z0-9_.]+\}$")


class ZabbixApiError(RuntimeError):
    """Represent an operational failure or invalid Zabbix API response.

    Use this exception to report safe failures to the presentation layer.
    """


class ZabbixApiClient:
    """Execute authorized Zabbix JSON-RPC queries and changes.

    Attributes:
        _api_url: Normalized JSON-RPC endpoint. Type: str.
        _api_token: Authentication token, used only in the request body. Type: str.
        _timeout_seconds: Maximum wait time for each request. Type: int.
        _request_id: Instance JSON-RPC sequence. Type: int. Starts at 0.
    """

    def __init__(self, api_url: str, api_token: str, *, timeout_seconds: int = 10):
        """Initialize the client with the authenticated API connection.

        Args:
            api_url: JSON-RPC endpoint URL. Type: str.
            api_token: Technical-account authentication token. Type: str.
            timeout_seconds: Maximum time per request, constrained from 3 to 30.
                Type: int.

        Returns:
            None: Initializes connection attributes and the request sequence.

        Raises:
            ZabbixApiError: If the URL or token is missing.
        """
        self._api_url = str(api_url or "").strip()
        self._api_token = str(api_token or "").strip()
        self._timeout_seconds = max(3, min(int(timeout_seconds), 30))
        self._request_id = 0

        if not self._api_url:
            raise ZabbixApiError("A URL da API do Zabbix não foi configurada.")
        if not self._api_token:
            raise ZabbixApiError("O token da API do Zabbix não foi configurado.")

    def list_catalog(self) -> dict[str, list[dict[str, Any]]]:
        """List managed RF.Fusion templates and their linked hosts.

        Args:
            None.

        Returns:
            dict[str, list[dict[str, Any]]]: Contains `templates` with `templateid`,
            `host`, and `name`, plus `hosts` with `hostid`, `host`, `name`, `status`, and
            `profiles`.

        Raises:
            ZabbixApiError: If the API cannot fulfill a query.
        """
        templates = self._call(
            API_METHOD_TEMPLATE_GET,
            {
                "output": ["templateid", "host", "name"],
                "filter": {"host": list(RF_FUSION_TEMPLATE_HOSTS)},
                "sortfield": "host",
                "sortorder": "ASC",
            },
        )
        templates_by_id = {
            str(template["templateid"]): template for template in templates
        }
        template_ids = set(templates_by_id)

        # Only hosts linked to managed RF.Fusion templates belong in this catalog.
        hosts = self._call(
            API_METHOD_HOST_GET,
            {
                "templateids": sorted(template_ids, key=int),
                "output": ["hostid", "host", "name", "status"],
                "selectParentTemplates": ["templateid", "host", "name"],
                "sortfield": "host",
                "sortorder": "ASC",
            },
        )
        managed_hosts = []
        for host in hosts:
            parent_templates = host.get("parentTemplates") or []
            matched_profiles = [
                templates_by_id[str(template.get("templateid"))]
                for template in parent_templates
                if str(template.get("templateid")) in template_ids
            ]
            if not matched_profiles:
                continue

            managed_hosts.append(
                {
                    "hostid": str(host["hostid"]),
                    "host": str(host.get("host") or ""),
                    "name": str(host.get("name") or host.get("host") or ""),
                    "status": str(host.get("status") or "0"),
                    "profiles": [
                        str(profile.get("name") or profile.get("host") or "")
                        for profile in matched_profiles
                    ],
                }
            )

        return {
            "templates": [
                {
                    "templateid": str(template["templateid"]),
                    "host": str(template.get("host") or ""),
                    "name": str(template.get("name") or template.get("host") or ""),
                }
                for template in templates
            ],
            "hosts": managed_hosts,
        }

    def list_appcataloga_problems(self) -> list[dict[str, Any]]:
        """List active problems whose context contains `appCataloga`.

        Args:
            None.

        Returns:
            list[dict[str, Any]]: Problems with `eventid`, `objectid`, `name`,
            `clock`, `severity`, `acknowledged`, `suppressed`, `tags` e `hosts`.

        Raises:
            ZabbixApiError: If the API rejects or does not respond to the query.
        """
        problems = self._call(
            API_METHOD_PROBLEM_GET,
            {
                "output": [
                    "eventid",
                    "objectid",
                    "name",
                    "clock",
                    "severity",
                    "acknowledged",
                    "suppressed",
                ],
                "selectHosts": ["hostid", "host", "name"],
                "selectTags": ["tag", "value"],
                "tags": [
                    {
                        "tag": APPCATALOGA_CONTEXT_TAG,
                        "value": APPCATALOGA_CONTEXT_VALUE,
                        "operator": TAG_OPERATOR_CONTAINS,
                    }
                ],
                "sortfield": ["eventid"],
                "sortorder": "DESC",
                "limit": 100,
            },
        )
        # Problem responses do not reliably embed hosts in this Zabbix version.
        return [
            {
                **problem,
                "hosts": self._get_hosts_for_trigger(problem.get("objectid")),
            }
            for problem in problems
        ]

    def _get_hosts_for_trigger(self, trigger_id: Any) -> list[dict[str, Any]]:
        """Query trigger hosts through the permitted `host.get` operation.

        Args:
            trigger_id: Trigger identifier returned by the problem. Type: Any.

        Returns:
            list[dict[str, Any]]: Hosts with `hostid`, `host`, and `name`; returns an
            empty list when the identifier is not positive.

        Raises:
            ZabbixApiError: If the API cannot query valid hosts.
        """
        normalized_trigger_id = str(trigger_id or "").strip()
        if not normalized_trigger_id.isdigit() or int(normalized_trigger_id) <= 0:
            return []
        # The technical account permits host.get but not trigger.get.
        return self._call(
            API_METHOD_HOST_GET,
            {
                "triggerids": [normalized_trigger_id],
                "output": ["hostid", "host", "name"],
                "sortfield": "host",
                "sortorder": "ASC",
            },
        )

    def get_host_configuration(self, host_id: str | int) -> dict[str, Any]:
        """Resolve a host's effective macros and the source of each value.

        Args:
            host_id: Zabbix host identifier. Type: str | int.

        Returns:
            dict[str, Any]: Configuration with `kind`, `target_id`, `title`,
            `technical_name`, `profiles`, `macros`, `direct_macros`, and
            `inherited_macro_groups`.

        Raises:
            ZabbixApiError: If the host, templates, or macros cannot be read.
        """
        host = self._get_host(host_id)
        template_layers, template_records = self._load_template_hierarchy(
            host.get("parentTemplates") or []
        )
        owner_records = {
            str(host["hostid"]): {
                "name": str(host.get("name") or host.get("host") or "Host"),
                "kind": "host",
            },
            **{
                template_id: {
                    "name": str(template.get("name") or template.get("host") or "Template"),
                    "kind": "template",
                }
                for template_id, template in template_records.items()
            },
        }
        macros_by_owner = self._get_macros_by_owner(owner_records)
        precedence = [(str(host["hostid"]), "host")]
        precedence.extend(
            (template_id, "template")
            for layer in template_layers
            for template_id in layer
        )
        effective_macros = self._resolve_effective_macros(
            precedence=precedence,
            macros_by_owner=macros_by_owner,
            owner_records=owner_records,
            target_owner_id=str(host["hostid"]),
        )
        direct_macros = [
            macro for macro in effective_macros if macro["is_direct_on_target"]
        ]
        inherited_macro_groups = self._group_inherited_host_macros(
            effective_macros=effective_macros,
            template_layers=template_layers,
            owner_records=owner_records,
        )

        return {
            "kind": "host",
            "target_id": str(host["hostid"]),
            "title": str(host.get("name") or host.get("host") or "Host"),
            "technical_name": str(host.get("host") or ""),
            "profiles": [
                str(template.get("name") or template.get("host") or "")
                for template in host.get("parentTemplates") or []
            ],
            "macros": effective_macros,
            "direct_macros": direct_macros,
            "inherited_macro_groups": inherited_macro_groups,
        }

    def get_host_configurations(
        self,
        host_ids: list[str | int],
    ) -> dict[str, dict[str, Any]]:
        """Resolve effective macros for multiple hosts with shared queries.

        Args:
            host_ids: Zabbix host identifiers. Type: list[str | int].

        Returns:
            dict[str, dict[str, Any]]: Configurations indexed by `hostid`; each has
            the same shape as `get_host_configuration`.

        Raises:
            ZabbixApiError: If the API cannot retrieve hosts, templates, or macros.
        """
        normalized_host_ids = sorted(
            {
                str(host_id)
                for host_id in host_ids
                if str(host_id).isdigit() and int(str(host_id)) > 0
            },
            key=int,
        )
        if not normalized_host_ids:
            return {}

        hosts = self._call(
            API_METHOD_HOST_GET,
            {
                "hostids": normalized_host_ids,
                "output": ["hostid", "host", "name", "status"],
                "selectParentTemplates": ["templateid", "host", "name"],
            },
        )
        if not hosts:
            return {}

        direct_templates = [
            template
            for host in hosts
            for template in host.get("parentTemplates") or []
        ]
        _, template_records = self._load_template_hierarchy(direct_templates)
        owner_records = {
            str(host["hostid"]): {
                "name": str(host.get("name") or host.get("host") or "Host"),
                "kind": "host",
            }
            for host in hosts
        }
        owner_records.update(
            {
                template_id: {
                    "name": str(
                        template.get("name") or template.get("host") or "Template"
                    ),
                    "kind": "template",
                }
                for template_id, template in template_records.items()
            }
        )
        macros_by_owner = self._get_macros_by_owner(owner_records)
        configurations: dict[str, dict[str, Any]] = {}

        for host in hosts:
            host_id = str(host["hostid"])
            template_layers = self._build_template_layers_from_records(
                direct_templates=host.get("parentTemplates") or [],
                template_records=template_records,
            )
            precedence = [(host_id, "host")]
            precedence.extend(
                (template_id, "template")
                for layer in template_layers
                for template_id in layer
            )
            effective_macros = self._resolve_effective_macros(
                precedence=precedence,
                macros_by_owner=macros_by_owner,
                owner_records=owner_records,
                target_owner_id=host_id,
            )

            configurations[host_id] = {
                "kind": "host",
                "target_id": host_id,
                "title": str(host.get("name") or host.get("host") or "Host"),
                "technical_name": str(host.get("host") or ""),
                "profiles": [
                    str(template.get("name") or template.get("host") or "")
                    for template in host.get("parentTemplates") or []
                ],
                "macros": effective_macros,
                "direct_macros": [
                    macro for macro in effective_macros if macro["is_direct_on_target"]
                ],
                "inherited_macro_groups": self._group_inherited_host_macros(
                    effective_macros=effective_macros,
                    template_layers=template_layers,
                    owner_records=owner_records,
                ),
            }

        return configurations

    def get_template_configuration(self, template_id: str | int) -> dict[str, Any]:
        """Resolve a template's direct and inherited macros.

        Args:
            template_id: Zabbix template identifier. Type: str | int.

        Returns:
            dict[str, Any]: Configuration with `kind`, `target_id`, `title`,
            `technical_name`, `profiles`, `macros`, and `direct_macros`.

        Raises:
            ZabbixApiError: If the template, hierarchy, or macros cannot be read.
        """
        template = self._get_template(template_id)
        template_layers, template_records = self._load_template_hierarchy([template])
        target_id = str(template["templateid"])
        owner_records = {
            item_id: {
                "name": str(item.get("name") or item.get("host") or "Template"),
                "kind": "template",
            }
            for item_id, item in template_records.items()
        }
        macros_by_owner = self._get_macros_by_owner(owner_records)
        precedence = [
            (template_id, "template")
            for layer in template_layers
            for template_id in layer
        ]
        effective_macros = self._resolve_effective_macros(
            precedence=precedence,
            macros_by_owner=macros_by_owner,
            owner_records=owner_records,
            target_owner_id=target_id,
        )

        return {
            "kind": "template",
            "target_id": target_id,
            "title": str(template.get("name") or template.get("host") or "Template"),
            "technical_name": str(template.get("host") or ""),
            "profiles": [
                str(item.get("name") or item.get("host") or "")
                for item in template.get("parentTemplates") or []
            ],
            "macros": effective_macros,
            "direct_macros": [
                macro
                for macro in effective_macros
                if macro["is_direct_on_target"]
            ],
        }

    def create_macro(
        self,
        *,
        owner_id: str,
        macro_name: str,
        macro_type: str,
        value: str,
    ) -> None:
        """Create a direct macro override on a host or template.

        Args:
            owner_id: Identifier of the owner host or template. Type: str.
            macro_name: Valid macro name. Type: str.
            macro_type: Allowed Zabbix macro type. Type: str.
            value: Value to save. Type: str.

        Returns:
            None: Sends the creation request to the API.

        Raises:
            ZabbixApiError: If the macro is invalid or the API rejects creation.
        """
        self._validate_writable_macro(macro_name, macro_type)
        self._call(
            API_METHOD_USER_MACRO_CREATE,
            {
                "hostid": str(owner_id),
                "macro": macro_name,
                "type": str(macro_type),
                "value": value,
            },
        )

    def update_macro(self, *, macro_id: str, value: str) -> None:
        """Update a direct macro value without changing its name or type.

        Args:
            macro_id: Zabbix direct macro identifier. Type: str.
            value: New macro value. Type: str.

        Returns:
            None: Sends the update request to the API.

        Raises:
            ZabbixApiError: If the API rejects the update.
        """
        self._call(
            API_METHOD_USER_MACRO_UPDATE,
            {"hostmacroid": str(macro_id), "value": value},
        )

    def delete_macro(self, *, macro_id: str) -> None:
        """Remove a direct macro to restore its configured inheritance.

        Args:
            macro_id: Zabbix direct macro identifier. Type: str.

        Returns:
            None: Sends the deletion request to the API.

        Raises:
            ZabbixApiError: If the API rejects deletion.
        """
        self._call(API_METHOD_USER_MACRO_DELETE, [str(macro_id)])

    def _get_host(self, host_id: str | int) -> dict[str, Any]:
        """Retrieve a Zabbix host with its directly linked templates.

        Args:
            host_id: Zabbix host identifier. Type: str | int.

        Returns:
            dict[str, Any]: Host with `hostid`, `host`, `name`, `status`, and
            `parentTemplates`.

        Raises:
            ZabbixApiError: If the host does not exist or the query fails.
        """
        hosts = self._call(
            API_METHOD_HOST_GET,
            {
                "hostids": [str(host_id)],
                "output": ["hostid", "host", "name", "status"],
                "selectParentTemplates": ["templateid", "host", "name"],
            },
        )
        if not hosts:
            raise ZabbixApiError("O host selecionado não foi encontrado no Zabbix.")
        return hosts[0]

    def _get_template(self, template_id: str | int) -> dict[str, Any]:
        """Retrieve a Zabbix template with its directly linked templates.

        Args:
            template_id: Zabbix template identifier. Type: str | int.

        Returns:
            dict[str, Any]: Template with `templateid`, `host`, `name`, and
            `parentTemplates`.

        Raises:
            ZabbixApiError: If the template does not exist or the query fails.
        """
        templates = self._call(
            API_METHOD_TEMPLATE_GET,
            {
                "templateids": [str(template_id)],
                "output": ["templateid", "host", "name"],
                "selectParentTemplates": ["templateid", "host", "name"],
            },
        )
        if not templates:
            raise ZabbixApiError("O template selecionado não foi encontrado no Zabbix.")
        return templates[0]

    def _load_template_hierarchy(
        self,
        direct_templates: list[dict[str, Any]],
    ) -> tuple[list[list[str]], dict[str, dict[str, Any]]]:
        """Load template ancestry one breadth-first layer at a time.

        Args:
            direct_templates: Templates linked directly to the target. Type:
            list[dict[str, Any]]. Each item must include `templateid`.

        Returns:
            tuple[list[list[str]], dict[str, dict[str, Any]]]: Ordered template-ID
            layers and template records indexed by ID.

        Raises:
            ZabbixApiError: If the API cannot retrieve a missing layer.
        """
        template_records = {
            str(item["templateid"]): item for item in direct_templates
        }
        layers: list[list[str]] = []
        pending = deque(
            sorted({str(item["templateid"]) for item in direct_templates}, key=int)
        )
        visited: set[str] = set()

        while pending:
            current_layer = []
            for _ in range(len(pending)):
                template_id = pending.popleft()
                if template_id in visited:
                    # Templates can converge through multiple inheritance paths.
                    continue
                visited.add(template_id)
                current_layer.append(template_id)

            if not current_layer:
                continue

            missing_ids = [
                template_id
                for template_id in current_layer
                if template_id not in template_records
                or "parentTemplates" not in template_records[template_id]
            ]
            if missing_ids:
                # Parent references omit their own parent hierarchy in API output.
                fetched_templates = self._call(
                    API_METHOD_TEMPLATE_GET,
                    {
                        "templateids": missing_ids,
                        "output": ["templateid", "host", "name"],
                        "selectParentTemplates": ["templateid", "host", "name"],
                    },
                )
                template_records.update(
                    {
                        str(item["templateid"]): item
                        for item in fetched_templates
                    }
                )

            layers.append(sorted(current_layer, key=int))
            for template_id in current_layer:
                for parent in template_records.get(template_id, {}).get("parentTemplates") or []:
                    parent_id = str(parent["templateid"])
                    if parent_id not in template_records:
                        template_records[parent_id] = parent
                    if parent_id not in visited:
                        pending.append(parent_id)

        return layers, template_records

    @staticmethod
    def _build_template_layers_from_records(
        *,
        direct_templates: list[dict[str, Any]],
        template_records: dict[str, dict[str, Any]],
    ) -> list[list[str]]:
        """Build template precedence from previously loaded records.

        Args:
            direct_templates: Templates linked directly to the host. Type:
            list[dict[str, Any]]. Each item must include `templateid`.
            template_records: Templates indexed by ID, including ancestors. Type:
            dict[str, dict[str, Any]].

        Returns:
            list[list[str]]: Template-ID layers in precedence order.
        """
        pending = deque(
            sorted(
                {str(template["templateid"]) for template in direct_templates},
                key=int,
            )
        )
        visited: set[str] = set()
        layers: list[list[str]] = []

        while pending:
            current_layer = []
            for _ in range(len(pending)):
                template_id = pending.popleft()
                if template_id in visited:
                    continue
                visited.add(template_id)
                current_layer.append(template_id)

            if not current_layer:
                continue

            ordered_layer = sorted(current_layer, key=int)
            layers.append(ordered_layer)
            for template_id in ordered_layer:
                for parent in (
                    template_records.get(template_id, {}).get("parentTemplates") or []
                ):
                    parent_id = str(parent["templateid"])
                    if parent_id not in visited:
                        pending.append(parent_id)

        return layers

    def _get_macros_by_owner(
        self,
        owner_records: dict[str, dict[str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch macro metadata and values only for text macros.

        Args:
            owner_records: Owners indexed by ID with `name` and `kind`. Type:
            dict[str, dict[str, str]].

        Returns:
            dict[str, list[dict[str, Any]]]: Macros by owner. Each macro contains
            `macro_id`, `owner_id`, `name`, `type`, `description`, and `value`.

        Raises:
            ZabbixApiError: If the API cannot retrieve metadata or values.
        """
        owner_ids = sorted(owner_records, key=int)
        if not owner_ids:
            return {}

        # Request secret metadata separately to avoid reading protected values.
        metadata_rows = self._call(
            API_METHOD_USER_MACRO_GET,
            {
                "hostids": owner_ids,
                "output": ["hostmacroid", "hostid", "macro", "type", "description"],
                "sortfield": "macro",
                "sortorder": "ASC",
            },
        )
        text_rows = self._call(
            API_METHOD_USER_MACRO_GET,
            {
                "hostids": owner_ids,
                "filter": {"type": MACRO_TYPE_TEXT},
                "output": ["hostmacroid", "value"],
            },
        )
        values_by_macro_id = {
            str(item["hostmacroid"]): str(item.get("value") or "")
            for item in text_rows
        }
        macros_by_owner: dict[str, list[dict[str, Any]]] = {
            owner_id: [] for owner_id in owner_ids
        }
        for macro in metadata_rows:
            normalized = {
                "macro_id": str(macro["hostmacroid"]),
                "owner_id": str(macro["hostid"]),
                "name": str(macro["macro"]),
                "type": str(macro.get("type") or MACRO_TYPE_TEXT),
                "description": str(macro.get("description") or ""),
                "value": values_by_macro_id.get(str(macro["hostmacroid"])),
            }
            macros_by_owner.setdefault(normalized["owner_id"], []).append(normalized)
        return macros_by_owner

    @staticmethod
    def _resolve_effective_macros(
        *,
        precedence: list[tuple[str, str]],
        macros_by_owner: dict[str, list[dict[str, Any]]],
        owner_records: dict[str, dict[str, str]],
        target_owner_id: str,
    ) -> list[dict[str, Any]]:
        """Resolve the first macro of each name by Zabbix precedence.

        Args:
            precedence: Owner-ID and type pairs in priority order. Type:
            list[tuple[str, str]].
            macros_by_owner: Normalized macros by owner. Type:
            dict[str, list[dict[str, Any]]].
            owner_records: Owner metadata by ID. Type: dict[str, dict[str, str]].
            target_owner_id: Requested host or template ID. Type: str.

        Returns:
            list[dict[str, Any]]: Effective macros ordered by name, including
            display values, source metadata, and editing indicators.
        """
        resolved: dict[str, dict[str, Any]] = {}
        for owner_id, owner_kind in precedence:
            owner = owner_records[owner_id]
            for macro in macros_by_owner.get(owner_id, []):
                if macro["name"] in resolved:
                    # The first owner is the effective value in Zabbix precedence.
                    continue
                macro_type = macro["type"]
                is_direct = owner_id == target_owner_id
                if macro_type == MACRO_TYPE_TEXT:
                    display_value = macro["value"] or ""
                    type_label = "Texto"
                    accepts_value = True
                elif macro_type == MACRO_TYPE_SECRET:
                    display_value = "Valor protegido"
                    type_label = "Segredo"
                    accepts_value = True
                else:
                    display_value = "Valor controlado pelo cofre"
                    type_label = "Cofre"
                    accepts_value = False

                if is_direct:
                    source_label = "Definida neste host" if owner_kind == "host" else "Definida neste template"
                elif owner_kind == "host":
                    source_label = f"Sobrescrita pelo host {owner['name']}"
                else:
                    source_label = f"Herdada de {owner['name']}"

                resolved[macro["name"]] = {
                    "name": macro["name"],
                    "type": macro_type,
                    "type_label": type_label,
                    "display_value": display_value,
                    "editable_value": macro["value"] if macro_type == MACRO_TYPE_TEXT else "",
                    "description": macro["description"],
                    "source_label": source_label,
                    "source_owner_id": owner_id,
                    "source_owner_name": owner["name"],
                    "source_owner_kind": owner_kind,
                    "source_macro_id": macro["macro_id"],
                    "is_direct_on_target": is_direct,
                    "accepts_value": accepts_value,
                    "is_protected": macro["name"] in PROTECTED_MACRO_NAMES,
                }
        return [resolved[name] for name in sorted(resolved)]

    @staticmethod
    def _group_inherited_host_macros(
        *,
        effective_macros: list[dict[str, Any]],
        template_layers: list[list[str]],
        owner_records: dict[str, dict[str, str]],
    ) -> list[dict[str, Any]]:
        """Group inherited macros by the template that provides each macro.

        Args:
            effective_macros: Effective macros for a host. Type:
            list[dict[str, Any]].
            template_layers: Template layers in precedence order. Type:
            list[list[str]].
            owner_records: Template metadata by ID. Type:
            dict[str, dict[str, str]].

        Returns:
            list[dict[str, Any]]: Groups with `template_id`, `template_name`,
            `is_direct_link`, and `macros`.
        """
        direct_template_ids = set(template_layers[0]) if template_layers else set()
        macros_by_template: dict[str, list[dict[str, Any]]] = {}
        for macro in effective_macros:
            if macro["is_direct_on_target"]:
                # Direct host values do not belong to inherited template groups.
                continue
            macros_by_template.setdefault(macro["source_owner_id"], []).append(macro)

        groups = []
        for template_id in [
            template_id for layer in template_layers for template_id in layer
        ]:
            macros = macros_by_template.get(template_id)
            if not macros:
                continue
            groups.append(
                {
                    "template_id": template_id,
                    "template_name": owner_records[template_id]["name"],
                    "is_direct_link": template_id in direct_template_ids,
                    "macros": macros,
                }
            )
        return groups

    @staticmethod
    def _validate_writable_macro(macro_name: str, macro_type: str) -> None:
        """Validate that a writable macro stays within the permitted scope.

        Args:
            macro_name: Macro name in Zabbix format. Type: str.
            macro_type: Zabbix macro type. Type: str.

        Returns:
            None: Completes when the macro is valid for modification.

        Raises:
            ZabbixApiError: If the macro name or type is not allowed.
        """
        if not MACRO_NAME_PATTERN.fullmatch(macro_name):
            raise ZabbixApiError("O nome da macro informado não é permitido.")
        if str(macro_type) not in {MACRO_TYPE_TEXT, MACRO_TYPE_SECRET}:
            raise ZabbixApiError("Somente macros de texto ou segredo podem ser alteradas.")

    def _call(self, method: str, params: Any) -> Any:
        """Send one JSON-RPC request without exposing authentication data.

        Args:
            method: Zabbix JSON-RPC method name. Type: str.
            params: Parameters accepted by the requested method. Type: Any.

        Returns:
            Any: The `result` field returned by the Zabbix API.

        Raises:
            ZabbixApiError: If an HTTP or connection failure, invalid JSON, API
            error, or response without `result` occurs.
        """
        self._request_id += 1
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "auth": self._api_token,
                "id": self._request_id,
            }
        ).encode("utf-8")
        # Keep the token inside the JSON-RPC body, never in the URL or logs.
        request = Request(
            self._api_url,
            data=payload,
            headers={"Content-Type": "application/json-rpc"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            raise ZabbixApiError(
                f"A API do Zabbix respondeu HTTP {error.code}."
            ) from error
        except URLError as error:
            raise ZabbixApiError("Não foi possível conectar à API do Zabbix.") from error
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise ZabbixApiError("A resposta da API do Zabbix é inválida.") from error

        if "error" in decoded:
            # Return API diagnostics without including authentication data.
            detail = decoded["error"].get("data") or decoded["error"].get("message")
            raise ZabbixApiError(f"A API do Zabbix recusou a operação: {detail}")
        if "result" not in decoded:
            raise ZabbixApiError("A API do Zabbix não retornou um resultado válido.")
        return decoded["result"]
