# Copyright (c) 2023 Canonical Ltd., Chi Wai Chan <chiwai.chan@canonical.com>

# This file is part of the sos project: https://github.com/sosreport/sos
#
# This copyrighted material is made available to anyone wishing to use,
# modify, copy, or redistribute it subject to the terms and conditions of
# version 2 of the GNU General Public License.
#
# See the LICENSE file in the source distribution for further information.

import json
import collections
import re
from typing import Optional
from sos.collector.clusters import Cluster


def _parse_option_string(strings: Optional[str] = None):
    """Parse commad separated string."""
    if not strings:
        return []
    return [string.strip() for string in strings.split(",")]


class juju(Cluster):
    """
    The juju cluster profile is intended to be used on juju managed clouds.
    It"s assumed that `juju` is installed on the machine where `sos` is called,
    and that the juju user has superuser privilege to the current controller.

    By default, the sos reports will be collected from all the applications in
    the current model. If necessary, you can filter the nodes by models /
    applications / units / machines with cluster options.

    Example:

    sos collect --cluster-type juju -c "juju.models=sos" -c "juju.apps=a,b,c"

    """

    cmd = "juju"
    cluster_name = "Juju Managed Clouds"
    option_list = [
        ("apps", "", "Filter node list by apps (comma separated regex)."),
        ("units", "", "Filter node list by units (comma separated string)."),
        ("models", "", "Filter node list by models (comma separated string)."),
        ("machines", "", "Filter node list by machines (comma separated string)."),
    ]

    def _cleanup_juju_output(self, output):
        """Remove leading characters before {."""
        return re.sub(r"(^[^{]*)(.*)", "\\2", output, 0, re.MULTILINE)

    def _get_model_info(self, model_name):
        juju_status = self._execute_juju_status(model_name)

        index = collections.defaultdict(dict)
        self._add_principals_to_index(index, juju_status, model_name)
        self._add_subordinates_to_index(index, juju_status, model_name)
        self._add_machines_to_index(index, juju_status, model_name)

        return index

    def _add_principals_to_index(self, index, juju_status, model_name):
        """Adds principal units to index."""
        for app, app_info in juju_status["applications"].items():
            nodes = []
            units = app_info.get("units", {})
            for unit, unit_info in units.items():
                machine = unit_info["machine"]
                node = f"{model_name}:{machine}"
                index["units"][unit] = [node]
                index["machines"][machine] = [node]
                nodes.append(node)

            index["apps"][app] = nodes

    def _add_subordinates_to_index(self, index, juju_status, model_name):
        """Add subordinates to index.

        Since subordinates does not have units they need to be manually added.
        """
        for app, app_info in juju_status["applications"].items():
            subordinate_to = app_info.get("subordinate-to", [])
            for parent in subordinate_to:
                index["apps"][app].extend(index["apps"][parent])
                units = juju_status["applications"][parent]["units"]
                for unit, unit_info in units.items():
                    node = f"{model_name}:{unit_info['machine']}"
                    for sub_key, sub_value in unit_info.get("subordinates", {}).items():
                        if sub_key.startswith(app + "/"):
                            index["units"][sub_key] = [node]
    def _add_machines_to_index(self, index, juju_status, model_name):
        """Add machines to index.

        If model does not have any applications it needs to be manually added.
        """
        for machine in juju_status["machines"].keys():
            node = f"{model_name}:{machine}"
            index["machines"][machine] = [node]



    def _execute_juju_status(self, model_name):
        model_option = f"-m {model_name}" if model_name else ""
        format_option = "--format json"
        status_cmd = f"{self.cmd} status {model_option} {format_option}"
        juju_status = None
        res = self.exec_primary_cmd(status_cmd)
        if res["status"] == 0:
            juju_status = json.loads(self._cleanup_juju_output((res["output"])))
        else:
            raise Exception(f"{status_cmd} did not return usable output")
        return juju_status

    def _filter_by_pattern(self, key, patterns, model_info):
        """Filter with regex match."""
        nodes = set()
        for pattern in patterns:
            for param, value in model_info[key].items():
                if re.match(pattern, param):
                    nodes.update(value or [])
        return nodes

    def _filter_by_fixed(self, key, patterns, model_info):
        """Filter with fixed match."""
        nodes = set()
        for pattern in patterns:
            for param, value in model_info[key].items():
                if pattern == param:
                    nodes.update(value or [])
        return nodes

    def set_transport_type(self):
        """Dynamically change transport to 'juju'."""
        return "juju"

    def get_nodes(self):
        """Get the machine numbers from `juju status`."""
        models = _parse_option_string(self.get_option("models"))
        apps = _parse_option_string(self.get_option("apps"))
        units = _parse_option_string(self.get_option("units"))
        machines = _parse_option_string(self.get_option("machines"))
        filters = {"apps": apps, "units": units, "machines": machines}

        if not models:
            models = [""]  # use current model by default

        nodes = set()

        for model in models:
            model_info = self._get_model_info(model)
            if not any(filters.values()):
                pass
            else:
                for key, resource in filters.items():
                    if key in ["apps"]:
                        _nodes = self._filter_by_pattern(key, resource, model_info)
                    else:
                        _nodes = self._filter_by_fixed(key, resource, model_info)
                    nodes.update(_nodes)

        return list(nodes)
