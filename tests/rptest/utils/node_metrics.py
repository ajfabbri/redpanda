from math import floor

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService


def all_greater_than_zero(l1: list[float]):
    return all([x > 0 for x in l1])


class NodeMetrics:
    """ Convenience class for grabbing per-node metrics, like disk space. """
    def __init__(self, redpanda: RedpandaService):
        self.redpanda = redpanda

    def _get_metrics_vals(self, name_substr: str, nodes=None) -> list[float]:
        mval = self.redpanda.metrics_sample(name_substr, nodes=nodes)
        assert mval
        return list(map(lambda s: floor(s.value), mval.samples))

    def disk_free_bytes_node(self, node: ClusterNode) -> float:
        vals = self._get_metrics_vals("storage_disk_free_bytes", nodes=[node])
        assert len(vals) == 1
        return vals[0]

    def disk_total_bytes(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_total_bytes")

    def disk_free_bytes(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_free_bytes")

    def disk_space_alert(self) -> list[float]:
        return self._get_metrics_vals("storage_disk_free_space_alert")

    def wait_until_populated(self, timeout_sec=15):
        """ Wait until we have metrics for all nodes. """
        # disk metrics are updated via health monitor's periodic tick().
        wait_until(lambda: all_greater_than_zero(self.disk_total_bytes()),
                   timeout_sec=15,
                   err_msg="Disk metrics not populated before timeout.")
