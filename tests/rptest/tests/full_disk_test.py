# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
from time import sleep, time

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.node_metrics import NodeMetrics
from rptest.util import wait_until_result

from kafka import KafkaProducer

N=3

# XXX TODO: TDD: test disabled until feature finished
SHOULD_FAIL = True

class FullDiskTest(RedpandaTest):

    TOPIC = "fulldisktest-topic"
    CONF_FREE_BYTES_THRESH = "storage_space_alert_free_threshold_bytes"

    def __init__(self, test_context: TestContext):
        self.topics = [TopicSpec(name=self.TOPIC)]
        super().__init__(test_context)

    # TODO factor out lib for this, common with cluster_config_test.py
    def _wait_for_node_config_value(self, node: ClusterNode, key: str, value: int) -> None:
        _get = lambda k: self.admin.get_cluster_config(node=node)[k]
        def match(key: str, val: int) -> bool:
            v = _get(key)
            self.logger.info(f"config wait: want {type(value)} {value}, got {v}"
                             + f" {type(v)}")
            return v == value

        wait_until(lambda: match(key, value), timeout_sec=15)

    def _trigger_low_space(self):
        orig_conf = self.admin.get_cluster_config()
        orig_min_free_bytes = orig_conf[self.CONF_FREE_BYTES_THRESH]

        assert len(self.redpanda.nodes) == N
        victim = random.choice(self.redpanda.nodes)
        vfree = self.node_metrics.disk_free_bytes_node(victim)
        new_threshold = int(max(vfree / 2, 1024))
        updates = {self.CONF_FREE_BYTES_THRESH: new_threshold}
        self.logger.info("Trigger low space: free thresh " +
                         f"{orig_min_free_bytes} -> {new_threshold}")
        self.admin.patch_cluster_config(upsert=updates, remove=[])

        self.logger.info(f"Confirming new config value..")
        self._wait_for_node_config_value(victim, self.CONF_FREE_BYTES_THRESH,
                                         new_threshold)


    def _setup(self) -> None:
        self.logger.debug(f"brokers: {self.redpanda.brokers_list()}")
        self.producer = KafkaProducer(
            bootstrap_servers=self.redpanda.brokers_list(),
            value_serializer=str.encode)
        self.node_metrics = NodeMetrics(self.redpanda)
        self.admin = Admin(self.redpanda)

        # XXX TODO wait_for() something instead?
        # To avoid "NotLeaderForPartitionError"
        sleep(5)


    @cluster(num_nodes=N)
    def test_write_rejection(self):
        """ Verify that we block external writers when free disk space on any
        node is below the threshold. """

        t0 = time()
        self._setup()
        t1 = time()

        # 1. Confirm we have a working setup.
        future = self.producer.send(self.TOPIC, "Q: What do redpandas eat?")
        # TODO async version of wait_until() would be sweet.
        self.node_metrics.wait_until_populated()
        future.get(timeout=15)

        t2 = time()
        self._trigger_low_space()
        t3 = time()
        self.logger.info(f"test timing: {t1-t0}, {t2-t1}, {t3-t2}")
        was_blocked = False
        try :
            future = self.producer.send(
                self.TOPIC,
                "A: Don't know, but they can do a lot of bites per second.")
            future.get(timeout=15)
        except BaseException as e:
            self.logger.info(f"Write rejected as expected: {e}")
            was_blocked = True
        assert was_blocked == SHOULD_FAIL, \
            f'Failed to {" "if SHOULD_FAIL else "not"} reject write for full disk.'
