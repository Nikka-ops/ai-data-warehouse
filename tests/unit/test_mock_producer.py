# -*- coding: utf-8 -*-
"""MockProducer / BaseProducer / BrazilianEcommerceSimulator 单元测试"""

import pytest

try:
    from src.ingestion.producers.mock_producer import MockProducer, BrazilianEcommerceSimulator
    from src.common.models import OrderEvent
except ImportError:
    pytest.skip("ingestion producers not available", allow_module_level=True)


class TestMockProducer:
    def setup_method(self):
        self.producer = MockProducer()

    # ------------------------------------------------------------------
    # 1. produce() stores messages per topic
    # ------------------------------------------------------------------
    def test_produce_stores_message_in_topic(self):
        self.producer.produce(topic="orders_stream", key="k1", value={"price": 99.0})
        msgs = self.producer.get_messages("orders_stream")
        assert len(msgs) == 1
        assert msgs[0]["key"] == "k1"
        assert msgs[0]["value"] == {"price": 99.0}

    # ------------------------------------------------------------------
    # 2. get_messages() returns stored messages for a topic
    # ------------------------------------------------------------------
    def test_get_messages_returns_all_stored_messages(self):
        for i in range(5):
            self.producer.produce(topic="test_topic", key=f"k{i}", value={"i": i})
        msgs = self.producer.get_messages("test_topic")
        assert len(msgs) == 5
        keys = [m["key"] for m in msgs]
        assert keys == [f"k{i}" for i in range(5)]

    # ------------------------------------------------------------------
    # 3. get_messages() returns empty list for unknown topic
    # ------------------------------------------------------------------
    def test_get_messages_unknown_topic_returns_empty_list(self):
        result = self.producer.get_messages("nonexistent_topic")
        assert result == []

    # ------------------------------------------------------------------
    # 4. Multiple topics are stored independently
    # ------------------------------------------------------------------
    def test_multiple_topics_stored_independently(self):
        self.producer.produce(topic="topic_a", key="a1", value={"x": 1})
        self.producer.produce(topic="topic_b", key="b1", value={"x": 2})
        self.producer.produce(topic="topic_b", key="b2", value={"x": 3})

        msgs_a = self.producer.get_messages("topic_a")
        msgs_b = self.producer.get_messages("topic_b")

        assert len(msgs_a) == 1
        assert len(msgs_b) == 2
        assert msgs_a[0]["key"] == "a1"
        assert msgs_b[0]["key"] == "b1"
        assert msgs_b[1]["key"] == "b2"

    # ------------------------------------------------------------------
    # 5. maxlen is respected (overflow drops oldest messages)
    # ------------------------------------------------------------------
    def test_maxlen_drops_oldest_on_overflow(self):
        producer = MockProducer(maxlen=3)
        for i in range(5):
            producer.produce(topic="t", key=f"k{i}", value={"i": i})

        msgs = producer.get_messages("t")
        assert len(msgs) == 3
        # oldest two (k0, k1) should have been dropped
        keys = [m["key"] for m in msgs]
        assert keys == ["k2", "k3", "k4"]

    # ------------------------------------------------------------------
    # 6. clear(topic) clears only that topic
    # ------------------------------------------------------------------
    def test_clear_single_topic_leaves_others_intact(self):
        self.producer.produce(topic="alpha", key="a", value={})
        self.producer.produce(topic="beta",  key="b", value={})

        self.producer.clear("alpha")

        assert self.producer.get_messages("alpha") == []
        assert len(self.producer.get_messages("beta")) == 1

    # ------------------------------------------------------------------
    # 7. clear() (no args) clears all topics
    # ------------------------------------------------------------------
    def test_clear_no_args_clears_all_topics(self):
        self.producer.produce(topic="alpha", key="a", value={})
        self.producer.produce(topic="beta",  key="b", value={})

        self.producer.clear()

        assert self.producer.get_messages("alpha") == []
        assert self.producer.get_messages("beta")  == []

    # ------------------------------------------------------------------
    # 8. close() empties all queues
    # ------------------------------------------------------------------
    def test_close_empties_all_queues(self):
        self.producer.produce(topic="topic_x", key="x1", value={"data": "foo"})
        self.producer.produce(topic="topic_y", key="y1", value={"data": "bar"})

        self.producer.close()

        assert self.producer.get_messages("topic_x") == []
        assert self.producer.get_messages("topic_y") == []


class TestBaseProducerProduceOrder:
    def setup_method(self):
        self.producer = MockProducer()

    # ------------------------------------------------------------------
    # 9. produce_order(order_event) calls produce with correct args
    # ------------------------------------------------------------------
    def test_produce_order_routes_to_orders_stream(self):
        import uuid
        from datetime import datetime

        order = OrderEvent(
            order_id=str(uuid.uuid4()),
            customer_id="C12345",
            seller_id="S9999",
            product_id="P100001",
            category="beleza_saude",
            price=149.90,
            quantity=1,
            event_time=datetime(2024, 6, 1, 12, 0, 0),
            state="SP",
        )

        self.producer.produce_order(order)

        msgs = self.producer.get_messages("orders_stream")
        assert len(msgs) == 1

        stored = msgs[0]
        assert stored["key"] == order.order_id
        # value must be the Pydantic dict serialisation
        value = stored["value"]
        assert isinstance(value, dict)
        assert value["order_id"]    == order.order_id
        assert value["customer_id"] == "C12345"
        assert value["price"]       == 149.90
        assert value["state"]       == "SP"


class TestBrazilianEcommerceSimulator:
    # Known state codes from _STATES in mock_producer.py
    _VALID_STATES = {
        "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO",
        "DF", "PE", "CE", "AM", "ES", "MT", "MS", "PA",
    }

    # All category price bounds from _CATEGORIES
    _CATEGORY_PRICE_BOUNDS = {
        "beleza_saude":           (20,   300),
        "relogios_presentes":     (50,   800),
        "cama_mesa_banho":        (30,   500),
        "esporte_lazer":          (40,   400),
        "informatica_acessorios": (30,   600),
        "moveis_decoracao":       (80,  1500),
        "utilidades_domesticas":  (15,   200),
        "automotivo":             (30,   500),
        "brinquedos":             (20,   300),
        "telefonia":              (100, 3000),
        "eletronicos":            (80,  2000),
        "ferramentas_jardim":     (20,   400),
    }

    def setup_method(self):
        self.simulator = BrazilianEcommerceSimulator()

    # ------------------------------------------------------------------
    # 10. generate_order() returns an OrderEvent with valid state
    # ------------------------------------------------------------------
    def test_generate_order_returns_valid_state(self):
        order = self.simulator.generate_order()
        assert isinstance(order, OrderEvent)
        assert order.state in self._VALID_STATES, (
            f"Unexpected state '{order.state}'"
        )

    # ------------------------------------------------------------------
    # 11. generate_order() returns price within the category's price range
    # ------------------------------------------------------------------
    def test_generate_order_price_within_category_range(self):
        # Run several samples to probe randomness
        for _ in range(20):
            order = self.simulator.generate_order()
            lo, hi = self._CATEGORY_PRICE_BOUNDS[order.category]
            assert lo <= order.price <= hi, (
                f"price {order.price} out of range [{lo}, {hi}] "
                f"for category '{order.category}'"
            )

    # ------------------------------------------------------------------
    # 12. generate_order() generates unique order_ids (100 samples)
    # ------------------------------------------------------------------
    def test_generate_order_unique_order_ids(self):
        order_ids = [self.simulator.generate_order().order_id for _ in range(100)]
        assert len(set(order_ids)) == 100, "Duplicate order_id detected in 100 samples"

    # ------------------------------------------------------------------
    # 13. run(..., duration_seconds=0.1) completes and sends messages
    # ------------------------------------------------------------------
    def test_run_completes_and_sends_messages(self):
        producer = MockProducer()
        # Short but non-zero duration to guarantee at least one order
        self.simulator.run(producer, rate_per_second=50, duration_seconds=0.1)

        msgs = producer.get_messages("orders_stream")
        assert len(msgs) >= 1, "run() should have produced at least one order"

        # Spot-check the first message has the expected fields
        value = msgs[0]["value"]
        assert "order_id" in value
        assert "state"    in value
        assert "price"    in value
        assert value["state"] in self._VALID_STATES
