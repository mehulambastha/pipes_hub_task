"""
Test Suite
Covers: OrderRequest, TradingSession, OrderQueue, OrderThrottler, OrderTracker, PersistentStorage
"""

import unittest
import threading
import time
import tempfile
import os
import sqlite3
from datetime import time as dt_time
from unittest.mock import patch


from order_management import (
    OrderRequest, OrderResponse,  # <-- Add OrderResponse
    RequestType, ResponseType,
    TradingSession, OrderQueue,
    OrderThrottler, OrderTracker,
    PersistentStorage
)

class TestOrderRequest(unittest.TestCase):
    def test_valid_order(self):
        order = OrderRequest(1, 100.0, 10, 'B', 101)
        self.assertEqual(order.side, 'B')

    def test_invalid_qty(self):
        with self.assertRaises(ValueError):
            OrderRequest(1, 100.0, 0, 'B', 102)

    def test_invalid_price(self):
        with self.assertRaises(ValueError):
            OrderRequest(1, -10, 5, 'B', 103)

    def test_invalid_side(self):
        with self.assertRaises(ValueError):
            OrderRequest(1, 100.0, 5, 'X', 104)

class TestTradingSession(unittest.TestCase):
    def setUp(self):
        self.session = TradingSession(dt_time(9, 0), dt_time(17, 0))

    def test_in_trading_hours(self):
        with patch('order_management.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(10, 0)
            self.assertTrue(self.session.is_trading_hours())

    def test_outside_trading_hours(self):
        with patch('order_management.datetime') as mock_dt:
            mock_dt.now.return_value.time.return_value = dt_time(8, 0)
            self.assertFalse(self.session.is_trading_hours())

class TestOrderQueue(unittest.TestCase):
    def setUp(self):
        self.queue = OrderQueue()

    def test_fifo_ordering(self):
        o1 = OrderRequest(1, 100, 10, 'B', 201)
        time.sleep(0.001)
        o2 = OrderRequest(1, 101, 5, 'S', 202)
        self.queue.add_order(o1)
        self.queue.add_order(o2)
        self.assertEqual(self.queue.get_next_order().order_id, 201)
        self.assertEqual(self.queue.get_next_order().order_id, 202)

    def test_modify_and_cancel(self):
        o = OrderRequest(1, 100, 10, 'B', 300)
        self.queue.add_order(o)
        self.assertTrue(self.queue.modify_order(300, 105, 20))
        self.assertTrue(self.queue.cancel_order(300))

class TestOrderThrottler(unittest.TestCase):
    def test_throttling_limit_and_reset(self):
        throttler = OrderThrottler(2)
        self.assertTrue(throttler.can_send_now())
        self.assertTrue(throttler.can_send_now())
        self.assertFalse(throttler.can_send_now())
        time.sleep(1.1)
        self.assertTrue(throttler.can_send_now())

class TestOrderTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = OrderTracker()

    def test_latency_tracking(self):
        order_id = 1001
        send_time = 1000.0
        response_time = 1000.5

        self.tracker.track_sent_order(order_id, send_time)
        response = OrderResponse(order_id, ResponseType.ACCEPT, response_time)
        latency = self.tracker.process_response(response)

        self.assertAlmostEqual(latency, 0.5, places=3)
        self.assertNotIn(order_id, self.tracker.pending_orders)

    def test_thread_safety(self):
        def worker(start_id):
            for i in range(50):
                oid = start_id + i
                self.tracker.track_sent_order(oid, time.time())
                time.sleep(0.001)  # small delay to prevent race
                self.tracker.process_response(OrderResponse(oid, ResponseType.ACCEPT, time.time()))

        threads = [threading.Thread(target=worker, args=(i * 1000,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(self.tracker.pending_orders), 0)

class TestPersistentStorage(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        self.storage = PersistentStorage(self.temp_db.name)

    def tearDown(self):
        os.unlink(self.temp_db.name)

    def test_response_persistence(self):
        ts = time.time()
        self.storage.store_response(500, ResponseType.ACCEPT, 0.05, ts)
        with sqlite3.connect(self.temp_db.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT order_id FROM order_responses WHERE order_id = 500")
            self.assertEqual(cur.fetchone()[0], 500)

if __name__ == '__main__':
    unittest.main(verbosity=2)
