#!/usr/bin/env python3
"""
Order Management System - 
@Mehul Ambastha
"""

import threading
import time
import queue
import logging
from datetime import datetime, time as dt_time
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Set
import json
import sqlite3
from contextlib import contextmanager
import signal
import sys


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_management.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RequestType(Enum):
    UNKNOWN = 0
    NEW = 1
    MODIFY = 2
    CANCEL = 3


class ResponseType(Enum):
    UNKNOWN = 0
    ACCEPT = 1
    REJECT = 2


@dataclass
class Logon:
    username: str
    password: str


@dataclass
class Logout:
    username: str


@dataclass
class OrderRequest:
    symbol_id: int
    price: float
    qty: int
    side: str  # 'B' or 'S'
    order_id: int
    request_type: RequestType = RequestType.NEW
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        if self.side not in ['B', 'S']:
            raise ValueError(f"Invalid side: {self.side}. Must be 'B' or 'S'")

        if self.request_type in [RequestType.NEW, RequestType.MODIFY]:
            if self.qty <= 0:
                raise ValueError(f"Invalid quantity: {self.qty}. Must be positive")
            if self.price <= 0:
                raise ValueError(f"Invalid price: {self.price}. Must be positive")


@dataclass
class OrderResponse:
    order_id: int
    response_type: ResponseType
    timestamp: float = field(default_factory=time.time)


class TradingSession:
    """Manages trading session state and time windows"""
    
    def __init__(self, start_time: dt_time, end_time: dt_time):
        self.start_time = start_time
        self.end_time = end_time
        self.is_logged_in = False
        self._lock = threading.Lock()
    
    def is_trading_hours(self) -> bool:
        """Check if current time is within trading hours"""
        current_time = datetime.now().time()
        
        # Handle overnight sessions (e.g., 22:00 to 06:00)
        if self.start_time <= self.end_time:
            return self.start_time <= current_time <= self.end_time
        else:
            return current_time >= self.start_time or current_time <= self.end_time
    
    def should_login(self) -> bool:
        """Check if we should send login message"""
        with self._lock:
            return self.is_trading_hours() and not self.is_logged_in
    
    def should_logout(self) -> bool:
        """Check if we should send logout message"""
        with self._lock:
            return not self.is_trading_hours() and self.is_logged_in
    
    def set_logged_in(self, status: bool):
        """Set login status thread-safely"""
        with self._lock:
            self.is_logged_in = status


class OrderThrottler:
    """Manages order rate limiting and queuing"""
    
    def __init__(self, max_orders_per_second: int):
        self.max_orders_per_second = max_orders_per_second
        self.current_second_count = 0
        self.current_second = int(time.time())
        self._lock = threading.Lock()
        self.order_queue = queue.Queue()
    
    def can_send_now(self) -> bool:
        """Check if we can send an order immediately"""
        with self._lock:
            current_sec = int(time.time())
            
            # Reset counter for new second
            if current_sec != self.current_second:
                self.current_second = current_sec
                self.current_second_count = 0
            
            if self.current_second_count < self.max_orders_per_second:
                self.current_second_count += 1
                return True
            
            return False
    
    def queue_order(self, order: OrderRequest):
        """Add order to throttling queue"""
        self.order_queue.put(order)
        logger.debug(f"Order {order.order_id} queued for throttling")
    
    def get_queued_order(self) -> Optional[OrderRequest]:
        """Get next order from queue if available"""
        try:
            return self.order_queue.get_nowait()
        except queue.Empty:
            return None


class OrderTracker:
    """Tracks sent orders and calculates round-trip latency"""
    
    def __init__(self):
        self.pending_orders: Dict[int, float] = {}  # order_id -> send_timestamp
        self._lock = threading.Lock()
    
    def track_sent_order(self, order_id: int, timestamp: float):
        """Track when an order was sent"""
        with self._lock:
            self.pending_orders[order_id] = timestamp
    
    def process_response(self, response: OrderResponse) -> Optional[float]:
        """Process response and return round-trip latency"""
        with self._lock:
            send_time = self.pending_orders.pop(response.order_id, None)
            if send_time:
                latency = response.timestamp - send_time
                logger.debug(f"Order {response.order_id} latency: {latency:.6f}s")
                return latency
            else:
                logger.warning(f"Received response for unknown order: {response.order_id}")
                return None


class PersistentStorage:
    """Handles persistent storage of order responses and metrics"""
    
    def __init__(self, db_path: str = "order_management.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS order_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    response_type TEXT NOT NULL,
                    latency_ms REAL,
                    timestamp REAL NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_order_id ON order_responses(order_id)
            """)
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with proper cleanup"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def store_response(self, order_id: int, response_type: ResponseType, 
                      latency: Optional[float], timestamp: float):
        """Store order response data"""
        with self._lock:
            with self._get_connection() as conn:
                latency_ms = latency * 1000 if latency else None
                conn.execute("""
                    INSERT INTO order_responses 
                    (order_id, response_type, latency_ms, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (order_id, response_type.name, latency_ms, timestamp))
        
        logger.info(f"Stored response for order {order_id}: {response_type.name}, "
                   f"latency: {latency_ms:.3f}ms" if latency_ms else "no latency")


class OrderQueue:
    """Manages queued orders with modification and cancellation support"""
    
    def __init__(self):
        self.orders: Dict[int, OrderRequest] = {}  # order_id -> OrderRequest
        self._lock = threading.Lock()
    
    def add_order(self, order: OrderRequest):
        """Add order to queue"""
        with self._lock:
            self.orders[order.order_id] = order
    
    def modify_order(self, order_id: int, price: float, qty: int) -> bool:
        """Modify existing order in queue"""
        with self._lock:
            if order_id in self.orders:
                self.orders[order_id].price = price
                self.orders[order_id].qty = qty
                logger.info(f"Modified queued order {order_id}: price={price}, qty={qty}")
                return True
            return False
    
    def cancel_order(self, order_id: int) -> bool:
        """Remove order from queue"""
        with self._lock:
            if order_id in self.orders:
                del self.orders[order_id]
                logger.info(f"Cancelled queued order {order_id}")
                return True
            return False
    
    def get_next_order(self) -> Optional[OrderRequest]:
        """Get next order from queue (FIFO)"""
        with self._lock:
            if self.orders:
                # Get oldest order (FIFO)
                order_id = min(self.orders.keys(), key=lambda x: self.orders[x].timestamp)
                return self.orders.pop(order_id)
            return None
    
    def size(self) -> int:
        """Get queue size"""
        with self._lock:
            return len(self.orders)


class OrderManagement:
    """Main Order Management System"""
    
    def __init__(self, 
                 start_time: dt_time,
                 end_time: dt_time, 
                 max_orders_per_second: int = 100,
                 username: str = "trading_system",
                 password: str = "secure_password"):
        
        # Core components
        self.trading_session = TradingSession(start_time, end_time)
        self.throttler = OrderThrottler(max_orders_per_second)
        self.order_tracker = OrderTracker()
        self.storage = PersistentStorage()
        self.order_queue = OrderQueue()
        
        # Configuration
        self.username = username
        self.password = password
        
        # Threading and synchronization
        self.shutdown_event = threading.Event()
        self.send_lock = threading.Lock()  # Protect non-thread-safe send methods
        
        # Start background threads
        self._start_background_threads()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Order Management System initialized")
    
    def _start_background_threads(self):
        """Start background processing threads"""
        # Session management thread
        self.session_thread = threading.Thread(
            target=self._session_manager, 
            name="SessionManager",
            daemon=True
        )
        self.session_thread.start()
        
        # Order processing thread
        self.order_processor_thread = threading.Thread(
            target=self._order_processor,
            name="OrderProcessor", 
            daemon=True
        )
        self.order_processor_thread.start()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown of the system"""
        logger.info("Shutting down Order Management System...")
        self.shutdown_event.set()
        
        # Send logout if logged in
        if self.trading_session.is_logged_in:
            try:
                with self.send_lock:
                    self.send_logout()
            except Exception as e:
                logger.error(f"Error during logout: {e}")
    
    def _session_manager(self):
        """Background thread to manage trading session state"""
        while not self.shutdown_event.is_set():
            try:
                if self.trading_session.should_login():
                    with self.send_lock:
                        self.send_logon()
                    self.trading_session.set_logged_in(True)
                    logger.info("Sent logon message - trading session active")
                
                elif self.trading_session.should_logout():
                    with self.send_lock:
                        self.send_logout()
                    self.trading_session.set_logged_in(False)
                    logger.info("Sent logout message - trading session ended")
                
                # Check every second
                self.shutdown_event.wait(1.0)
                
            except Exception as e:
                logger.error(f"Error in session manager: {e}")
                self.shutdown_event.wait(5.0)  # Wait before retry
    
    def _order_processor(self):
        """Background thread to process queued orders"""
        while not self.shutdown_event.is_set():
            try:
                # Only process if we're in trading hours
                if not self.trading_session.is_trading_hours():
                    self.shutdown_event.wait(1.0)
                    continue
                
                # Try to send queued orders
                if self.throttler.can_send_now():
                    order = self.order_queue.get_next_order()
                    if order:
                        self._send_order_internal(order)
                
                # Small delay to prevent busy waiting
                self.shutdown_event.wait(0.01)
                
            except Exception as e:
                logger.error(f"Error in order processor: {e}")
                self.shutdown_event.wait(1.0)
    
    def on_data(self, request: OrderRequest):
        """Process incoming order request from upstream system"""
        try:
            logger.debug(f"Received order request: {request.order_id}, "
                        f"type: {request.request_type.name}")
            
            # Reject orders outside trading hours
            if not self.trading_session.is_trading_hours():
                logger.warning(f"Rejecting order {request.order_id} - outside trading hours")
                return
            
            # Handle different request types
            if request.request_type == RequestType.NEW:
                self._handle_new_order(request)
            
            elif request.request_type == RequestType.MODIFY:
                self._handle_modify_order(request)
            
            elif request.request_type == RequestType.CANCEL:
                self._handle_cancel_order(request)
            
            else:
                logger.warning(f"Unknown request type for order {request.order_id}")
        
        except Exception as e:
            logger.error(f"Error processing order request {request.order_id}: {e}")
    
    def _handle_new_order(self, request: OrderRequest):
        """Handle new order request"""
        if self.throttler.can_send_now():
            # Can send immediately
            self._send_order_internal(request)
        else:
            # Need to queue
            self.order_queue.add_order(request)
            logger.debug(f"Queued new order {request.order_id} due to throttling")
    
    def _handle_modify_order(self, request: OrderRequest):
        """Handle modify order request"""
        # Try to modify queued order first
        if self.order_queue.modify_order(request.order_id, request.price, request.qty):
            return  # Successfully modified queued order
        
        # If not in queue, treat as new modify request
        if self.throttler.can_send_now():
            self._send_order_internal(request)
        else:
            self.order_queue.add_order(request)
            logger.debug(f"Queued modify order {request.order_id} due to throttling")
    
    def _handle_cancel_order(self, request: OrderRequest):
        """Handle cancel order request"""
        # Try to cancel queued order first
        if self.order_queue.cancel_order(request.order_id):
            return  # Successfully cancelled queued order
        
        # If not in queue, send cancel to exchange
        if self.throttler.can_send_now():
            self._send_order_internal(request)
        else:
            self.order_queue.add_order(request)
            logger.debug(f"Queued cancel order {request.order_id} due to throttling")
    
    def _send_order_internal(self, request: OrderRequest):
        """Internal method to send order to exchange"""
        try:
            send_timestamp = time.time()
            
            with self.send_lock:
                self.send(request)
            
            # Track the sent order
            self.order_tracker.track_sent_order(request.order_id, send_timestamp)
            
            logger.info(f"Sent order {request.order_id} to exchange "
                       f"({request.request_type.name})")
        
        except Exception as e:
            logger.error(f"Error sending order {request.order_id}: {e}")
    
    def on_data_response(self, response: OrderResponse):
        """Process incoming order response from exchange"""
        try:
            logger.debug(f"Received response for order {response.order_id}: "
                        f"{response.response_type.name}")
            
            # Calculate latency and store response
            latency = self.order_tracker.process_response(response)
            
            # Store in persistent storage
            self.storage.store_response(
                response.order_id, 
                response.response_type, 
                latency, 
                response.timestamp
            )
        
        except Exception as e:
            logger.error(f"Error processing response for order {response.order_id}: {e}")
    
    # Exchange interface methods (these would be implemented by the exchange library)
    def send(self, request: OrderRequest):
        """Send order to exchange - placeholder implementation"""
        # This method is provided by the exchange library
        # For testing purposes, we'll just log
        logger.info(f"EXCHANGE SEND: Order {request.order_id}, "
                   f"Symbol: {request.symbol_id}, Side: {request.side}, "
                   f"Price: {request.price}, Qty: {request.qty}")
    
    def send_logon(self):
        """Send logon message to exchange - placeholder implementation"""
        logger.info(f"EXCHANGE LOGON: {self.username}")
    
    def send_logout(self):
        """Send logout message to exchange - placeholder implementation"""
        logger.info(f"EXCHANGE LOGOUT: {self.username}")
    
    # Status and monitoring methods
    def get_status(self) -> Dict:
        """Get system status for monitoring"""
        return {
            "trading_session_active": self.trading_session.is_logged_in,
            "is_trading_hours": self.trading_session.is_trading_hours(),
            "queued_orders": self.order_queue.size(),
            "pending_responses": len(self.order_tracker.pending_orders),
            "orders_per_second_limit": self.throttler.max_orders_per_second,
            "current_second_count": self.throttler.current_second_count
        }


# Example usage and testing
if __name__ == "__main__":
    # Configuration - Trading hours 10:00 AM to 1:00 PM IST
    start_time = dt_time(10, 0)  # 10:00 AM
    end_time = dt_time(13, 0)    # 1:00 PM
    
    # Create order management system
    oms = OrderManagement(
        start_time=start_time,
        end_time=end_time,
        max_orders_per_second=100
    )
    
    try:
        # Example order processing
        print("Order Management System started. Press Ctrl+C to stop.")
        
        # Simulate some orders for testing
        test_orders = [
            OrderRequest(1, 100.50, 1000, 'B', 1001, RequestType.NEW),
            OrderRequest(1, 100.75, 500, 'S', 1002, RequestType.NEW),
            OrderRequest(1, 101.00, 1000, 'B', 1001, RequestType.MODIFY),
            OrderRequest(1, 0, 0, 'B', 1002, RequestType.CANCEL),
        ]
        
        # Process test orders with delay
        for i, order in enumerate(test_orders):
            time.sleep(2)  # Wait 2 seconds between orders
            print(f"\nSending test order {i+1}: {order.order_id} ({order.request_type.name})")
            oms.on_data(order)
            
            # Print system status
            status = oms.get_status()
            print(f"System Status: {json.dumps(status, indent=2)}")
        
        # Keep running
        while not oms.shutdown_event.is_set():
            time.sleep(10)
            status = oms.get_status()
            print(f"\nPeriodic Status: {json.dumps(status, indent=2)}")
    
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    finally:
        oms.shutdown()
        print("Order Management System stopped.")
