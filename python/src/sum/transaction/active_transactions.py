import threading
class MastersRoutingKeyByTransactionId:
    def __init__(self):
        self.routing_key_by_transaction_id = {}
        self.mutex = threading.Lock()

    def register_transaction_master_routing_key(self, transaction_id, routing_key):
        with self.mutex:
            self.routing_key_by_transaction_id[transaction_id] = routing_key

    def look_master_routing_key(self, transaction_id):
        with self.mutex:
            return self.routing_key_by_transaction_id.get(transaction_id)

    def delete_transaction_info(self, transaction_id):
        with self.mutex:
            self.routing_key_by_transaction_id.pop(transaction_id, None)
