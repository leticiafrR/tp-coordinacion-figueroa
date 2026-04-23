import threading
class TransactionStatus:
    def __init__(self, expected_processed_data_count):
        self.expected_processed_data_count = int(expected_processed_data_count)
        self.current_processed_data_count = 0


class TransactionsMonitor:
    def __init__(self):
        self.transactions = {}
        self.mutex = threading.Lock()

    def begin_transaction(self, client_id, expected_data_count):
        with self.mutex:
            if client_id not in self.transactions:
                self.transactions[client_id] = TransactionStatus(expected_data_count)
            else:
                self.transactions[client_id].expected_processed_data_count = int(
                    expected_data_count
                )

    def add_processed_data_count(self, processed_data_count, transaction_id):
        with self.mutex:
            status = self.transactions.get(transaction_id)
            if status is None:
                return False
            status.current_processed_data_count += int(processed_data_count)
            return True

    def digestion_complete(self, transaction_id):
        with self.mutex:
            status = self.transactions.get(transaction_id)
            if status is None:
                return False
            return (
                status.current_processed_data_count
                == status.expected_processed_data_count
            )

    def delete_transaction(self, transaction_id):
        with self.mutex:
            self.transactions.pop(transaction_id, None)