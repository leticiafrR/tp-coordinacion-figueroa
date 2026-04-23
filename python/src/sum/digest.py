import threading
class ClientDigest:
    def __init__(self):
        self.cant_data_processed = 0
        self.data_per_fruit = {}

    def digest(self, fruit, amount):
        self.cant_data_processed += 1
        current_amount = self.data_per_fruit.get(fruit, 0)
        self.data_per_fruit[fruit] = current_amount + int(amount)


class DigestPool:
    def __init__(self):
        self.pool = {}
        self.mutex = threading.Lock()

    def digest_client_data(self, client_id, fruit, amount):
        with self.mutex:
            client_digest = self.pool.get(client_id)
            if client_digest is None:
                client_digest = ClientDigest()
                self.pool[client_id] = client_digest
            client_digest.digest(fruit, amount)

    def get_current_client_digest(self, client_id):
        with self.mutex:
            client_digest = self.pool.get(client_id)
            if client_digest is None:
                return ClientDigest()
            snapshot = ClientDigest()
            snapshot.cant_data_processed = client_digest.cant_data_processed
            snapshot.data_per_fruit = dict(client_digest.data_per_fruit)
            return snapshot

    def delete_client_digest(self, client_id):
        with self.mutex:
            self.pool.pop(client_id, None)
