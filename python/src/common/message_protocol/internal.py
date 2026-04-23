import json


CONTROL_MSG_TYPE_KEY = "_control_message_type"
CONTROL_MSG_TYPE_COMMIT = "Commit"
CONTROL_MSG_TYPE_TRYING_READY = "TryingReady"
CONTROL_MSG_TYPE_OK = "Ok"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def make_control_message(message_type, **fields):
    payload = {CONTROL_MSG_TYPE_KEY: message_type}
    payload.update(fields)
    return payload


def get_control_message_type(message):
    if not isinstance(message, dict):
        return None
    return message.get(CONTROL_MSG_TYPE_KEY)


def make_commit(transaction_id, master_routing_key):
    return make_control_message(
        CONTROL_MSG_TYPE_COMMIT,
        transaction_id=transaction_id,
        master_routing_key=master_routing_key,
    )


def make_trying_ready(transaction_id, amount_fruits_processed):
    return make_control_message(
        CONTROL_MSG_TYPE_TRYING_READY,
        transaction_id=transaction_id,
        amount_fruits_processed=int(amount_fruits_processed),
    )


def make_ok(transaction_id):
    return make_control_message(
        CONTROL_MSG_TYPE_OK,
        transaction_id=transaction_id,
    )
