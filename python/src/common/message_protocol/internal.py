import json


class ControlMsgType:
    """Control message type constants, following the pattern of external.MsgType."""
    OK = 1
    COMMIT = 2
    TRYING_READY = 3


CONTROL_MSG_TYPE_KEY = "_control_message_type"


CONTROL_MESSAGE_PARSERS = {
    ControlMsgType.COMMIT: lambda message: {
        "transaction_id": message[1],
        "master_routing_key": message[2],
    }
    if len(message) == 3
    else None,
    ControlMsgType.TRYING_READY: lambda message: {
        "transaction_id": message[1],
        "amount_fruits_processed": int(message[2]),
    }
    if len(message) == 3
    else None,
    ControlMsgType.OK: lambda message: {
        "transaction_id": message[1],
    }
    if len(message) == 2
    else None,
}


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def make_control_message(message_type, **fields):
    del fields
    return [message_type]


def get_control_message_type(message):
    return message[0]


def parse_control_message(message):
    message_type = get_control_message_type(message)
    parser = CONTROL_MESSAGE_PARSERS.get(message_type)
    if parser is None:
        return None
    return parser(message)


def make_commit(transaction_id, master_routing_key):
    return [ControlMsgType.COMMIT, transaction_id, master_routing_key]


def make_trying_ready(transaction_id, amount_fruits_processed):
    return [
        ControlMsgType.TRYING_READY,
        transaction_id,
        int(amount_fruits_processed),
    ]


def make_ok(transaction_id):
    return [ControlMsgType.OK, transaction_id]
