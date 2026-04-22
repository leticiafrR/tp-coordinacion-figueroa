import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.client_id])

    def serialize_eof_message(self, _message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        # si es que no es mi mensaje de eof entonces no es mi confirmación
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            [fruit_top, client_id] = fields
            if client_id == self.client_id:
                return fruit_top
            return None
        return fields
