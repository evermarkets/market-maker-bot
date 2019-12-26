import hashlib
import hmac
import json
import base64


def body_to_string(body):
    return json.dumps(body, separators=(',', ':'))


class Authentication:
    def __init__(self, cfg):
        self.config = cfg
        self.api_key, self.api_secret = self.config.api_key, self.config.api_secret

    def generate_signature(self, timestamp, http_method, request_path, body):
        if body is None or body == '' or body == {}:
            body = ''
        else:
            body = body_to_string(body)

        message = str(timestamp) + http_method + request_path + body
        secret = base64.b64decode(self.api_secret)
        signature = hmac.new(secret, message.encode(), digestmod=hashlib.sha256).digest()
        return base64.encodebytes(signature)
