from flask import make_response

def main():

    return make_response("야옹~ 야옹~", 200, {"content_type": "application/json"})