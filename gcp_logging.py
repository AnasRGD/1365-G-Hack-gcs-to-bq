import json


def __simple_log(msg: str, severity):
    data: dict = {"message": msg, "severity": severity}
    print(json.dumps(data))


def warning(msg: str):
    __simple_log(msg, "warning")


def info(msg: str):
    __simple_log(msg, "info")


def error(msg: str):
    __simple_log(msg, "error")
