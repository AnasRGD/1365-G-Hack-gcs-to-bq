import json
from typing import List, Optional

from flask import Request, jsonify, make_response

from context import Context
from incoming_request import BodyRequest
from processor import process


def gcs_to_bq(request: Request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turnedq into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """
    ctx: Context = Context()
    ctx.load_environment()
    execution_id: str = request.headers.get("function-execution-id")
    data: str = request.get_data(cache=True, as_text=True, parse_form_data=False)
    request_json: dict = json.loads(data)
    body_request: BodyRequest = BodyRequest(request_json)

    if body_request.object_id is None and body_request.folder is None:
        return make_response(
            jsonify(
                {
                    "errors": "object_id or folder is required in the request body",
                    "status": "ERROR",
                }
            ),
            400,
        )

    result: (str, Exception, Optional[int], List[str]) = process(
        ctx, body_request, execution_id
    )

    json_response: dict = {
        "filenames": result[3],
        "bucket": body_request.bucket_id,
        "errors": str(result[1]),
        "status": result[0],
        "linesTreated": result[2],
    }

    if result[0] != "DONE":
        status_code: int = 500
    elif len(result[3]) == 0:
        status_code: int = 200
    else:
        status_code: int = 201

    response = make_response(jsonify(json_response), status_code)

    return response
