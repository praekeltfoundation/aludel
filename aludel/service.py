"""Decorators and helpers for building RESTful services.

This is used by applying the :func:`service` decorator to a class and then
applying the :func:`handler` decorator to handler methods on that class.

TODO: Document this better.
"""

from functools import wraps, update_wrapper
import json

from klein import Klein

from twisted.internet.defer import maybeDeferred
from twisted.python import log


__all__ = [
    'APIError', 'BadRequestParams', 'handler', 'service',
    'set_request_id', 'get_request_id', 'get_params', 'get_json_params',
    'get_url_params', 'format_response', 'format_error',
]


class APIError(Exception):
    code = 500
    message = None  # Replace BaseException's deprecated message attr.

    def __init__(self, message, code=None):
        super(APIError, self).__init__(message)
        if code is not None:
            self.code = code
        self.message = message


class BadRequestParams(APIError):
    code = 400


def handler(*args, **kw):
    """Decorator for HTTP request handlers.

    This decorator takes the same parameters as Klein's ``route()`` decorator.

    When used on a method of a class decorated with :func:`service`, the method
    is turned into a Klein request handler and response formatting is added.

    TODO: Document this better.
    """
    def deco(func):
        func._handler_args = (args, kw)
        return func
    return deco


def service(service_class):
    """Decorator for RESTful API classes.

    This decorator adds a bunch of magic to a class with
    :func:`handler`-decorated methods on it so it can be used as a Klein HTTP
    resource.

    TODO: Document this better.
    """
    service_class.app = Klein()
    for attr in dir(service_class):
        meth = getattr(service_class, attr)
        if hasattr(meth, '_handler_args'):
            handler = _make_handler(service_class, meth)
            setattr(service_class, attr, handler)
    return service_class


def _make_handler(service_class, handler_method):
    args, kw = handler_method._handler_args

    @wraps(handler_method)
    def wrapper(*args, **kw):
        return _handler_wrapper(handler_method, *args, **kw)
    update_wrapper(wrapper, handler_method)
    route = service_class.app.route(*args, **kw)
    return route(wrapper)


def _handler_wrapper(func, self, request, *args, **kw):
    d = maybeDeferred(func, self, request, *args, **kw)
    d.addCallback(format_response, request)
    if hasattr(self, 'handle_api_error'):
        d.addErrback(self.handle_api_error, request)
    d.addErrback(_handle_api_error, request)
    return d


def _handle_api_error(failure, request):
    error = failure.value
    if not failure.check(APIError):
        log.err(failure)
        error = APIError('Internal server error.')
    return format_error(error, request)


def set_request_id(request, request_id):
    # We name-mangle the attr because `request` isn't our object.
    request.__request_id = request_id


def get_request_id(request):
    try:
        return request.__request_id
    except AttributeError:
        return None


def get_params(params, mandatory, optional=()):
    keys = set(params.keys())
    missing = set(mandatory) - keys
    extra = keys - (set(mandatory) | set(optional))
    if missing:
        raise BadRequestParams("Missing request parameters: '%s'" % (
            "', '".join(sorted(missing))))
    if extra:
        raise BadRequestParams("Unexpected request parameters: '%s'" % (
            "', '".join(sorted(extra))))
    return params


def get_json_params(request, mandatory, optional=()):
    return get_params(json.loads(request.content.read()), mandatory, optional)


def get_url_params(request, mandatory, optional=()):
    if 'request_id' in request.args:
        set_request_id(request, request.args['request_id'][0])
    params = get_params(request.args, mandatory, optional)
    return dict((k, v[0]) for k, v in params.iteritems())


def format_response(params, request):
    request.setHeader('Content-Type', 'application/json')
    params['request_id'] = get_request_id(request)
    return json.dumps(params)


def format_error(error, request):
    request.setHeader('Content-Type', 'application/json')
    request.setResponseCode(error.code)
    return json.dumps({
        'request_id': get_request_id(request),
        'error': error.message,
    })
