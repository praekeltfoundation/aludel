import json
from urllib import urlencode
from StringIO import StringIO

from klein import Klein

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web.client import Agent, FileBodyProducer, readBody
from twisted.web.http_headers import Headers
from twisted.web.server import Site

from aludel.service import Service, APIError, BadRequestParams


class FakeRequest(object):
    def __init__(self, content=None, args=None):
        self.code = 200
        self.headers = {}
        self.content = StringIO(content or '')
        self.args = args or {}

    def setResponseCode(self, code):
        self.code = code

    def setHeader(self, name, value):
        values = self.headers.setdefault(name.lower(), [])
        values.append(value)


class ApiClient(object):
    def __init__(self, base_url):
        self._base_url = base_url

    def _make_url(self, url_path):
        return '%s/%s' % (self._base_url, url_path.lstrip('/'))

    def _make_call(self, method, url_path, headers, body, expected_code):
        agent = Agent(reactor)
        url = self._make_url(url_path)
        d = agent.request(method, url, headers, body)
        return d.addCallback(self._get_response_body, expected_code)

    def _get_response_body(self, response, expected_code):
        assert response.code == expected_code
        return readBody(response).addCallback(json.loads)

    def get(self, url_path, params, expected_code=200):
        url_path = '?'.join([url_path, urlencode(params)])
        return self._make_call('GET', url_path, None, None, expected_code)

    def put(self, url_path, headers, content, expected_code=200):
        body = FileBodyProducer(StringIO(content))
        return self._make_call('PUT', url_path, headers, body, expected_code)


class TestService(TestCase):
    timeout = 5

    listener = None

    def tearDown(self):
        return self.stop_listening()

    def listen(self, service_instance):
        assert self.listener is None
        site = Site(service_instance.app.resource())
        self.listener = reactor.listenTCP(0, site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        return ApiClient('http://localhost:%s' % self.listener_port)

    def stop_listening(self):
        if self.listener is not None:
            self.listener, listener = None, self.listener
            return listener.loseConnection()

    def test_make_service(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:who>')
            def hello(slf, request, who):
                return {'hello': who}

        assert isinstance(FooService.app, Klein)

        req = FakeRequest()
        resp = self.successResultOf(FooService().hello(req, 'world'))
        assert json.loads(resp) == {
            'request_id': None,
            'hello': 'world',
        }

    def test_request_id(self):
        req = FakeRequest()
        assert Service.get_request_id(req) is None
        Service.set_request_id(req, 'foo')
        assert Service.get_request_id(req) == 'foo'

    def test_get_params_mandatory(self):
        params = {
            'foo': 'hello',
            'bar': 'world',
        }
        assert Service.get_params(params, ['foo', 'bar']) == {
            'foo': 'hello',
            'bar': 'world',
        }

    def test_get_params_missing(self):
        params = {
            'foo': 'hello',
            'bar': 'world',
        }
        err = self.assertRaises(BadRequestParams, Service.get_params,
                                params, ['foo', 'bar', 'baz'])
        assert err.message == (
            "Missing request parameters: 'baz'")

    def test_get_params_unexpected(self):
        params = {
            'foo': 'hello',
            'bar': 'world',
        }
        err = self.assertRaises(BadRequestParams, Service.get_params,
                                params, [])
        assert err.message == (
            "Unexpected request parameters: 'bar', 'foo'")

    def test_get_params_optional(self):
        params = {
            'foo': 'hello',
            'bar': 'world',
        }
        assert Service.get_params(params, ['foo'], ['bar']) == {
            'foo': 'hello',
            'bar': 'world',
        }
        assert Service.get_params(params, ['foo'], ['bar', 'baz']) == {
            'foo': 'hello',
            'bar': 'world',
        }

    def test_get_json_params(self):
        req = FakeRequest(content=json.dumps({'foo': 'hello', 'bar': 'world'}))
        assert Service.get_json_params(req, ['foo'], ['bar', 'baz']) == {
            'foo': 'hello',
            'bar': 'world',
        }

    def test_get_url_params_no_request_id(self):
        req = FakeRequest(args={'foo': ['hello', 'bye'], 'bar': ['world']})
        assert Service.get_url_params(req, ['foo'], ['bar', 'baz']) == {
            'foo': 'hello',
            'bar': 'world',
        }
        assert Service.get_request_id(req) is None

    def test_get_url_params_with_request_id(self):
        req = FakeRequest(args={
            'request_id': ['req0'],
            'foo': ['hello', 'bye'],
            'bar': ['world'],
        })
        assert Service.get_url_params(req, ['request_id', 'foo', 'bar']) == {
            'request_id': 'req0',
            'foo': 'hello',
            'bar': 'world',
        }
        assert Service.get_request_id(req) == 'req0'

    def test_format_response_no_request_id(self):
        req = FakeRequest()
        response = Service.format_response({'foo': 'bar'}, req)
        assert req.headers == {'content-type': ['application/json']}
        assert json.loads(response) == {
            'request_id': None,
            'foo': 'bar',
        }

    def test_format_response_with_request_id(self):
        req = FakeRequest()
        Service.set_request_id(req, 'req0')
        response = Service.format_response({'foo': 'bar'}, req)
        assert req.headers == {'content-type': ['application/json']}
        assert json.loads(response) == {
            'request_id': 'req0',
            'foo': 'bar',
        }

    def test_format_error_no_request_id(self):
        req = FakeRequest()
        response = Service.format_error(APIError('bad thing'), req)
        assert req.code == 500
        assert req.headers == {'content-type': ['application/json']}
        assert json.loads(response) == {
            'request_id': None,
            'error': 'bad thing',
        }

    def test_format_error_with_request_id(self):
        req = FakeRequest()
        Service.set_request_id(req, 'req0')
        response = Service.format_error(APIError('bad thing'), req)
        assert req.code == 500
        assert req.headers == {'content-type': ['application/json']}
        assert json.loads(response) == {
            'request_id': 'req0',
            'error': 'bad thing',
        }

    def test_format_error_with_status_code(self):
        req = FakeRequest()
        Service.set_request_id(req, 'req0')
        response = Service.format_error(APIError('teapot', 418), req)
        assert req.code == 418
        assert req.headers == {'content-type': ['application/json']}
        assert json.loads(response) == {
            'request_id': 'req0',
            'error': 'teapot',
        }

    @inlineCallbacks
    def test_simple_get_handler(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:who>')
            def hello(slf, request, who):
                return {'hello': who}

        client = yield self.listen(FooService())
        resp = yield client.get('hello/world', {})
        assert resp == {
            'request_id': None,
            'hello': 'world',
        }

    @inlineCallbacks
    def test_get_handler_with_params(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello')
            def hello(slf, request):
                params = Service.get_url_params(request, ['request_id', 'who'])
                return {'hello': params['who']}

        client = yield self.listen(FooService())
        resp = yield client.get('hello', {
            'request_id': 'req0',
            'who': 'world',
        })
        assert resp == {
            'request_id': 'req0',
            'hello': 'world',
        }

    @inlineCallbacks
    def test_get_handler_with_api_error(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:who>')
            def hello(slf, request, who):
                raise APIError('teapot', 418)

        client = yield self.listen(FooService())
        resp = yield client.get('hello/world', {}, expected_code=418)
        assert resp == {
            'request_id': None,
            'error': 'teapot',
        }

    @inlineCallbacks
    def test_get_handler_with_other_error(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:who>')
            def hello(slf, request, who):
                raise Exception('oops')

        client = yield self.listen(FooService())
        resp = yield client.get('hello/world', {}, expected_code=500)
        assert resp == {
            'request_id': None,
            'error': 'Internal server error.',
        }
        [failure] = self.flushLoggedErrors()
        assert 'oops' in str(failure.value)

    @inlineCallbacks
    def test_custom_error_handler(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:who>')
            def hello(slf, request, who):
                raise Exception('oops')

            def handle_api_error(slf, failure, request):
                raise APIError("Internal error: %r" % failure.value)

        client = yield self.listen(FooService())
        resp = yield client.get('hello/world', {}, expected_code=500)
        assert resp == {
            'request_id': None,
            'error': "Internal error: Exception('oops',)",
        }

    @inlineCallbacks
    def test_simple_put_handler(self):
        @Service.service
        class FooService(object):
            @Service.handler('/hello/<string:request_id>', methods=['PUT'])
            def hello(slf, request, request_id):
                Service.set_request_id(request, request_id)
                params = Service.get_json_params(request, ['who'])
                return {'hello': params['who']}

        client = yield self.listen(FooService())
        resp = yield client.put(
            'hello/req0', Headers({'Content-Type': ['application/json']}),
            '{"who": "world"}')
        assert resp == {
            'request_id': 'req0',
            'hello': 'world',
        }
