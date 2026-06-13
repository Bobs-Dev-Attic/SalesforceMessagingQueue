/**
 * Configurable HttpCalloutMock for exercising the queue in tests. Returns a fixed
 * status / body, and records the last request it saw for assertions.
 */
@IsTest
public class MessageQueueMock implements HttpCalloutMock {

    private final Integer statusCode;
    private final String body;
    public HttpRequest lastRequest { get; private set; }

    public MessageQueueMock(Integer statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }

    public HttpResponse respond(HttpRequest request) {
        this.lastRequest = request;
        HttpResponse response = new HttpResponse();
        response.setStatusCode(statusCode);
        response.setStatus(statusCode == 200 ? 'OK' : 'ERR');
        if (body != null) {
            response.setBody(body);
        }
        response.setHeader('Content-Type', 'application/json');
        return response;
    }
}
