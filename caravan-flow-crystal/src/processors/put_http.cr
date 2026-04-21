require "http/client"
require "uri"
require "../processor"
require "../flowfile_v3"

# PutHTTP — POST the FlowFile's content to a downstream URL. Supports
# two payload formats:
#   raw  — the bytes as-is, FlowFile attributes as X-Flow-* headers
#   v3   — FlowFileV3 binary (attributes + content) as the body
#
# Retries with exponential backoff on 5xx / 429 / connection errors.
# Emits success on 2xx, failure with error.message on terminal fails.
class PutHTTP < Processor
  property endpoint : String = ""
  property format : String = "raw"
  property retries : Int32 = 3
  property retry_initial_delay_ms : Int32 = 200
  property retry_max_delay_ms : Int32 = 5000

  register "PutHTTP",
    description: "POST FlowFile to downstream HTTP endpoint",
    category: "Sink",
    params: {
      endpoint: {
        type: "String", required: true,
        placeholder: "http://localhost:8080/ingest",
      },
      format: {
        type: "Enum", default: "raw",
        choices: ["raw", "v3"],
        description: "raw = content bytes; v3 = FlowFile V3 framing (preserves attrs)",
      },
      retries: {
        type: "Int32", default: "3",
        description: "retry count on 5xx / 429 / connect errors",
      },
      retry_initial_delay_ms: {
        type: "Int32", default: "200",
        description: "initial backoff between retries, doubled each attempt",
      },
      retry_max_delay_ms: {
        type: "Int32", default: "5000",
        description: "cap on the exponential backoff delay",
      },
    }

  def process(ff : FlowFile) : Nil
    body, headers = build_request(ff)
    last_error = ""
    delay = @retry_initial_delay_ms

    (@retries + 1).times do |attempt|
      begin
        response = HTTP::Client.post(@endpoint, headers: headers, body: body)
        if response.status_code >= 200 && response.status_code < 300
          ff.attributes["put.http.status"] = response.status_code.to_s
          emit "success", ff
          return
        end
        last_error = "HTTP #{response.status_code}"
        # Retry 5xx / 429; fail fast on 4xx non-429.
        if response.status_code < 500 && response.status_code != 429
          break
        end
      rescue e : IO::Error | Socket::Error
        last_error = e.message || e.class.name
      rescue e
        last_error = e.message || e.class.name
        break
      end
      sleep delay.milliseconds
      delay = Math.min(delay * 2, @retry_max_delay_ms)
    end

    ff.attributes["error.message"] = last_error
    emit "failure", ff
  end

  private def build_request(ff : FlowFile) : {Bytes, HTTP::Headers}
    headers = HTTP::Headers.new
    case @format
    when "v3"
      body = FlowFileV3.pack(ff)
      headers["Content-Type"] = "application/flowfile-v3"
      {body, headers}
    else
      # raw: attrs go on X-Flow-* headers so they survive transport.
      ff.attributes.each { |k, v| headers["X-Flow-#{k}"] = v }
      headers["Content-Type"] = ff.attributes["mime.type"]? || "application/octet-stream"
      {ff.content, headers}
    end
  end
end
