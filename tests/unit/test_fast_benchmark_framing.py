from scripts.qa.fast_benchmark import SSEParser


def test_crlf_split_does_not_terminate_multiline_event():
    parser = SSEParser()
    parser.feed(b'data: {"type":"response.output_text.delta",\r', 1.0)
    parser.feed(b'\ndata: "delta":"hello"}\r\n\r\n', 2.0)
    assert parser.response.first_text_timestamp == 2.0
