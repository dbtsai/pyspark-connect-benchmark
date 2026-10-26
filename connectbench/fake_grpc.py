"""Local fixture-only gRPC service; run using the official-client environment."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import signal
import uuid
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


def fields(data):
    """Read only the envelope; skip nested plans without decoding recursion."""
    def varint(pos):
        value, shift = 0, 0
        while True:
            b = data[pos]
            pos += 1
            value |= (b & 127) << shift
            if b < 128:
                return value, pos
            shift += 7
    pos = 0
    out = {}
    while pos < len(data):
        tag, pos = varint(pos)
        wire = tag & 7
        if wire == 2:
            length, pos = varint(pos)
            out[tag >> 3] = data[pos:pos + length]
            pos += length
        elif wire == 0:
            _, pos = varint(pos)
        elif wire in (1, 5):
            pos += 8 if wire == 1 else 4
        else:
            raise ValueError(f"unsupported envelope wire type {wire}")
    return out


def main():
    import grpc
    from pyspark.sql.connect import proto as p
    from pyspark.sql.connect.types import pyspark_types_to_proto_types
    from .backend import responses, ReplayStub
    from .fixtures import KINDS, spark_schema

    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=0)
    parser.add_argument('--width', type=int, default=1)
    parser.add_argument('--batch-rows', type=int, default=1000)
    parser.add_argument('--kind', choices=KINDS, default='long')
    parser.add_argument('--fixture-script', default='default')
    parser.add_argument('--local-cache-threshold', type=int, default=1 << 60)
    args = parser.parse_args()
    frames = responses('', args.rows, args.width, args.batch_rows, args.kind)
    schema = pyspark_types_to_proto_types(spark_schema(args.width, args.kind))
    server_id = str(uuid.uuid4())
    stats = {'execute_calls': 0, 'request_bytes': [], 'analyze_calls': 0, 'config_calls': 0,
             'schema_calls': 0, 'version_calls': 0, 'explain_calls': 0, 'ddl_calls': 0,
             'release_calls': 0, 'rpc_request_bytes': 0, 'arrow_batches': 0,
             'arrow_payload_bytes': 0, 'artifact_calls': 0, 'artifact_status_calls': 0,
             'artifact_files': 0, 'artifact_chunks': 0, 'artifact_payload_bytes': 0,
             'error_calls': 0, 'fetch_error_calls': 0, 'tree_calls': 0, 'persist_calls': 0}
    lock = threading.Lock()
    artifacts = set()

    def count(**increments):
        with lock:
            for key, value in increments.items():
                stats[key] += value

    def execute(raw, context):
        envelope = fields(raw)
        sid = envelope[1].decode()
        operation = envelope.get(6, str(uuid.uuid4()).encode()).decode()
        count(execute_calls=1, rpc_request_bytes=len(raw))
        with lock:
            stats['request_bytes'].append(len(raw))
        for i, frame in enumerate(frames):
            header = p.ExecutePlanResponse(session_id=sid, server_side_session_id=server_id,
                operation_id=operation, response_id=str(i))
            if i == 0:
                header.schema.CopyFrom(schema)
            count(arrow_batches=1, arrow_payload_bytes=len(frame))
            yield header.SerializeToString() + frame
        done = p.ExecutePlanResponse(session_id=sid, server_side_session_id=server_id,
            operation_id=operation, response_id='complete')
        done.result_complete.SetInParent()
        yield done.SerializeToString()

    def analyze(raw, context):
        count(analyze_calls=1, rpc_request_bytes=len(raw))
        envelope = fields(raw)
        response = p.AnalyzePlanResponse(session_id=envelope[1].decode(), server_side_session_id=server_id)
        operations = set(envelope) & set(range(4, 17))
        assert len(operations) == 1, 'Expected one scripted analysis operation'
        operation = operations.pop()
        if operation == 4:
            count(schema_calls=1)
            if args.fixture_script == 'missing-column':
                from google.protobuf.any_pb2 import Any
                from google.rpc import error_details_pb2, status_pb2
                from grpc_status import rpc_status
                message = '[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] missing_column cannot be resolved'
                info = error_details_pb2.ErrorInfo(reason='org.apache.spark.sql.AnalysisException',
                    domain='org.apache.spark', metadata={
                        'classes': '["org.apache.spark.sql.AnalysisException"]',
                        'errorClass': 'UNRESOLVED_COLUMN.WITHOUT_SUGGESTION',
                        'messageParameters': '{"objectName":"missing_column"}',
                        'errorId': 'fixture-missing-column'})
                packed = Any()
                packed.Pack(info)
                count(error_calls=1)
                context.abort_with_status(rpc_status.to_status(status_pb2.Status(
                    code=13, message=message, details=[packed])))
            response.schema.schema.CopyFrom(schema)
        elif operation == 5:
            count(explain_calls=1)
            response.explain.explain_string = 'Fixture plan (no query execution)'
        elif operation == 6:
            count(tree_calls=1)
            response.tree_string.tree_string = 'root\n |-- c0: long (nullable = true)'
        elif operation in (14, 15):
            count(persist_calls=1)
            getattr(response, 'persist' if operation == 14 else 'unpersist').SetInParent()
        elif operation == 10:
            count(version_calls=1)
            response.spark_version.version = '4.2.0'
        elif operation == 11:
            from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
            ddl = p.AnalyzePlanRequest.DDLParse.FromString(envelope[11]).ddl_string
            allowed = {
                'id BIGINT, value DOUBLE, text STRING': StructType([
                    StructField('id', LongType()), StructField('value', DoubleType()),
                    StructField('text', StringType())]),
                'id BIGINT': StructType([StructField('id', LongType())]),
                'value BIGINT': StructType([StructField('value', LongType())]),
            }
            assert ddl in allowed, f'Unscripted DDL: {ddl}'
            parsed = allowed[ddl]
            response.ddl_parse.parsed.CopyFrom(pyspark_types_to_proto_types(parsed))
            count(ddl_calls=1)
        else:
            raise AssertionError(f'Unscripted analysis operation: {operation}')
        return response.SerializeToString()

    def config(raw, context):
        count(config_calls=1, rpc_request_bytes=len(raw))
        req = p.ConfigRequest.FromString(raw)
        response = ReplayStub(req.session_id, config_values={
            'spark.sql.session.localRelationCacheThreshold': str(args.local_cache_threshold),
            'spark.sql.connect.serverStacktrace.enabled': 'false',
            'spark.sql.pyspark.jvmStacktrace.enabled': 'false'}).Config(req)
        response.server_side_session_id = server_id
        return response.SerializeToString()

    def release(raw, context):
        count(release_calls=1, rpc_request_bytes=len(raw))
        req = p.ReleaseExecuteRequest.FromString(raw)
        return p.ReleaseExecuteResponse(session_id=req.session_id,
            server_side_session_id=server_id, operation_id=req.operation_id).SerializeToString()

    def remember(sid, name):
        with lock:
            artifacts.add((sid, name))

    def add_artifacts(requests, context):
        from .artifact_fixture import receive
        count(artifact_calls=1)
        try:
            parsed = (p.AddArtifactsRequest.FromString(raw) for raw in requests)
            return receive(parsed, p, server_id, count, remember).SerializeToString()
        except AssertionError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

    def artifact_status(raw, context):
        req = p.ArtifactStatusesRequest.FromString(raw)
        count(artifact_status_calls=1, rpc_request_bytes=len(raw))
        response = p.ArtifactStatusesResponse(session_id=req.session_id,
                                             server_side_session_id=server_id)
        with lock:
            for name in req.names:
                response.statuses[name].exists = (req.session_id, name) in artifacts
        return response.SerializeToString()

    def fetch_error(raw, context):
        assert args.fixture_script == 'missing-column', 'Unscripted error details'
        count(fetch_error_calls=1, rpc_request_bytes=len(raw))
        req = p.FetchErrorDetailsRequest.FromString(raw)
        assert req.error_id == 'fixture-missing-column'
        response = p.FetchErrorDetailsResponse(session_id=req.session_id,
            server_side_session_id=server_id, root_error_idx=0)
        error = response.errors.add(error_type_hierarchy=['org.apache.spark.sql.AnalysisException'],
            message='[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] missing_column cannot be resolved')
        error.spark_throwable.error_class = 'UNRESOLVED_COLUMN.WITHOUT_SUGGESTION'
        error.spark_throwable.message_parameters['objectName'] = 'missing_column'
        return response.SerializeToString()

    server = grpc.server(ThreadPoolExecutor(max_workers=4), options=[
        ('grpc.max_receive_message_length', 256 * 1024 * 1024),
        ('grpc.max_send_message_length', 256 * 1024 * 1024)])
    server.add_generic_rpc_handlers([grpc.method_handlers_generic_handler(
        'spark.connect.SparkConnectService', {
            'ExecutePlan': grpc.unary_stream_rpc_method_handler(execute),
            'AnalyzePlan': grpc.unary_unary_rpc_method_handler(analyze),
            'Config': grpc.unary_unary_rpc_method_handler(config),
            'ReleaseExecute': grpc.unary_unary_rpc_method_handler(release),
            'AddArtifacts': grpc.stream_unary_rpc_method_handler(add_artifacts),
            'ArtifactStatus': grpc.unary_unary_rpc_method_handler(artifact_status),
            'FetchErrorDetails': grpc.unary_unary_rpc_method_handler(fetch_error),
        })])

    def snapshot():
        with lock:
            return json.dumps({k: v for k, v in stats.items() if k != 'request_bytes'}).encode()

    class StatsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/stats':
                self.send_error(404)
                return
            body = snapshot()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    stats_http = HTTPServer(('127.0.0.1', 0), StatsHandler)
    threading.Thread(target=stats_http.serve_forever, daemon=True).start()
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    print(json.dumps({'port': port, 'fixture_bytes': sum(map(len, frames)),
                     'stats_endpoint': f'http://127.0.0.1:{stats_http.server_port}/stats'}), flush=True)
    def shutdown(*_):
        print(json.dumps(stats), flush=True)
        server.stop(0)
    signal.signal(signal.SIGTERM, shutdown)
    server.wait_for_termination()


if __name__ == '__main__':
    main()
