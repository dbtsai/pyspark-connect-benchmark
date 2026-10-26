import unittest
import zlib


class HarnessTest(unittest.TestCase):
    def test_fake_envelope_skips_nested_payload(self):
        from connectbench.fake_grpc import fields
        # Session id + opaque plan payload; nested bytes need not be decoded.
        self.assertEqual(fields(b'\x0a\x03abc\x1a\x03\xff\xff\xff'),
                         {1: b'abc', 3: b'\xff\xff\xff'})

    def test_unscripted_rpc_fails_closed(self):
        from connectbench.backend import ReplayStub
        with self.assertRaisesRegex(AssertionError, "Unscripted RPC"):
            ReplayStub("session").AnalyzePlan(None)

    def test_real_client_paths(self):
        try:
            import pyspark.sql.connect.session
        except ImportError:
            self.skipTest("Spark Connect dependencies are not installed")
        import argparse
        from connectbench.run import worker
        for stage in ("construct", "protobuf", "serialize", "outbound", "decode", "return-arrow"):
            with self.subTest(stage=stage):
                result = worker(argparse.Namespace(stage=stage, family="wide", size=3,
                    rows=17, width=2, batch_rows=5, kind="long"))
                self.assertGreater(result["elapsed_ns"], 0)


class UploadFixtureTest(unittest.TestCase):
    def receive(self, requests):
        from pyspark.sql.connect import proto
        from connectbench.artifact_fixture import receive
        self.counters, self.uploaded = {}, []
        def count(**values):
            for key, value in values.items():
                self.counters[key] = self.counters.get(key, 0) + value
        return receive(requests, proto, 'server', count,
                       lambda sid, name: self.uploaded.append((sid, name)))

    def chunk(self, data):
        from pyspark.sql.connect import proto
        return proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data))

    def test_batch_and_chunked_uploads_preserve_bytes_and_session(self):
        from pyspark.sql.connect import proto as p
        chunk = self.chunk
        requests = [
            p.AddArtifactsRequest(session_id='session', batch=p.AddArtifactsRequest.Batch(
                artifacts=[p.AddArtifactsRequest.SingleChunkArtifact(name='files/one', data=chunk(b'ab'))])),
            p.AddArtifactsRequest(session_id='session', begin_chunk=p.AddArtifactsRequest.BeginChunkedArtifact(
                name='files/two', total_bytes=5, num_chunks=2, initial_chunk=chunk(b'123'))),
            p.AddArtifactsRequest(session_id='session', chunk=chunk(b'45'))]
        response = self.receive(requests)
        self.assertEqual(response.session_id, 'session')
        self.assertEqual(self.uploaded, [('session', 'files/one'), ('session', 'files/two')])
        self.assertTrue(all(a.is_crc_successful for a in response.artifacts))
        self.assertEqual(self.counters['artifact_payload_bytes'], 7)
        self.assertEqual(self.counters['artifact_chunks'], 3)
        self.assertEqual(self.counters['rpc_request_bytes'], sum(r.ByteSize() for r in requests))

    def test_bad_crc_and_truncated_stream_are_rejected(self):
        from pyspark.sql.connect import proto as p
        bad = self.chunk(b'bad')
        bad.crc ^= 1
        with self.assertRaisesRegex(AssertionError, 'CRC mismatch'):
            self.receive([p.AddArtifactsRequest(session_id='s', batch=p.AddArtifactsRequest.Batch(
                artifacts=[p.AddArtifactsRequest.SingleChunkArtifact(name='x', data=bad)]))])
        begin = p.AddArtifactsRequest(session_id='s', begin_chunk=p.AddArtifactsRequest.BeginChunkedArtifact(
            name='x', total_bytes=6, num_chunks=2, initial_chunk=self.chunk(b'123')))
        with self.assertRaisesRegex(AssertionError, 'length/chunk count mismatch'):
            self.receive([begin])


class UnifiedCatalogTest(unittest.TestCase):
    def test_selection_is_disjoint_and_case_identifiers_are_unambiguous(self):
        from connectbench.catalog import cases
        all_cases = cases()
        self.assertEqual(len(all_cases), len({c['name'] for c in all_cases}))
        subsets = [cases(suites=(suite,)) for suite in ('core', 'api')]
        self.assertEqual(all_cases, [c for subset in subsets for c in subset])
        selected = cases(smoke=True, suites=('api',))
        self.assertTrue(selected)
        self.assertTrue(all(c['suite'] == 'api' for c in selected))
        self.assertTrue(any(c['group'] == 'artifact' for c in selected))

    def test_half_null_fixture_and_deep_nested_checksum(self):
        from connectbench.fixtures import value_at, canonical, weight
        values = [value_at(i, 'nullable50') for i in range(20)]
        self.assertEqual(sum(v is None for v in values), 10)
        from pyspark.sql import Row
        value = Row(outer=Row(inner=Row(value=3, items=[3, None])))
        self.assertEqual(canonical(value, 'nested3'), value_at(3, 'nested3'))
        self.assertEqual(weight(value), weight(value_at(3, 'nested3')))

if __name__ == "__main__":
    unittest.main()
