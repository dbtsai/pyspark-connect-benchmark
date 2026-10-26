"""Validate fixture content independently of client performance measurements."""
import unittest


class FixtureTest(unittest.TestCase):
    def test_arrow_frames_preserve_types_nulls_and_values_across_batches(self):
        import pyarrow as pa
        from pyspark.sql.connect import proto
        from connectbench.backend import responses
        from connectbench.fixtures import KINDS, arrow_type, canonical, value_at
        for kind in KINDS:
            with self.subTest(kind=kind):
                decoded = []
                frames = responses('test', 17, 3, 5, kind)
                self.assertEqual(len(frames), 4)
                for raw in frames:
                    response = proto.ExecutePlanResponse.FromString(raw)
                    table = pa.ipc.open_stream(response.arrow_batch.data).read_all()
                    self.assertEqual(table.num_rows, response.arrow_batch.row_count)
                    self.assertEqual(table.num_columns, 3)
                    self.assertEqual(table.column(0).type, arrow_type(kind))
                    decoded.extend(table.to_pylist())
                for i, row in enumerate(decoded):
                    for actual in row.values():
                        self.assertEqual(canonical(actual, kind), value_at(i, kind))

    def test_empty_fixture_still_contains_schema_and_one_batch(self):
        import pyarrow as pa
        from pyspark.sql.connect import proto
        from connectbench.backend import responses
        frame, = responses('test', 0, 3, 5, 'nested')
        table = pa.ipc.open_stream(proto.ExecutePlanResponse.FromString(frame).arrow_batch.data).read_all()
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.column_names, ['c0', 'c1', 'c2'])

    def test_nested_checksum_forces_fields_and_handles_nulls(self):
        from pyspark.sql import Row
        from connectbench.fixtures import weight
        expected = len('items') + len('labels') + 100 + 17 + 200 + len('key') + len('value')
        self.assertEqual(weight(Row(items=[1, None, 2], labels={'key': 'value'})), expected)

    def test_normalizes_nullable_pandas_arrays_and_aware_timestamps(self):
        from datetime import datetime, timezone
        from connectbench.fixtures import canonical
        self.assertEqual(canonical([0.0, float('nan'), 1.0], 'array'), [0, None, 1])
        self.assertEqual(canonical(datetime(2020, 1, 1, tzinfo=timezone.utc), 'timestamp'),
                         datetime(2020, 1, 1))


if __name__ == '__main__':
    unittest.main()
