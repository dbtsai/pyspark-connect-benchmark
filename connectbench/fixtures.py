"""Deterministic Arrow/Spark fixtures shared by the expanded benchmark and checks."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal


KINDS = ('long', 'string', 'double', 'decimal', 'timestamp', 'date', 'boolean',
         'binary', 'nullable', 'array', 'struct', 'map', 'nested', 'large-string', 'nullable50', 'nested3')


def value_at(index, kind):
    if kind == 'nullable50':
        return None if index % 2 == 0 else index
    if kind == 'nested3':
        return {'outer': {'inner': {'value': index, 'items': [index, None]}}}
    if kind in ('long', 'nullable'):
        return None if kind == 'nullable' and index % 5 == 0 else index
    if kind == 'string':
        return f'value-{index:08d}'
    if kind == 'large-string':
        return f'value-{index:08d}' + 'x' * 1010  # exactly 1,024 bytes
    if kind == 'double':
        return index * 0.5
    if kind == 'decimal':
        return Decimal(index % 10000).scaleb(-2)
    if kind == 'timestamp':
        return datetime(2020, 1, 1) + timedelta(microseconds=index * 1001)
    if kind == 'date':
        return date(2020, 1, 1) + timedelta(days=index % 365)
    if kind == 'boolean':
        return index % 2 == 0
    if kind == 'binary':
        return index.to_bytes(8, 'little')
    if kind == 'array':
        return [index, None, index + 1]
    if kind == 'struct':
        return {'number': index, 'text': f'v{index}'}
    if kind == 'map':
        return {'a': index, 'b': index + 1}
    if kind == 'nested':
        return {'items': [index, None, index + 1], 'labels': {'key': f'v{index}'}}
    raise ValueError(kind)


def arrow_type(kind):
    import pyarrow as pa
    return {
        'long': pa.int64(), 'nullable': pa.int64(), 'nullable50': pa.int64(),
        'nested3': pa.struct([('outer', pa.struct([('inner', pa.struct([
            ('value', pa.int64()), ('items', pa.list_(pa.int64()))]))]))]), 'string': pa.string(),
        'large-string': pa.string(), 'double': pa.float64(),
        'decimal': pa.decimal128(18, 2), 'timestamp': pa.timestamp('us', tz='UTC'),
        'date': pa.date32(), 'boolean': pa.bool_(), 'binary': pa.binary(),
        'array': pa.list_(pa.int64()),
        'struct': pa.struct([('number', pa.int64()), ('text', pa.string())]),
        'map': pa.map_(pa.string(), pa.int64()),
        'nested': pa.struct([('items', pa.list_(pa.int64())),
                             ('labels', pa.map_(pa.string(), pa.string()))]),
    }[kind]


def spark_type(kind):
    from pyspark.sql import types as t
    return {
        'long': t.LongType(), 'nullable': t.LongType(), 'nullable50': t.LongType(),
        'nested3': t.StructType([t.StructField('outer', t.StructType([
            t.StructField('inner', t.StructType([t.StructField('value', t.LongType()),
                t.StructField('items', t.ArrayType(t.LongType()))]))]))]), 'string': t.StringType(),
        'large-string': t.StringType(), 'double': t.DoubleType(),
        'decimal': t.DecimalType(18, 2), 'timestamp': t.TimestampType(),
        'date': t.DateType(), 'boolean': t.BooleanType(), 'binary': t.BinaryType(),
        'array': t.ArrayType(t.LongType()),
        'struct': t.StructType([t.StructField('number', t.LongType()),
                                t.StructField('text', t.StringType())]),
        'map': t.MapType(t.StringType(), t.LongType()),
        'nested': t.StructType([
            t.StructField('items', t.ArrayType(t.LongType())),
            t.StructField('labels', t.MapType(t.StringType(), t.StringType()))]),
    }[kind]


def spark_schema(width, kind):
    from pyspark.sql.types import StructType, StructField
    return StructType([StructField(f'c{i}', spark_type(kind)) for i in range(width)])


def record_batch(start, stop, width, kind):
    import pyarrow as pa
    array = pa.array([value_at(i, kind) for i in range(start, stop)], type=arrow_type(kind))
    return pa.record_batch([array] * width, names=[f'c{i}' for i in range(width)])


def canonical(value, kind):
    """Normalize documented Arrow/Row representations for endpoint validation."""
    if kind == 'timestamp' and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if kind == 'nested3':
        if hasattr(value, 'asDict'):
            value = value.asDict(recursive=True)
        inner = value['outer']['inner']
        return {'outer': {'inner': {'value': inner['value'],
                                   'items': canonical(inner['items'], 'array')}}}
    if kind in ('struct', 'nested'):
        if hasattr(value, 'asDict'):
            value = value.asDict(recursive=True)
        value = dict(value)
        if kind == 'nested':
            value['items'] = canonical(value['items'], 'array')
            value['labels'] = dict(value['labels'])
        return value
    if kind == 'map':
        return dict(value)
    if kind == 'array':
        # pandas uses NaN in float-coerced nullable integer arrays.
        return [None if v is not None and v != v else v for v in value]
    if kind == 'binary':
        return bytes(value)
    return value


def weight(value):
    """Small deterministic all-field checksum; recursively forces nested conversion."""
    if value is None:
        return 17
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal)):
        return int(value * 100)
    if isinstance(value, datetime):
        delta = value - datetime(2020, 1, 1)
        return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    if isinstance(value, date):
        return (value - date(2020, 1, 1)).days
    if isinstance(value, (str, bytes, bytearray)):
        return len(value)
    if hasattr(value, 'asDict'):
        value = value.asDict(recursive=True)
    if isinstance(value, dict):
        return sum(weight(k) + weight(v) for k, v in value.items())
    return sum(weight(v) for v in value)
