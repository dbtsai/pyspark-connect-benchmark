"""Public Python API workloads, plan shapes, protocol paths and result conversion.

Every interpreter executes this same code. Inputs and existing plans are prepared
before timing; unsupported APIs raise normally and remain visible in results.
"""


def cases(smoke=False, include_additional=False):
    matrix = []

    def add(name, group, iterations=1, **kwargs):
        matrix.append(dict(name=name, group=group, iterations=iterations, rows=0,
                           width=1, kind='long', batch_rows=1000, **kwargs))

    for api in ('withColumn', 'withColumns', 'select', 'filter', 'aggregate', 'join',
                'union', 'sort', 'window'):
        add(f'api-{api}', 'api', 200, api=api, depth=0)
    for depth in (16, 64, 256):
        for api in ('filter', 'withColumn', 'select', 'join'):
            add(f'increment-{api}-depth{depth}', 'api', 100, api=api, depth=depth)
    for depth in (4, 12, 32, 64):
        add(f'linear-expression-{depth}', 'expression', 50, depth=depth, mode='linear')
    for size in (1000, 10000):
        add(f'isin-{size}', 'expression', 10, depth=size, mode='isin')
    for depth in (16, 64, 256):
        for action in ('schema', 'explain', 'collect-empty'):
            add(f'prebuilt-{action}-depth{depth}', 'prebuilt', action=action, depth=depth)
    for action, count in (('schema-first', 1), ('schema-repeat', 50),
                          ('version-first', 1), ('version-repeat', 50),
                          ('config-get', 50), ('config-set', 50),
                          ('columns', 50), ('dtypes', 50),
                          ('column-access', 200), ('sql-loop', 20), ('df-loop', 20)):
        add(f'metadata-{action}', 'metadata', count, action=action)
    for source in ('tuples', 'rows', 'pandas', 'arrow'):
        for schema in ('explicit', 'inferred', 'ddl'):
            add(f'input-{source}-{schema}-10000x3', 'input', source=source,
                schema=schema, input_rows=10000, input_width=3)
    for source in ('tuples', 'pandas', 'arrow'):
        add(f'input-{source}-explicit-10000x32', 'input', source=source,
            schema='explicit', input_rows=10000, input_width=32)
    for payload in (0, 65536, 1048576):
        for mode in ('construct', 'outbound'):
            add(f'udf-{mode}-capture-{payload}', 'udf', 5, payload=payload, mode=mode)
    # Return cases use independent dimensions; fixture preparation is never timed.
    for kind in ('double', 'decimal', 'timestamp', 'date', 'boolean', 'binary',
                 'nullable', 'array', 'struct', 'map', 'nested', 'large-string'):
        for action in ('toArrow', 'collect-scan', 'toPandas'):
            matrix.append(dict(name=f'return-{kind}-{action}', group='return',
                iterations=1, rows=10000, width=3, kind=kind, batch_rows=1000, action=action))
    for rows, width, batch in ((100000, 8, 1000), (2097152, 1, 10000), (4096, 512, 512)):
        for action in ('toArrow', 'collect', 'iterator-consume'):
            matrix.append(dict(name=f'return-long-{rows}x{width}-{action}', group='return',
                iterations=1, rows=rows, width=width, kind='long', batch_rows=batch, action=action))
    for rows, width in ((100000, 8), (4096, 512)):
        for action in ('toArrow', 'collect'):
            matrix.append(dict(name=f'allocation-first-{rows}x{width}-{action}', group='return',
                iterations=1, rows=rows, width=width, kind='long', batch_rows=1000,
                action=action, warmups=0))
    if include_additional:
        matrix.extend(_additional_cases())
    if smoke:
        names = {'api-withColumn', 'increment-filter-depth256', 'linear-expression-64',
                 'prebuilt-schema-depth16', 'metadata-schema-repeat', 'metadata-version-repeat',
                 'input-pandas-explicit-10000x3', 'input-tuples-ddl-10000x3',
                 'udf-outbound-capture-65536', 'return-nested-collect-scan',
                 'return-long-100000x8-iterator-consume'}
        matrix = [c for c in matrix if c['name'] in names]
    return matrix


def prepare(spark, case):
    """Return (operation, untimed validator, number of warmups)."""
    from pyspark.sql import DataFrame, Row, Window, functions as f, types as t
    group = case['group']
    if group in ('construction', 'plan', 'query', 'input-extra', 'udf-extra',
                 'inspection', 'rpc', 'artifact', 'return-extra'):
        return _prepare_additional(spark, case)

    def check_df(result):
        assert isinstance(result, DataFrame), type(result)

    if group == 'api':
        df = spark.range(10)
        for i in range(case['depth']):
            df = df.filter(f.col('id') >= i)
        other = spark.range(5).withColumnRenamed('id', 'right_id')
        expr = f.col('id') + 1
        columns = {f'x{i}': expr + i for i in range(8)}
        projection = [f.col('id').alias(f'c{i}') for i in range(8)]
        predicate = f.col('id') > 1
        window = Window.orderBy('id')
        operations = {
            'withColumn': lambda: df.withColumn('x', expr),
            'withColumns': lambda: df.withColumns(columns),
            'select': lambda: df.select(*projection),
            'filter': lambda: df.filter(predicate),
            'aggregate': lambda: df.groupBy().agg(f.sum('id').alias('total')),
            'join': lambda: df.join(other, df.id == other.right_id, 'inner'),
            'union': lambda: df.union(df),
            'sort': lambda: df.orderBy(f.col('id').desc()),
            'window': lambda: df.select('id', f.row_number().over(window).alias('rn')),
        }
        return operations[case['api']], check_df, 3
    if group == 'expression':
        df = spark.range(10)
        literals = list(range(case['depth']))

        def operation():
            expr = f.col('id')
            if case['mode'] == 'isin':
                expr = expr.isin(literals)
            else:
                for i in range(case['depth']):
                    expr = (expr + i) * 2
            return df.select(expr.alias('value'))
        return operation, check_df, 3
    if group == 'prebuilt':
        df = spark.range(10)
        for i in range(case['depth']):
            df = df.filter(f.col('id') >= i)
        # Channel warmup on a different frame must not populate this frame's schema cache.
        spark.range(1).limit(0).collect()
        if case['action'] == 'schema':
            return lambda: df.schema, lambda x: check_schema(x, case), 0
        if case['action'] == 'explain':
            return lambda: df.explain(), lambda x: None, 1
        target = df.limit(0)
        return lambda: target.collect(), lambda x: assert_equal(len(x), 0), 1
    if group == 'metadata':
        df = spark.range(10)
        action = case['action']
        operations = {
            'schema-first': lambda: df.schema, 'schema-repeat': lambda: df.schema,
            'version-first': lambda: spark.version, 'version-repeat': lambda: spark.version,
            'config-get': lambda: spark.conf.get('spark.sql.session.timeZone'),
            'config-set': lambda: spark.conf.set('spark.sql.session.timeZone', 'UTC'),
            'columns': lambda: df.columns, 'dtypes': lambda: df.dtypes,
            'column-access': lambda: df['id'],
            'sql-loop': lambda: spark.sql('SELECT 1').collect(),
            'df-loop': lambda: spark.range(1).limit(0).collect(),
        }
        if action in ('schema-first', 'schema-repeat'):
            validator = lambda x: check_schema(x, case)
        elif action.startswith('version'):
            validator = lambda x: assert_equal(x, '4.2.0')
        elif action == 'config-get':
            validator = lambda x: assert_equal(x, 'UTC')
        elif action == 'columns':
            validator = lambda x: assert_equal(x, ['c0'])
        elif action == 'dtypes':
            validator = lambda x: assert_equal(x, [('c0', 'bigint')])
        elif action.endswith('loop'):
            validator = lambda x: assert_equal(len(x), 0)
        else:
            validator = lambda x: None
        return operations[action], validator, 0 if action.endswith('first') else 1
    if group == 'input':
        import pandas as pd
        import pyarrow as pa
        n, width = case['input_rows'], case['input_width']
        if width == 3:
            names = ['id', 'value', 'text']
            values = [(i, i * 0.5, f'v{i}') for i in range(n)]
            schema = t.StructType([t.StructField('id', t.LongType()),
                t.StructField('value', t.DoubleType()), t.StructField('text', t.StringType())])
        else:
            names = [f'c{i}' for i in range(width)]
            values = [tuple(i + j for j in range(width)) for i in range(n)]
            schema = t.StructType([t.StructField(name, t.LongType()) for name in names])
        source = case['source']
        if source == 'rows':
            data = [Row(**dict(zip(names, v))) for v in values]
        elif source in ('pandas', 'arrow'):
            data = pd.DataFrame(values, columns=names)
            if source == 'arrow':
                data = pa.Table.from_pandas(data, preserve_index=False)
        else:
            data = values
        selected = {'explicit': schema, 'inferred': None,
                    'ddl': 'id BIGINT, value DOUBLE, text STRING'}[case['schema']]
        return lambda: spark.createDataFrame(data, schema=selected), check_df, 1
    if group == 'udf':
        # Outbound forces serialization if a client defers it until plan encoding.
        payload = b'x' * case['payload']
        df = spark.range(10)

        def operation():
            fn = f.udf(lambda value: value + len(payload), t.LongType(), useArrow=False)
            result = df.select(fn('id').alias('value'))
            return result if case['mode'] == 'construct' else result.collect()
        validator = check_df if case['mode'] == 'construct' else lambda x: assert_equal(len(x), 0)
        return operation, validator, 1
    if group == 'return':
        from .fixtures import canonical, value_at, weight
        df = spark.range(case['rows'])
        action = case['action']

        def operation():
            if action == 'iterator-consume':
                count = total = 0
                for row in df.toLocalIterator():
                    count += 1
                    total += sum(weight(v) for v in row)
                return count, total
            if action == 'collect-scan':
                rows = df.collect()
                return rows, sum(weight(v) for row in rows for v in row)
            return list(df.toLocalIterator()) if action == 'toLocalIterator' else getattr(df, action)()

        def validate(result):
            if action == 'iterator-consume':
                count, checksum = result
            else:
                if action == 'collect-scan':
                    result, checksum = result
                if action == 'toArrow':
                    count = result.num_rows
                    assert_equal(result.num_columns, case['width'])
                    get = lambda i, j: result.column(j)[i].as_py()
                elif action == 'toPandas':
                    count = len(result)
                    assert_equal(len(result.columns), case['width'])
                    get = lambda i, j: result.iloc[i, j]
                else:
                    count = len(result)
                    get = lambda i, j: result[i][j]
                    assert_equal(len(result[0]), case['width'])
                for i in (0, case['rows'] - 1):
                    for j in range(case['width']):
                        actual = canonical(get(i, j), case['kind'])
                        expected = value_at(i, case['kind'])
                        # pandas represents a null long as NaN after float coercion.
                        if expected is None and action == 'toPandas':
                            import pandas as pd
                            assert pd.isna(actual), actual
                        else:
                            assert_equal(actual, expected)
            assert_equal(count, case['rows'])
            if action in ('collect-scan', 'iterator-consume'):
                expected_checksum = case['width'] * sum(weight(value_at(i, case['kind']))
                                                        for i in range(case['rows']))
                assert_equal(checksum, expected_checksum)
        if case.get('warmups') == 0:
            # Warm only the channel, leaving the first full-size result allocation timed.
            assert_equal(spark.version, '4.2.0')
        return operation, validate, case.get('warmups', 1)
    raise ValueError(group)


def assert_equal(actual, expected):
    assert actual == expected, (actual, expected)


def check_schema(schema, case):
    from .fixtures import spark_schema
    # Schema wrappers need not implement cross-instance Python __eq__ identically.
    assert_equal(schema.jsonValue(), spark_schema(case['width'], case['kind']).jsonValue())


def _additional_cases():
    matrix = []

    def add(name, group, iterations=1, **kw):
        matrix.append(dict(name=name, group=group, iterations=iterations,
                           rows=0, width=1, kind='long', batch_rows=1000) | kw)

    for depth in (16, 64, 256):
        add(f'chain-withColumn-{depth}', 'construction', 3, depth=depth, shape='chain')
        add(f'increment-withColumn-chain-{depth}', 'construction', 100,
            depth=depth, shape='increment')
    for width in (1, 16, 128, 512):
        add(f'projection-{width}', 'construction', 30, size=width, shape='wide')
        add(f'api-select-width{width}', 'construction', 100, size=width, shape='wide-call')
    add('aggregate-64-sums', 'construction', 20, shape='aggregate', size=64)
    add('linear-expression-1', 'construction', 100, shape='expression', size=1)
    add('isin-1', 'construction', 100, shape='isin', size=1)
    for shape, sizes in (('chain', (1, 16, 64, 256)), ('wide', (16, 128, 512)), ('expression', (4, 12, 32)),
                          ('filter', (64,)), ('aggregate', (64,)), ('join', (8,)),
                          ('window', (1,)), ('sql', (1,)), ('sql-parameters', (1,))):
        for size in sizes:
            for action in ('schema-first', 'schema-repeat', 'collect-empty'):
                add(f'plan-{shape}-{size}-{action}', 'plan',
                    20 if action == 'schema-repeat' else 1,
                    shape=shape, size=size, action=action)
    for count in (20, 50):
        for action in ('sql', 'dataframe'):
            add(f'query-{action}-{count}-one-value', 'query', count, action=action, rows=1)
    add('query-take-10', 'query', 5, action='take', rows=10)
    add('query-show-20', 'query', 5, action='show', rows=1, kind='string')
    add('query-shuffle-config-get', 'query', 50, action='config-get')
    add('query-shuffle-config-set', 'query', 50, action='config-set')
    # Prepared local inputs: scaling, representation, types, and deferred sending.
    for n in (100, 1000, 100000, 1000000):
        for source in ('tuples', 'pandas'):
            add(f'input-scale-{source}-{n}x3', 'input-extra', source=source,
                input_rows=n, input_width=3, input_kind='long', action='construct')
    for kind in ('string', 'timestamp', 'array', 'date', 'decimal'):
        for source in ('tuples', 'pandas'):
            add(f'input-type-{source}-{kind}-10000x3', 'input-extra', source=source,
                input_rows=10000, input_width=3, input_kind=kind, action='construct')
    for n in (30000, 60000):
        for action in ('construct', 'first-query', 'repeat-query'):
            add(f'input-cache1MiB-{n}x3-{action}', 'input-extra', source='pandas',
                input_rows=n, input_width=3, input_kind='long', action=action,
                local_cache_threshold=1048576, rows=1 if action != 'construct' else 0)
    for source in ('tuples', 'pandas'):
        for action in ('first-query', 'repeat-query'):
            add(f'input-inline-{source}-100000x3-{action}', 'input-extra', source=source,
                input_rows=100000, input_width=3, input_kind='long', action=action, rows=1)
    for style in ('scalar', 'scalar-arrow', 'pandas', 'udtf', 'mapInPandas', 'applyInPandas'):
        for action in ('construct', 'first-query', 'repeat-query'):
            add(f'udf-{style}-{action}', 'udf-extra', style=style, action=action, payload=0)
    for payload in (1048576, 10485760):
        for action in ('first-query', 'repeat-query'):
            add(f'udf-scalar-prepared-{payload}-{action}', 'udf-extra', style='scalar',
                action=action, payload=payload)
    add('udf-scalar-capture-10MiB-construct', 'udf-extra', style='scalar', action='construct', payload=10485760)
    for action in ('construct', 'first-query', 'repeat-query'):
        add(f'udf-register-sql-{action}', 'udf-extra', style='register', action=action, payload=0)
    for depth in (1, 16, 64, 256):
        for action in ('repr', 'dir', 'completion', 'positional', 'printSchema'):
            add(f'inspection-{action}-{depth}-first', 'inspection', depth=depth, action=action)
    for action in ('repr', 'dir', 'completion', 'printSchema'):
        add(f'inspection-{action}-256-repeat', 'inspection', 20, depth=256,
            action=action, warmups=1)
    for action in ('currentDatabase', 'tableExists', 'listTables', 'tempview', 'cache',
                   'persist', 'saveAsTable', 'missing-column'):
        add(f'rpc-{action}', 'rpc', 5, action=action,
            rows=1 if action in ('currentDatabase', 'tableExists') else 0,
            kind='string' if action == 'currentDatabase' else
                 'boolean' if action == 'tableExists' else 'long',
            fixture_script=action)
    for suffix, size, count in (('py', 10240, 1), ('zip', 10240, 1), ('bin', 10240, 1),
                                 ('bin', 1048576, 1), ('bin', 104857600, 1), ('bin', 10240, 10)):
        add(f'artifact-{suffix}-{size}-count{count}', 'artifact', size=size,
            suffix=suffix, count=count, warmups=0,
            expected_rpc={'artifact_payload_bytes': size * count, 'artifact_files': count})
    add('artifact-zip-archive-10240-count1', 'artifact', size=10240, suffix='zip',
        count=1, archive=True, warmups=0, expected_rpc={'artifact_payload_bytes': 10240, 'artifact_files': 1})
    for rows in (1, 1000, 4096, 16384, 262144, 1048576):
        add(f'return-sweep-{rows}x1-toArrow', 'return-extra', rows=rows, action='toArrow')
    for width in (1, 8, 64, 512):
        add(f'return-sweep-4096x{width}-collect-scan', 'return-extra', rows=4096,
            width=width, action='collect-scan')
    for kind in ('nullable50', 'nested3'):
        for action in ('toArrow', 'collect-scan', 'toPandas', 'iterator-consume'):
            add(f'return-{kind}-{action}', 'return-extra', rows=10000, width=3,
                kind=kind, action=action)
    # Numeric cell buffers: exact 64 KiB/1 MiB/16 MiB/128 MiB, plus IPC overhead.
    for nbytes in (65536, 1048576, 16777216, 134217728):
        add(f'return-buffer-{nbytes}-toArrow', 'return-extra', rows=nbytes // 64,
            width=8, action='toArrow', batch_rows=16384)
    return matrix


def _prepared_plan(spark, shape, size):
    from pyspark.sql import functions as f, Window
    df = spark.range(100)
    if shape in ('chain', 'increment'):
        for i in range(size):
            df = df.withColumn(f'x{i}', f.col('id') + i)
    elif shape == 'wide':
        df = df.select(*[(f.col('id') + i).alias(f'x{i}') for i in range(size)])
    elif shape == 'expression':
        expr = f.col('id')
        for i in range(size):
            expr = (expr * 3 + i) % 1000003
        df = df.select(expr.alias('x'))
    elif shape == 'isin':
        df = df.select(f.col('id').isin(list(range(size))))
    elif shape == 'filter':
        for i in range(size):
            df = df.filter(f.col('id') >= i)
    elif shape == 'aggregate':
        df = df.groupBy().agg(*[f.sum(f.col('id') + i).alias(f's{i}') for i in range(size)])
    elif shape == 'join':
        for i in range(size):
            right = spark.range(10).withColumnRenamed('id', f'right{i}')
            df = df.join(right, f.col('id') == f.col(f'right{i}'), 'left')
    elif shape == 'window':
        df = df.select('id', f.row_number().over(Window.orderBy('id')).alias('rn'))
    elif shape == 'sql':
        df = spark.sql('SELECT id + 1 AS x FROM range(100) WHERE id > 5')
    elif shape == 'sql-parameters':
        df = spark.sql('SELECT :value AS x', args={'value': 42})
    else:
        raise ValueError(shape)
    return df


def _prepare_additional(spark, case):
    from pyspark.sql import DataFrame, functions as f
    group, action = case['group'], case.get('action')

    def check_df(value):
        assert isinstance(value, DataFrame), type(value)

    def empty(value):
        assert_equal(len(value), 0)

    if group == 'construction':
        if case['shape'] == 'wide-call':
            df = spark.range(100)
            expressions = [(f.col('id') + i).alias(f'x{i}') for i in range(case['size'])]
            return lambda: df.select(*expressions), check_df, 3
        if case['shape'] == 'increment':
            df = _prepared_plan(spark, 'chain', case['depth'])
            expr = f.col('id') + 1
            return lambda: df.withColumn('next', expr), check_df, 3
        return lambda: _prepared_plan(spark, case['shape'], case.get('depth', case.get('size'))), check_df, 1
    if group == 'plan':
        df = _prepared_plan(spark, case['shape'], case['size'])
        spark.range(1).limit(0).collect()
        if action.startswith('schema'):
            return lambda: df.schema, lambda v: check_schema(v, case), int(action.endswith('repeat'))
        target = df.limit(0)
        return target.collect, empty, 1
    if group == 'query':
        df = spark.range(100)
        if action == 'show':
            return lambda: df.show(20), lambda v: assert_equal(v, None), 1
        if action.startswith('config'):
            op = (lambda: spark.conf.get('spark.sql.shuffle.partitions')) if action == 'config-get' else (
                lambda: spark.conf.set('spark.sql.shuffle.partitions', '200'))
            return op, (lambda v: assert_equal(v, '200')) if action == 'config-get' else lambda v: None, 1
        op = {'sql': lambda: spark.sql('SELECT 0').collect(),
              'dataframe': lambda: spark.range(1).select(f.lit(0)).collect(),
              'take': lambda: df.take(10)}[action]
        def validate(value):
            assert_equal(len(value), case['rows'])
            assert_equal([r[0] for r in value], list(range(case['rows'])))
        return op, validate, 1
    if group == 'input-extra':
        import pandas as pd
        from .fixtures import value_at, spark_schema
        n, width, kind = case['input_rows'], case['input_width'], case['input_kind']
        schema = spark_schema(width, kind)
        data = [tuple(value_at(i, kind) for _ in range(width)) for i in range(n)]
        if case['source'] == 'pandas':
            data = pd.DataFrame(data, columns=schema.fieldNames())
        create = lambda: spark.createDataFrame(data, schema=schema)
        if action == 'construct':
            return create, check_df, 0 if 'local_cache_threshold' in case else 1
        df = create()
        target = df.select('c0').limit(1)
        return target.collect, lambda v: assert_equal((len(v), v[0][0]), (1, 0)), int(action == 'repeat-query')
    if group == 'udf-extra':
        from pyspark.sql import types as t
        payload = b'x' * case['payload']
        df = spark.range(10)

        def make():
            style = case['style']
            if style in ('scalar', 'scalar-arrow', 'pandas', 'register'):
                fun = lambda v: v + len(payload)
                udf = (f.pandas_udf(fun, t.LongType()) if style == 'pandas' else
                       f.udf(fun, t.LongType(), useArrow=style == 'scalar-arrow'))
                if style == 'register':
                    spark.udf.register('bench_udf', udf)
                    return spark.sql('SELECT bench_udf(1)')
                return df.select(udf('id').alias('value'))
            if style == 'udtf':
                class Echo:
                    def eval(self, value):
                        yield (value + len(payload),)
                fn = f.udtf(Echo, returnType='value BIGINT', useArrow=False)
                return fn(f.lit(1))
            def batches(iterator):
                for batch in iterator:
                    yield batch
            if style == 'mapInPandas':
                return df.mapInPandas(batches, 'id BIGINT')
            return df.groupBy().applyInPandas(lambda pdf: pdf, 'id BIGINT')
        if action == 'construct':
            return make, check_df, 1
        target = make()
        return target.collect, empty, int(action == 'repeat-query')
    if group == 'inspection':
        df = _prepared_plan(spark, 'chain', case['depth'])
        op = {'repr': lambda: repr(df), 'dir': lambda: dir(df),
              'completion': lambda: df._ipython_key_completions_(),
              'positional': lambda: df[0], 'printSchema': lambda: df.printSchema()}[action]
        def validate(value):
            if action in ('repr', 'dir', 'completion'):
                assert 'c0' in value, f'Fixture column c0 missing from {action}: {str(value)[:300]}'
            elif action == 'positional':
                from pyspark.sql import Column
                assert isinstance(value, Column), type(value)
        return op, validate, case.get('warmups', 0)
    if group == 'rpc':
        df = spark.range(10)
        if action == 'missing-column':
            def operation():
                try:
                    spark.range(10).select('missing_column').schema
                except Exception as error:
                    return type(error).__name__, str(error)
                raise AssertionError('Expected scripted missing-column error')
            def validate(value):
                assert 'missing_column' in value[1], value
                assert 'UNRESOLVED_COLUMN' in value[1], value
            return operation, validate, 1
        op = {'currentDatabase': lambda: spark.catalog.currentDatabase(),
              'tableExists': lambda: spark.catalog.tableExists('bench_table'),
              'listTables': lambda: spark.catalog.listTables(),
              'tempview': lambda: df.createOrReplaceTempView('bench_view'),
              'cache': lambda: df.cache(), 'persist': lambda: df.persist(),
              'saveAsTable': lambda: df.write.mode('overwrite').saveAsTable('bench_table')}[action]
        def validate(value):
            if action == 'currentDatabase':
                assert_equal(value, 'value-00000000')
            elif action == 'tableExists':
                assert_equal(value, True)
            elif action == 'listTables':
                assert_equal(value, [])
            elif action in ('cache', 'persist'):
                check_df(value)
        return op, validate, 1
    if group == 'artifact':
        import tempfile
        from pathlib import Path
        import zipfile
        # TemporaryDirectory is retained by the operation closure until process exit.
        directory = tempfile.TemporaryDirectory(prefix='connectbench-artifact-')
        paths = []
        for i in range(case['count']):
            path = Path(directory.name) / f'bench{i}.{case["suffix"]}'
            if case['suffix'] == 'zip':
                with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as archive:
                    archive.writestr('bench.py', b'#' * (case['size'] - 114))
                assert path.stat().st_size == case['size']
            elif case['suffix'] == 'py':
                path.write_bytes(b'#' * case['size'])
            else:
                # Keep file preparation from preallocating a full upload-sized RSS pool.
                block = b'x' * min(case['size'], 65536)
                with path.open('wb') as stream:
                    remaining = case['size']
                    while remaining:
                        written = min(remaining, len(block))
                        stream.write(block[:written])
                        remaining -= written
            paths.append(str(path))
        def operation():
            _ = directory
            return spark.addArtifacts(*paths,
                pyfile=case['suffix'] in ('py', 'zip') and not case.get('archive', False),
                archive=case.get('archive', False), file=case['suffix'] == 'bin')
        return operation, lambda v: assert_equal(v, None), 0
    if group == 'return-extra':
        return prepare(spark, case | {'group': 'return'})
    raise ValueError(group)
