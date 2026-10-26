"""Unified catalog. Legacy entry points retain their original matrices."""


def core_cases():
    matrix = []
    for stage in ('construct', 'outbound-rpc'):
        for family, sizes in [('deep', (10, 100)), ('wide', (100, 1000)),
                              ('expression', (5, 10)), ('joins', (5, 25)),
                              ('unions', (10, 100)), ('mixed', (10, 100)),
                              ('local', (1000, 100000))]:
            for size in sizes:
                matrix.append(dict(name=f'core-{stage}-{family}-{size}', suite='core',
                    group='core-plan', stage=stage, family=family, size=size,
                    rows=0, width=1, kind='long', batch_rows=1000, iterations=1))
    for kind, width, batch in [('long', 4, 1000), ('long', 50, 10000),
                               ('string', 4, 1000), ('string', 4, 10000)]:
        for action in ('toArrow', 'collect', 'toPandas', 'toLocalIterator', 'collect-scan'):
            matrix.append(dict(name=f'core-return-{kind}-w{width}-b{batch}-{action}',
                suite='core', group='core-return', stage='return-rpc', rows=100000,
                width=width, kind=kind, batch_rows=batch, action=action, iterations=1))
    return matrix


def cases(smoke=False, suites=('all',)):
    from .ferrum_workloads import cases as api_cases
    matrix = core_cases() + [c | {'suite': 'api'} for c in api_cases(include_additional=True)]
    if 'all' not in suites:
        matrix = [c for c in matrix if c['suite'] in suites]
    if smoke:
        names = {'core-construct-unions-10', 'core-outbound-rpc-mixed-10',
                 'core-return-long-w4-b1000-collect-scan', 'api-withColumn',
                 'input-pandas-explicit-10000x3', 'return-nested-collect-scan',
                 'chain-withColumn-64', 'plan-sql-parameters-1-schema-repeat',
                 'query-dataframe-20-one-value', 'input-cache1MiB-60000x3-first-query',
                 'udf-pandas-first-query', 'inspection-printSchema-16-first',
                 'rpc-missing-column', 'artifact-bin-1048576-count1',
                 'return-nested3-collect-scan', 'return-nullable50-toPandas'}
        matrix = [c for c in matrix if c['name'] in names]
    return matrix


def prepare(spark, case):
    from .ferrum_workloads import prepare as prepare_api, assert_equal
    if case.get('suite') != 'core':
        return prepare_api(spark, case)
    if case['stage'] == 'return-rpc':
        return prepare_api(spark, case | {'group': 'return'})
    from .workloads import build
    from pyspark.sql import DataFrame
    if case['stage'] == 'construct':
        def validate(value):
            assert isinstance(value, DataFrame), type(value)
        return lambda: build(spark, case['family'], case['size']), validate, 0
    spark.range(1).limit(0).toArrow()
    return (lambda: build(spark, case['family'], case['size']).toArrow(),
            lambda v: assert_equal(v.num_rows, 0), 0)
