import os
from batch_framework.filesystem import LocalBackend, DropboxBackend
from batch_framework.rdb import DuckDBBackend
from src.meta import metagraph
from src.puppygraph import convert_duckdb_to_schema
from src.puppygraph import ResultCollectLayer
import json
rdb = DuckDBBackend(LocalBackend('data/duckdb'), db_name='demo.db')
to_puppygraph_adaptor = ResultCollectLayer(
    rdb, metagraph=metagraph,
    input_fs=DropboxBackend('/data/graph/')
)

if __name__ == '__main__':
    if os.path.exists('data/duckdb/demo.db'):
        os.remove('data/duckdb/demo.db')
    if os.path.exists('duckdb/demo.db'):
        os.remove('duckdb/demo.db')
    to_puppygraph_adaptor.execute()
    rdb.commit()
    schema = convert_duckdb_to_schema(
        'data/duckdb/demo.db',
        metagraph=metagraph
    )
    json.dump(schema, open('schema.json', 'w'))
