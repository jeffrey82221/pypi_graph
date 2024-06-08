import duckdb
from typing import Dict
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import FileSystem
from .meta import MetaGraph

type_mapping = {
    'VARCHAR': 'String',
    'INTEGER': 'Int',
    'UBIGINT': 'Int',
    'BIGINT': 'Int',
    'NUMERIC': 'Double'
}


class ResultCollectLayer(SQLExecutor):
    """
    Store all generated links and nodes tables
    into DuckDB.
    """

    def __init__(self, rdb: DuckDBBackend, metagraph: MetaGraph,
                 input_fs: FileSystem):
        nodes = list(metagraph.node_grouping.keys())
        links = list(metagraph.triplets.keys())
        self._targets = [
            f'node_{n}' for n in nodes
        ] + [
            f'link_{l}' for l in links
        ]
        super().__init__(rdb, input_fs=input_fs)

    @property
    def input_ids(self):
        return [f'{t}_final' for t in self._targets]

    @property
    def output_ids(self):
        return self._targets

    def sqls(self, **kwargs):
        results = dict()
        for target in self._targets:
            results[target] = f'SELECT * FROM {target}_final'
        return results


def convert_duckdb_to_schema(duckdb_path: str, metagraph: MetaGraph) -> Dict:
    """
    Generate schema.json of puppy graph
    from metagraph and the graph-holding duck db.
    """
    conn = duckdb.connect(duckdb_path)
    db_tables = conn.execute('SHOW ALL TABLES;').fetchdf()
    db_tables.set_index('name', inplace=True)
    print(db_tables[['column_names', 'column_types']])
    nodes = list(metagraph.node_grouping.keys())
    links = list(metagraph.triplets.keys())
    db_table_names = sorted(db_tables.index.tolist())
    target_table_names = sorted(
        [f'node_{n}' for n in nodes] + [f'link_{l}' for l in links])
    assert len(db_table_names) == len(
        target_table_names), f'duck db missing tables: {set(target_table_names) - set(db_table_names)}'
    schema = dict()
    schema['catalogs'] = [
        {
            "name": "duckdb_data",
            "type": "duckdb",
            "jdbc": {
                "jdbcUri": "jdbc:duckdb:/home/share/demo.db",
                "driverClass": "org.duckdb.DuckDBDriver"
            }
        }
    ]
    vertices = []
    for node in nodes:
        table_name = f'node_{node}'
        column_names = db_tables.loc[table_name]['column_names']
        column_types = db_tables.loc[table_name]['column_types']
        attributes = [{
            'type': type_mapping[t],
            'name': n
        } for n, t in zip(column_names, column_types) if n not in [
            'node_id', 'from_id', 'to_id']]
        vertices.append(
            {
                'label': node,
                'mappedTableSource': {
                    'catalog': 'duckdb_data',
                    'schema': 'main',
                    'table': table_name,
                    'metaFields': {
                        "id": 'node_id'
                    }
                },
                'attributes': attributes
            }
        )
    edges = []
    for link in links:
        table_name = f'link_{link}'
        column_names = db_tables.loc[table_name]['column_names']
        column_types = db_tables.loc[table_name]['column_types']
        attributes = [{
            'type': type_mapping[t],
            'name': n
        } for n, t in zip(column_names, column_types) if n not in ['link_id', 'from_id', 'to_id']]
        edges.append(
            {
                'label': link,
                'mappedTableSource': {
                    'catalog': 'duckdb_data',
                    'schema': 'main',
                    'table': table_name,
                    'metaFields': {
                        "id": 'link_id',
                        "from": 'from_id',
                        "to": 'to_id'
                    }
                },
                'from': metagraph.triplets[link][0],
                'to': metagraph.triplets[link][1],
                'attributes': attributes
            }
        )
    schema['vertices'] = vertices
    schema['edges'] = edges
    return schema
