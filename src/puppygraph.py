import duckdb
from typing import Dict
from .meta import MetaGraph
type_mapping = {
        'VARCHAR': 'String',
        'INTEGER': 'Int',
        'UBIGINT': 'Int',
        'BIGINT': 'Int',
        'NUMERIC': 'Double'
    }
"""
{"Status":"Error","Message":
"Please fix 3 schema problems:\nerror: 
can not convert the type of the 'num_releases' 
attribute of the 'package' vertex from 'BIGINT' to 'Int'\n
error: meta field 'link_id' not found in table 'link_has_url'\n
error: can not convert the type of the 'num_match_dist' attribute of 
the 'has_requirement' edge 
from 'BIGINT' to 'Int'\n"}% 
"""
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
    assert len(db_tables) == len(nodes + links), 'duck db schema does not match with metagraph'
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
        column_names = db_tables.loc[f'node_{node}']['column_names']
        column_types = db_tables.loc[f'node_{node}']['column_types']
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
                    'table': f'node_{node}',
                    'metaFields': {
                        "id": 'node_id'
                    }
                },
                'attributes': attributes
            }
        )
    edges = []
    for link in links:
        try:
            column_names = db_tables.loc[f'link_{link}']['column_names']
            column_types = db_tables.loc[f'link_{link}']['column_types']
        except KeyError as e:
            raise ValueError(db_tables) from e
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
                    'table': f'link_{link}',
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
