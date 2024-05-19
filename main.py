import copy
from batch_framework.filesystem import LocalBackend
from batch_framework.rdb import DuckDBBackend
from src.graph import GraphDataPlatform
from src.meta import metagraph

table_to_graph_transformer = GraphDataPlatform(
    metagraph=copy.deepcopy(metagraph),
    canon_fs=LocalBackend('data/canon/output/'),
    subgraph_fs=LocalBackend('data/subgraph/'),
    output_fs=LocalBackend('data/graph/'),
    rdb=DuckDBBackend()
)