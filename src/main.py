from typing import List
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import FileSystem
from batch_framework.etl import ETLGroup
from batch_framework.storage import PandasStorage
from .graph import GraphDataPlatform
from .graph.metagraph import MetaGraph
from .tabularize import LatestTabularize


class WholeGraphDataPlatform(ETLGroup):
    """
    Data Flow:
        1. canonicalize data
        2. extract subgraphs
        3. do entity resolution
        4. group subgraph
    """

    def __init__(self, metagraph: MetaGraph,
                 raw_fs: FileSystem,
                 canon_fs: FileSystem,
                 subgraph_fs: FileSystem,
                 output_fs: FileSystem
                 ):
        # Connecting MetaGraph with Entity Resolution Meta
        # Basic ETL components
        # 1. Extract Subgraphs from Canonicalized Tables
        args = []
        args.append(
            LatestTabularize(
                input_storage=PandasStorage(raw_fs),
                output_storage=PandasStorage(canon_fs)
            )
        )
        args.append(GraphDataPlatform(
            metagraph,
            canon_fs,
            subgraph_fs=subgraph_fs,
            output_fs=output_fs,
            rdb=DuckDBBackend()
        ))
        self._input_ids = args[0].input_ids
        self._output_ids = args[-1].output_ids
        super().__init__(*args)

    @property
    def input_ids(self):
        return self._input_ids

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids

    @property
    def output_ids(self):
        return self._output_ids
