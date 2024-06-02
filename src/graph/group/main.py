from typing import List
from batch_framework.filesystem import FileSystem
from batch_framework.etl import ETLGroup, SQLExecutor
from batch_framework.rdb import RDB
from .groupers import NodeGrouper, LinkGrouper
from .meta import GroupingMeta


class Ingestor(SQLExecutor):
    def __init__(self, target_ids: List[str], rdb: RDB,
                 input_fs: FileSystem):
        self._target_ids = target_ids
        super().__init__(rdb, input_fs=input_fs)

    @property
    def input_ids(self):
        return self._target_ids

    @property
    def output_ids(self):
        return [id.replace('_final', '') for id in self.input_ids]

    def sqls(self, **kwargs):
        results = dict()
        for in_id, out_id in zip(self.input_ids, self.output_ids):
            results[out_id] = f'SELECT * FROM {in_id}'
        return results


class GraphGrouper(ETLGroup):
    def __init__(self, meta: GroupingMeta, rdb: RDB, input_fs: FileSystem,
                 output_fs: FileSystem):
        node_grouper = NodeGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=output_fs
        )
        link_grouper = LinkGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=output_fs
        )
        self._meta = meta
        self._inputs = node_grouper.input_ids + link_grouper.input_ids
        self._outputs = node_grouper.output_ids + link_grouper.output_ids
        ingestor = Ingestor(
            self._outputs,
            rdb,
            input_fs=output_fs
        )
        args = [node_grouper, link_grouper, ingestor]
        super().__init__(*args)

    @property
    def input_ids(self):
        return self._inputs

    @property
    def output_ids(self):
        return self._outputs
