"""
Validate ID(s) of a Subgraph
"""
from batch_framework.storage import PandasStorage
from batch_framework.etl import ETLGroup
from ..metagraph import MetaGraph
from ..validate import FromLinkIDValidator, ToLinkIDValidator

__all__ = ['Validator']


class Validator(ETLGroup):
    def __init__(self, metagraph: MetaGraph, storage: PandasStorage):
        self._storage = storage
        self.metagraph = metagraph
        super().__init__(*self.validator_list)

    @property
    def input_ids(self):
        results = []
        results.extend(self.metagraph.nodes)
        results.extend(self.metagraph.links)
        return results

    @property
    def output_ids(self):
        return []

    @property
    def validator_list(self):
        results = []
        for link, (src_node, target_node) in self.metagraph._subgraphs.items():
            results.append(
                FromLinkIDValidator(
                    link,
                    src_node,
                    self._storage))
            results.append(
                ToLinkIDValidator(
                    link,
                    target_node,
                    self._storage))
        return results
