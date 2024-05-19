import copy
from batch_framework.filesystem import LocalBackend
from plugins.new_dropbox_backend import NewDropboxBackend
from batch_framework.rdb import DuckDBBackend
from src.graph import GraphDataPlatform
from src.meta import metagraph

def rawdata_cloud2local():
    """
    Migrate input raw data from Dropbox 
    to local.
    """
    for folder in ['output']:
        local_fs = LocalBackend(f'./data/canon/{folder}/')
        dropbox_fs = NewDropboxBackend(f'/data/canon/{folder}/')
        for file in ['latest_package.parquet', 'latest_requirement.parquet', 'latest_url.parquet']:
            print('folder:', folder, 'file:', file, 'upload started')
            buff = dropbox_fs.download_core(file)
            buff.seek(0)
            local_fs.upload_core(buff, file)
            print('folder:', folder, 'file:', file, 'uploaded')

table_to_graph_transformer = GraphDataPlatform(
    metagraph=copy.deepcopy(metagraph),
    canon_fs=LocalBackend('data/canon/output/'),
    subgraph_fs=LocalBackend('data/subgraph/'),
    output_fs=LocalBackend('data/graph/'),
    rdb=DuckDBBackend()
)

if __name__ == '__main__':
    # rawdata_cloud2local()
    table_to_graph_transformer.execute()
