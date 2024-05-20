import os
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

def graphdata_local2cloud():
    """
    Migrade subgrade and graph data from local to 
    Dropbox
    """
    for folder in ['subgraph', 'graph']:
        local_fs = LocalBackend(f'./data/{folder}/')
        dropbox_fs = NewDropboxBackend(f'/data/{folder}/')
        for file in os.listdir(f'./data/{folder}/'):
            print('folder:', folder, 'file:', file, 'upload started')
            buff = local_fs.download_core(file)
            buff.seek(0)
            dropbox_fs.upload_core(buff, file)
            print('folder:', folder, 'file:', file, 'uploaded')

def get_transformer(local: bool=False, rdb: DuckDBBackend=DuckDBBackend()) -> GraphDataPlatform:
    if local:
        return GraphDataPlatform(
            metagraph=copy.deepcopy(metagraph),
            canon_fs=LocalBackend('data/canon/output/'),
            subgraph_fs=LocalBackend('data/subgraph/'),
            output_fs=LocalBackend('data/graph/'),
            rdb=rdb
        )
    else:
        return GraphDataPlatform(
            metagraph=copy.deepcopy(metagraph),
            canon_fs=NewDropboxBackend('/data/canon/output/'),
            subgraph_fs=NewDropboxBackend('/data/subgraph/'),
            output_fs=NewDropboxBackend('/data/graph/'),
            rdb=rdb
        )


if __name__ == '__main__':
    # rawdata_cloud2local()
    rdb = DuckDBBackend(LocalBackend('data/duckdb'), db_name='demo.db')
    transformer = get_transformer(local=True, rdb=rdb)
    transformer.execute()
    df = rdb._conn.execute('show all tables;').fetchdf()
    cols = df.columns
    print(cols)
    print(df[['name', 'column_names']])
    rdb.commit()
    # graphdata_local2cloud()
    # get_transformer(local=False).execute()