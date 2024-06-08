import copy
from batch_framework.filesystem import LocalBackend, DropboxBackend
from src.main import WholeGraphDataPlatform
from src.meta import metagraph


def rawdata_cloud2local():
    """
    Migrate input raw data from Dropbox
    to local.
    """
    for folder in ['raw']:
        local_fs = LocalBackend(f'./data/canon/{folder}/')
        dropbox_fs = DropboxBackend(f'/data/canon/{folder}/')
        for file in ['latest.parquet']:
            print('folder:', folder, 'file:', file, 'upload started')
            buff = dropbox_fs.download_core(file)
            buff.seek(0)
            local_fs.upload_core(buff, file)
            print('folder:', folder, 'file:', file, 'uploaded')


def get_transformer(local: bool = False) -> WholeGraphDataPlatform:
    if local:
        return WholeGraphDataPlatform(
            metagraph=copy.deepcopy(metagraph),
            raw_fs=LocalBackend('data/canon/raw/'),
            canon_fs=LocalBackend('data/canon/output/'),
            subgraph_fs=LocalBackend('data/subgraph/'),
            output_fs=LocalBackend('data/graph/')
        )
    else:
        return WholeGraphDataPlatform(
            metagraph=copy.deepcopy(metagraph),
            raw_fs=DropboxBackend('/data/canon/raw/'),
            canon_fs=DropboxBackend('/data/canon/output/'),
            subgraph_fs=DropboxBackend('/data/subgraph/'),
            output_fs=DropboxBackend('/data/graph/')
        )


if __name__ == '__main__':
    # rawdata_cloud2local()
    graph_platform = get_transformer(local=False)
    graph_platform.execute()
