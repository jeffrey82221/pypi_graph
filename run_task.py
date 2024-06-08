import sys
from etl import get_transformer
from batch_framework.adaptor import GithubActionAdaptor

if __name__ == '__main__':
    etl_obj = get_transformer(local=False)
    adaptor = GithubActionAdaptor(etl_group=etl_obj)
    task_id = sys.argv[1]
    adaptor.run_by_id(task_id)
