import io
from fsspec.implementations.dirfs import DirFileSystem
import tqdm
import base64
import time
from typing import Iterator
from threading import Thread
from split_file_reader.split_file_writer import SplitFileWriter
from split_file_reader import SplitFileReader
from concurrent.futures import ThreadPoolExecutor
from batch_framework.filesystem import DropboxBackend

__all__ = ['NewDropboxBackend']


class Pipe:
    """
    FIFO pipe for connecting a writing thread with
    a reading thread
    """

    def __init__(self, max_size=8, wait_duration=0.01):
        self._max_size = max_size
        self._current_size = 0
        self._array = []
        self._index = -1
        self._on = True
        self._wait_duration = wait_duration
        self._total_size = None

    def send_input(self, input_element):
        assert self._on, 'pipe already closed'
        while self._current_size >= self._max_size:
            time.sleep(self._wait_duration)
        self._array.append(input_element)
        self._current_size += 1

    def generate_output(self) -> Iterator:
        while self._current_size == 0:
            time.sleep(self._wait_duration)
        while self._on or self._index < len(self._array) - 1:
            self._index += 1
            yield self._array[self._index]
            self._current_size -= 1
            while self._on and self._current_size == 0:
                time.sleep(self._wait_duration)

    def load_input(self, input_pipe: Iterator):
        assert self._total_size is not None
        for element in input_pipe:
            self.send_input(element)

    def __getitem__(self, idx: int):
        if idx >= self._total_size:
            raise IndexError
        self._index += 1
        assert idx == self._index
        while idx >= len(self._array):
            time.sleep(self._wait_duration)
        result = self._array[idx]
        self._current_size -= 1
        return result

    def set_total_size(self, count: int):
        self._total_size = count

    def __len__(self):
        while self._total_size is None:
            time.sleep(self._wait_duration)
        return self._total_size

    def close(self):
        self._on = False


class NewDropboxBackend(DropboxBackend):
    def upload_core(self, file_obj: io.BytesIO, remote_path: str):
        """Upload file object

        Args:
            file_obj (io.BytesIO): file to be upload
            remote_path (str): remote file path
        """
        self._upload_core(file_obj, remote_path)
        self._check_upload_success(file_obj, remote_path)
        file_obj.flush()
        file_obj.close()

    def _check_upload_success(self, file_obj: io.BytesIO, remote_path: str):
        download_data = self.download_core(remote_path)
        download_data.seek(0)
        file_obj.seek(0)
        assert download_data.getbuffer() == file_obj.getbuffer(), 'upload before != after'
        download_data.flush()
        download_data.close()

    def _upload_core(self, file_obj: io.BytesIO,
                     remote_path: str, max_workers=8, chunk_size=1000000):
        """Upload file object to local storage

        Args:
            file_obj (io.BytesIO): file to be upload
            remote_path (str): remote file path
        """
        assert '.' in remote_path, f'requires file ext .xxx provided in `remote_path` but it is {remote_path}'
        file_name = remote_path.split('.')[0]
        ext = remote_path.split('.')[1]
        if self._fs.exists(file_name):
            self._fs.rm(file_name)
        self._fs.mkdir(file_name)
        file_obj.seek(0)
        chunk_cnt = file_obj.getbuffer().nbytes // chunk_size + 1
        assert self._fs.exists(file_name), f'{file_name} folder make failed'
        dfs = DirFileSystem(f'/{file_name}', self._fs)
        pipe = Pipe(max_workers)
        thread = Thread(target=self._upload_all,
                        args=(dfs, ext, pipe, max_workers, chunk_cnt, remote_path))
        thread.start()

        def gen(pipe):
            while True:
                buff = io.BytesIO()
                pipe.send_input(buff)
                yield buff
        with SplitFileWriter(gen(pipe), chunk_size) as sfw:
            sfw.write(file_obj.getbuffer())
        pipe.close()
        thread.join()

    def _upload_all(self, dfs, ext, pipe, max_workers, chunk_cnt, remote_path):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            input_pipe = enumerate(pipe.generate_output())
            output_pipe = executor.map(
                lambda x: self._upload_chunk(dfs, ext, x[0], x[1]),
                input_pipe)
            output = list(
                tqdm.tqdm(
                    output_pipe,
                    total=chunk_cnt,
                    desc=f'Upload {remote_path}'))
        total_size = len(output)
        print('number of chunks:', total_size)
        with dfs.open('total.txt', 'w') as f:
            f.write(str(total_size))
        print(f'Done upload {total_size} files')

    def _upload_chunk(self, dfs, ext, index, chunk):
        chunk.seek(0)
        with dfs.open(f'{index}.{ext}', 'w') as f:
            data = base64.b64encode(chunk.read()).decode()
            f.write(data)
        chunk.flush()
        chunk.close()

    def download_core(self, remote_path: str) -> io.BytesIO:
        """Download file from remote storage

        Args:
            remote_path (str): remote file path

        Returns:
            io.BytesIO: downloaded file
        """
        assert '.' in remote_path, f'requires file ext .xxx provided in `remote_path` but it is {remote_path}'
        file_name = remote_path.split('.')[0]
        ext = remote_path.split('.')[1]
        assert self._fs.exists(
            f'{file_name}'), f'{file_name} folder does not exists for FileSystem: {self._fs}'
        dfs = DirFileSystem(file_name, self._fs)
        with dfs.open('total.txt', 'r') as f:
            total_size = int(f.read())
        max_workers = 32
        pipe = Pipe(max_workers)
        pipe.set_total_size(total_size)
        print(f'Start download {total_size} files')
        thread = Thread(
            target=self._download_all,
            args=(dfs, ext, pipe, max_workers, total_size, remote_path)
        )
        thread.start()
        with SplitFileReader(pipe) as sfw:
            result = io.BytesIO(sfw.read())
        thread.join()
        print(f'Done download {total_size} files')
        result.seek(0)
        return result

    def _download_all(self, dfs, ext, pipe, max_workers,
                      chunk_cnt, remote_path):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            input_pipe = range(chunk_cnt)
            input_pipe = map(lambda index: (dfs, index, ext), input_pipe)
            chunks = executor.map(self._download_chunk, input_pipe)
            chunks = tqdm.tqdm(
                chunks,
                desc=f'Download {remote_path}',
                total=chunk_cnt
            )
            pipe.load_input(chunks)

    def _download_chunk(self, x):
        dfs, index, ext = x
        with dfs.open(f'{index}.{ext}', 'r') as f:
            chunk = io.BytesIO(base64.b64decode(f.read()))
        return chunk
