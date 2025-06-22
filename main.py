import shutil
import os
import zlib
import io
import typing


def get_head_block(head: bytes) -> typing.Tuple[int, int, int]:
    if len(head) != 53:
        raise ValueError("Неверный формат файла: ожидалcя заголовок блока")

    type_block = int(head[0:16].decode("utf-8"), base=16)
    len_block = int(head[17:33].decode("utf-8"), base=16)
    address_next_block = int(head[34:50].decode("utf-8"), base=16)
    if address_next_block == 18446744073709551615:
        address_next_block = None
    return (head, type_block, len_block, address_next_block)


def get_data_block(file: io.BufferedReader, offset: int) -> bytes:
    data = file.read(offset)
    return data


def read_block(
    file: io.BufferedReader, readed_blocks: set
) -> typing.Tuple[int, bytes, bytes]:
    readed_blocks.add(file.tell())
    
    start_line = file.readline()
    if not start_line:
        return None
        
    if start_line != b"\r\n":
        raise ValueError("Неверный формат файла: ожидалась стартовая строка блока")
        
    head, head1, len_block, address_block = get_head_block(file.readline())

    data_block = get_data_block(file, len_block)
    if address_block != None:
        current_address = file.tell()
        file.seek(address_block, 0)

        _, _, data_related_block = read_block(file, readed_blocks)

        if data_related_block != None:
            data_block = data_block + data_related_block
            
        file.seek(current_address, 0)

    return (head1, head, data_block)


path_dir = os.getcwd()
path_source = os.path.join(path_dir, "ccs")
path_log = os.path.join(path_dir, "log")
path_files = os.path.join(path_dir, "files")

if os.path.exists(path_files):
    shutil.rmtree(path_files)
os.mkdir(path_files)

file_source = open(path_source, "rb")
file_log = open(path_log, "w")

file_source.seek(20, 0)

readed_blocks = set()

while True:
    state = ""
    current_postion = file_source.tell()
    if current_postion in readed_blocks:
        state = "skipped"
    read_block_result = read_block(file_source, readed_blocks)
    if read_block_result == None:
        break
    head1, head, data = read_block_result
    # b"0000000000000060" = 96
    # b"0000000000000064" = 100
    # b"0000000000000066" = 102
    # if head1 in (96, 100, 102):
    if state != "skipped":
        try:
            data_for_write = zlib.decompress(data, -15)
            state = "decommpressed"
        except Exception as e:
            print(f"{file_source.tell()} {str(e)}")
            data_for_write = data
            state = "error"
        head_str = head.decode("utf-8").rstrip()
        file_receiver = open(
            os.path.join(
                path_files,
                f"{file_source.tell()}_{head_str.replace(' ', '_')}_{state}.txt",
            ),
            "wb",
        )
        file_receiver.write(data_for_write)
        file_receiver.close()
    file_log.write(f"{current_postion}: {head_str} {state} \r\n")

file_source.close()
file_log.close()
