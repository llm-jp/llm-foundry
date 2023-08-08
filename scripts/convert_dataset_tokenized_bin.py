from array import array
from argparse import ArgumentParser, Namespace
import gzip
import os
import re
from typing import BinaryIO, Dict, Iterable

from streaming import MDSWriter
from torch.utils.data import IterableDataset


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Convert binary dataset into MDS format")
    parser.add_argument("--src_root", type=str, required=True)
    parser.add_argument("--out_root", type=str, required=True)
    parser.add_argument("--eod_id", type=int, required=True)

    parser.add_argument("--token_concat_len", type=int, default=2048, help="Concatenate up to this many tokens")
    parser.add_argument("--split", type=str, default="train")
    parser.add_argument("--no_wrap", default=False, action="store_true")
    parser.add_argument("--compression", type=str, default=None)

    parsed = parser.parse_args()

    if os.path.exists(f"{parsed.out_root}/{parsed.split}"):
        raise ValueError(
            f"Remove {parsed.split} from {parsed.out_root} before."
        )

    return parsed


def numeric_key(s: str, num_numeric_fields: int=10):
    m = re.fullmatch("([^0-9]*)([0-9]*)" * num_numeric_fields, s)
    return [int(f) if _ % 2 else f for _, f in enumerate(m.groups()) if _ == 0 or f]


def load_bin_dataset(src_root: str):
    def _process(path: str, fin: BinaryIO):
        num_docs = 0
        num_tokens = 0
        print(f"processing", path, end="\t", flush=True)
        while True:
            try:
                buf = array("i")
                buf.fromfile(fin, 1)
                length = buf[0]
                assert length > 0, f"{length=}, {num_docs=}, {num_tokens=}"
                del buf[0]
                buf.fromfile(fin, length)
                assert len(buf) == length, f"{length=}, len(buf)={len(buf)}, {num_docs=}, {num_tokens=}"
                if num_docs % 10000 == 0:
                    print(".", end="", flush=True)
                num_docs += 1
                num_tokens += length
                yield buf
            except EOFError:
                break
        print()
        print("stats:", path, f"{num_docs} docs", f"{num_tokens} tokens", sep="\t")

    if os.path.isdir(src_root):
        files = sorted(os.listdir(src_root), key=numeric_key)
        src_root += "/"
    else:
        files = [src_root]
        src_root = ""
    for file in files:
        path = f"{src_root}{file}".replace("\\", "/").replace("//", "/")
        if os.path.isdir(path):
            load_bin_dataset(path)
        elif path.endswith(".bin.gz") or path.endswith(".bin.gzip"):
            with gzip.open(path, "rb") as fin:
                for _ in _process(path, fin):
                    yield _
        elif path.endswith(".bin"):
            with gzip.open(path, "rb") as fin:
                for _ in _process(path, fin):
                    yield _
        else:
            print(f"  skip", path)


class ConcatTokenIdsDataset(IterableDataset):
    def __init__(
        self,
        src_root: str,
        eod_id: int,
        token_concat_len: int,
        no_wrap: bool,
    ):
        self.src_root = src_root
        self.eod_id = eod_id
        self.token_concat_len = token_concat_len
        self.should_wrap = not no_wrap

    def __iter__(self) -> Iterable[Dict[str, bytes]]:
        buffer = array("I")
        for sample in load_bin_dataset(self.src_root):
            buffer.extend(sample)
            buffer.append(self.eod_id)
            while len(buffer) >= self.token_concat_len:
                concat_sample = buffer[:self.token_concat_len]
                buffer = buffer[self.token_concat_len:] if self.should_wrap else array("I")
                yield {
                    "tokens": concat_sample.tobytes()
                }


def main(args: Namespace) -> None:
    dataset = ConcatTokenIdsDataset(
        src_root=args.src_root,
        eod_id=args.eod_id,
        token_concat_len=args.token_concat_len,
        no_wrap=args.no_wrap,
    )

    print(f"Converting to MDS format...")
    with MDSWriter(columns={"tokens": "bytes"},
                   out=os.path.join(args.out_root, args.split),
                   compression=args.compression) as out:
        for sample in dataset:
            out.write(sample)


if __name__ == "__main__":
    main(parse_args())
