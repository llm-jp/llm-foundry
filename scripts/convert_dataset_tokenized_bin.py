# Copyright 2022 MosaicML LLM Foundry authors
# SPDX-License-Identifier: Apache-2.0

"""Streaming dataset conversion scripts for json files."""
from array import array
from argparse import ArgumentParser, Namespace
import gzip
import os
from typing import Dict, Iterable, Optional

from streaming import MDSWriter
from torch.utils.data import IterableDataset
from tqdm import tqdm


def parse_args() -> Namespace:
    """Parse commandline arguments."""
    parser = ArgumentParser(
        description=
        'Convert dataset into MDS format, optionally concatenating and tokenizing'
    )
    parser.add_argument('--path', type=str, required=True)
    parser.add_argument('--out_root', type=str, required=True)
    parser.add_argument('--compression', type=str, default=None)

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--concat_tokens',
        type=int,
        help='Convert text to tokens and concatenate up to this many tokens')
    parser.add_argument('--split', type=str, default='train')

    parser.add_argument('--no_wrap', default=False, action='store_true')

    parsed = parser.parse_args()

    if os.path.isdir(parsed.out_root) and len(
            set(os.listdir(parsed.out_root)).intersection(set(
                parsed.split))) > 0:
        raise ValueError(
            f'--out_root={parsed.out_root} contains {os.listdir(parsed.out_root)} which cannot overlap with the requested splits {parsed.splits}.'
        )

    return parsed


class ConcatTokenIdsDataset(IterableDataset):
    """An IterableDataset that returns token samples for MDSWriter.

    Returns dicts of {'tokens': bytes}
    """

    def __init__(
        self,
        bin_dataset: Iterable[array],
        max_length: int,
        no_wrap: bool,
    ):
        self.bin_dataset = bin_dataset
        self.max_length = max_length
        self.should_wrap = not no_wrap

    def __iter__(self) -> Iterable[Dict[str, bytes]]:
        buffer = array("I")
        for sample in self.bin_dataset:
            buffer += sample
            while len(buffer) >= self.max_length:
                concat_sample = buffer[:self.max_length]
                buffer = buffer[self.max_length:] if self.should_wrap else array("I")
                yield {
                    # convert to bytes to store in MDS binary format
                    'tokens': concat_sample.tobytes()
                }


def load_bin_dataset(path: str) -> Iterable[array]:
    files = os.listdir(path)
    for file in files:
        with gzip.open(file, "rb") as fin:
            buf = array("I")
            buf.fromfile(fin, 1)
            length = buf[0]
            buf.fromfile(fin, length)
            yield buf


def build_bin_dataset(
    path: str,
    split: str,
    max_length: Optional[int] = None,
    no_wrap: bool = False,
) -> IterableDataset:
    """Build an IterableDataset over the HF C4 or pile source data.

    Args:
        path (str): path to Dataset dir or file
        split (str): Split name.
        max_length (int): The length of concatenated tokens
        no_wrap (bool): if concatenating, whether to wrap text across `max_length` boundaries

    Returns:
        An IterableDataset.
    """
    if max_length is None:
        raise ValueError(f'max_length must be set.')

    bin_dataset = load_bin_dataset(path)
    return ConcatTokenIdsDataset(
        bin_dataset=bin_dataset,
        max_length=max_length,
        no_wrap=no_wrap,
    )


def main(args: Namespace) -> None:
    """Main: create C4/pile streaming dataset.

    Args:
        args (Namespace): Commandline arguments.
    """
    columns = {'tokens': 'bytes'}
    hf_split = args.split
    folder_split = args.split

    # Get samples
    dataset = build_bin_dataset(path=args.path,
                               split=hf_split,
                               max_length=args.concat_tokens,
                               no_wrap=args.no_wrap)

    print('here')

    # Write samples
    print(f'Converting to MDS format...')
    print(
        f'Note that the progress bar is based on the dataset length before tokenization.'
    )
    print(f'It will finish at a value below 100% if tokenizing')
    with MDSWriter(columns=columns,
                   out=os.path.join(args.out_root, folder_split),
                   compression=args.compression) as out:
        for sample in tqdm(dataset, desc=folder_split):
            out.write(sample)


if __name__ == '__main__':
    main(parse_args())
