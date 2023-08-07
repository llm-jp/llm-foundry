from array import array
import gzip
import sys


def load_bin_dataset(path, fin, max_docs, max_tokens):
    num_docs = 0
    num_tokens = 0
    while True:
        try:
            buf = array("I")
            buf.fromfile(fin, 1)
            length = buf[0]
            buf.fromfile(fin, length)
            num_docs += 1
            if max_docs > 0 and num_docs == max_docs:
                break
            num_tokens += length
            if max_tokens < 0 and num_tokens > -max_tokens:
                break
            yield buf
            if max_tokens > 0 and num_tokens >= max_tokens:
                break
        except EOFError:
            break
    print("stats:", path, f"{num_docs} docs", f"{num_tokens} tokens", sep="\t")


def main() -> None:
    path = sys.argv[1]
    max_docs = int(sys.argv[2])
    max_tokens = int(sys.argv[3])
    if path.endswith(".bin.gz") or path.endswith(".bin.gzip"):
        with gzip.open(path, "rb") as fin:
            for _ in load_bin_dataset(path, fin, max_docs, max_tokens):
                _.tofile(sys.stdout.buffer)
    elif path.endswith(".bin"):
        with open(path, "rb") as fin:
            for _ in load_bin_dataset(path, fin, max_docs, max_tokens):
                _.tofile(sys.stdout.buffer)
    else:
        assert False, f"bad path: {path}"


if __name__ == "__main__":
    main()
