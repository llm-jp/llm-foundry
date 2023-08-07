from array import array
import gzip
import json
import sys

from sentencepiece import SentencePieceProcessor


def tokenize_jsonl(tokenizer, fin, fout, input_field="text"):
    for line in fin:
        r = json.loads(line)
        text = r[input_field]
        token_ids = tokenizer.encode(text)
        buf = array("I")
        buf.append(len(token_ids))
        buf.extend(token_ids)
        buf.tofile(fout)


if __name__ == "__main__":
    tokenizer = SentencePieceProcessor(model_file=sys.argv[1])
    tokenize_jsonl(tokenizer, sys.stdin, sys.stdout.buffer)
