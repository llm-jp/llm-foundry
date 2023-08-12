from array import array
from datetime import datetime
import json
import sys

from sentencepiece import SentencePieceProcessor


def tokenize_jsonl(tokenizer, fin, fout, input_field="text"):
    num_docs = 0
    num_tokens = 0
    for line in fin:
        r = json.loads(line)
        text = r[input_field]
        token_ids = tokenizer.encode(text)
        buf = array("I")
        buf.append(len(token_ids))
        buf.extend(token_ids)
        buf.tofile(fout)
        num_docs += 1
        num_tokens += len(token_ids)
    return num_docs, num_tokens


if __name__ == "__main__":
    sp_model_file = sys.argv[1]
    jsonl_files = sys.argv[2:]

    def _exec():
        t = datetime.now()
        num_docs, num_tokens = tokenize_jsonl(tokenizer, fin, sys.stdout.buffer)
        t = datetime.now() - t
        print(f"stats: {sp_model_file} {jsonl_file} {num_docs} docs, {num_tokens} tokens, processed in {t}", file=sys.stderr)

    tokenizer = SentencePieceProcessor(model_file=sp_model_file)
    if jsonl_files:
        for jsonl_file in jsonl_files:
            with open(jsonl_file, "r", encoding="utf8") as fin:
                _exec()
    else:
        jsonl_file = "[STDIN]"
        _exec()
