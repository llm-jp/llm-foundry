import sys
import json


def head_n_tokens(max_tokens, fin, fout):
    docs = 0
    tokens = 0
    for _ in fin:
        r = json.loads(_)
        c = len(r["tokens"])
        if max_tokens < 0 and -max_tokens < tokens + c:
            break
        docs += 1
        tokens += c
        print(_, end="", file=fout)
        if max_tokens > 0 and max_tokens < tokens:
            break
    print(f"stats: {docs=}, {tokens=}", file=sys.stderr)


if __name__ == "__main__":
    head_n_tokens(int(sys.argv[1]), sys.stdin, sys.stdout)
