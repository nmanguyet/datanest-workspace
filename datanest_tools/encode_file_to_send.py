
#%%
import base64
from turtle import pd
import zipfile

from pyparsing import line

# Read the file as bytes
with open("/Users/anhnguyet/Documents/dev_fts/oil_usd_raw_202001_202512.csv.zip", "rb") as file:
    encoded_string = base64.b64encode(file.read()).decode('utf-8')

print(encoded_string)
# print(base64.b64decode(encoded_string).decode("utf-8")
# %%
len(encoded_string)

# %%
import lzma, base64, hashlib, glob, os

SRC = "/Users/anhnguyet/Documents/dev_fts/tac_vnpt.csv"
DIR = "chunks"
N   = 8

def h8(s):  # checksum ngan cho tung chunk
    return hashlib.sha256(s.encode()).hexdigest()[:8]

def encode():
    data = open(SRC, "rb").read()
    comp = lzma.compress(data, preset=9 | lzma.PRESET_EXTREME)
    enc  = base64.b64encode(comp).decode()
    size = (len(enc) + N - 1) // N
    os.makedirs(DIR, exist_ok=True)
    lines = [f"FULL {len(enc)} {hashlib.sha256(comp).hexdigest()}"]
    for k, i in enumerate(range(0, len(enc), size)):
        c = enc[i:i+size]
        open(f"{DIR}/part_{k:02d}.txt", "w").write(c)
        lines.append(f"{k:02d} {len(c)} {h8(c)}")     # idx  do_dai  checksum
    open(f"{DIR}/manifest.txt", "w").write("\n".join(lines))
    print(f"Da tao {N} chunk + manifest trong '{DIR}/'")

def verify():   # kiem tra tung chunk hien co so voi manifest
    man = {}
    full = None
    for ln in open(f"{DIR}/manifest.txt").read().splitlines():
        p = ln.split()
        if p[0] == "FULL": full = (int(p[1]), p[2])
        else: man[p[0]] = (int(p[1]), p[2])
    all_ok = True
    for idx in sorted(man):
        f = f"{DIR}/part_{idx}.txt"
        want_len, want_h = man[idx]
        if not os.path.exists(f):
            print(f"part {idx}: THIEU"); all_ok = False; continue
        c = open(f).read()                       # khong strip: phai trung voi luc encode ghi/hash
        if len(c) != want_len:
            print(f"part {idx}: SAI DO DAI (co {len(c)}, can {want_len})"); all_ok = False
        elif h8(c) != want_h:
            print(f"part {idx}: SAI NOI DUNG (checksum lech)"); all_ok = False
        else:
            print(f"part {idx}: OK  (len={len(c)}, hash={h8(c)})")
    return all_ok, full

def decode():
    ok, full = verify()
    if not ok:
        print("\n>> Con chunk sai/thieu, sua roi chay lai."); return
    files = sorted(glob.glob(f"{DIR}/part_*.txt"))
    joined = "".join(open(f).read() for f in files)   # khong strip, trung voi verify/encode
    comp = base64.b64decode(joined)
    assert hashlib.sha256(comp).hexdigest() == full[1], "SHA tong khong khop"
    open("restored.csv", "wb").write(lzma.decompress(comp))
    print("\n>> OK het. Da ghi restored.csv")

verify()
# encode()
# %%
