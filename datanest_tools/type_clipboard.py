##
import fcntl
import os
import shutil
import subprocess
import sys
import tempfile
import time

# ============================================================
# CAU HINH - chinh thong so cho tung che do o day
# ============================================================
# --- Code KHO (VM kho tinh) ---
HARD_WAIT          = 4      # giay cho de click vao VM
HARD_DELAY_MS      = 20     # do tre giua cac phim (ms); rot ky tu -> tang len
HARD_NEWLINE_PAUSE = 0.03   # nghi them sau moi lan xuong dong
HARD_CHUNK         = 20     # so phim moi lot, kiem tra dung khan cap giua cac lot
HARD_STOP_CORNER   = True   # dua chuot len goc TREN-TRAI de dung

# --- Code DE (VPN thuong) ---
EASY_WAIT     = 5           # giay cho de click vao cua so remote
EASY_DELAY_MS = 10          # do tre khi xdotool type

# --- Chung cho CA 2 che do ---
CHECK_VIETNAMESE = True     # dang bat bo go tieng Viet -> bao va dung (ca 2 che do)
# ============================================================

# Anh xa ky tu dac biet -> ten keysym cua X (dung cho Code KHO)
SPECIAL = {
    " ": "space",
    "!": "exclam", "@": "at", "#": "numbersign", "$": "dollar",
    "%": "percent", "^": "asciicircum", "&": "ampersand", "*": "asterisk",
    "(": "parenleft", ")": "parenright", "-": "minus", "_": "underscore",
    "=": "equal", "+": "plus", "[": "bracketleft", "{": "braceleft",
    "]": "bracketright", "}": "braceright", "\\": "backslash", "|": "bar",
    ";": "semicolon", ":": "colon", "'": "apostrophe", '"': "quotedbl",
    ",": "comma", "<": "less", ".": "period", ">": "greater",
    "/": "slash", "?": "question", "`": "grave", "~": "asciitilde",
}

# Chuan hoa ky tu "thong minh" ve ASCII (dung cho Code KHO)
SMART = {
    "\u201c": '"', "\u201d": '"',
    "\u2018": "'", "\u2019": "'",
    "\u2013": "-", "\u2014": "-",
    "\u00a0": " ",
    "\u2026": "...",
}

VN_MARKERS = ("bamboo", "unikey", "telex", "vni", "viqr", "vietnam", "openkey")


# ------------------------- helper dung chung -------------------------

def have(cmd):
    return shutil.which(cmd) is not None


_lock_fh = None  # giu tham chieu de khoa khong bi giai phong som


def acquire_single_instance():
    """Chi cho phep 1 phien chay. Neu da co phien khac -> thoat ngay.
    flock tu dong nha khi tien trinh ket thuc, khong lo file khoa cu."""
    global _lock_fh
    path = os.path.join(tempfile.gettempdir(), "vm_turbotyper.lock")
    _lock_fh = open(path, "w")
    try:
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        print("Da co mot phien VM TurboTyper dang chay -> thoat.",
              file=sys.stderr)
        sys.exit(0)


def _out(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=3).stdout.strip()
    except Exception:
        return ""


def read_clipboard():
    for cmd in (["xclip", "-selection", "clipboard", "-o"],
                ["xsel", "-b", "-o"],
                ["wl-paste", "-n"]):
        if have(cmd[0]):
            try:
                return subprocess.check_output(cmd, text=True)
            except subprocess.CalledProcessError:
                return ""
    print("[LOI] Khong tim thay xclip / xsel / wl-paste.", file=sys.stderr)
    sys.exit(1)


def countdown(seconds):
    for s in range(int(seconds), 0, -1):
        print("  Bat dau sau %ds... (click vao cua so remote)" % s,
              end="\r", flush=True)
        time.sleep(1)
    print(" " * 55, end="\r")


# ------------------------- CHON CHE DO -------------------------

def _choose_tk():
    """3 nut dung thu tu bang tkinter; nut X = thoat an toan.
    Tra ve 'easy' / 'hard' / None, hoac 'no-tk' neu khong dung duoc tkinter."""
    try:
        import tkinter as tk
        result = {"v": None}
        root = tk.Tk()
        root.title("VM TurboTyper")
        root.resizable(False, False)
        tk.Label(root, text="Chon nha mang:", padx=24, pady=16).pack()
        bar = tk.Frame(root, padx=16, pady=16)
        bar.pack()

        def pick(v):
            result["v"] = v
            root.destroy()

        # Thu tu tu trai sang phai: Viettel -> VNPT -> Thoat
        tk.Button(bar, text="1 - Viettel", width=12, height=2,
                  command=lambda: pick("easy")).pack(side="left", padx=6)
        tk.Button(bar, text="2 - VNPT", width=12, height=2,
                  command=lambda: pick("hard")).pack(side="left", padx=6)
        tk.Button(bar, text="Thoat", width=12, height=2,
                  command=lambda: pick(None)).pack(side="left", padx=6)

        root.protocol("WM_DELETE_WINDOW", lambda: pick(None))  # nut X = thoat
        root.update_idletasks()
        w, h = root.winfo_width(), root.winfo_height()
        root.geometry("+%d+%d" % ((root.winfo_screenwidth() - w) // 2,
                                  (root.winfo_screenheight() - h) // 3))
        root.mainloop()
        return result["v"]
    except Exception:
        return "no-tk"


def choose_mode():
    """Hien cua so chon che do; tra ve 'hard' / 'easy' / None (thoat).
    Viettel = Code DE (go nhanh) ; VNPT = Code KHO (kiem tra tieng Viet)."""
    r = _choose_tk()
    if r != "no-tk":
        return r

    # Khong co tkinter -> zenity dang list (nut X van thoat an toan)
    if have("zenity"):
        out = subprocess.run(
            ["zenity", "--list", "--title", "VM TurboTyper",
             "--text", "Chon nha mang:", "--column", "Nha mang",
             "--ok-label", "Chon", "--cancel-label", "Thoat",
             "--width", "300", "--height", "220",
             "1 - Viettel", "2 - VNPT"],
            capture_output=True, text=True).stdout.strip()
        if out.startswith("1"):
            return "easy"
        if out.startswith("2"):
            return "hard"
        return None

    # Fallback cuoi: hoi trong terminal
    while True:
        c = input("Chon:  [1] Viettel (DE)   [2] VNPT (KHO)   (q = thoat): ") \
            .strip().lower()
        if c == "1":
            return "easy"
        if c == "2":
            return "hard"
        if c in ("q", "quit", "exit"):
            return None


# ------------------------- CODE DE -------------------------

def run_easy():
    text = read_clipboard()
    if not text.strip():
        print("[LOI] Clipboard rong.", file=sys.stderr)
        return
    print("Che do DE - se go %d ky tu. (Ctrl+C de dung)" % len(text))
    countdown(EASY_WAIT)
    try:
        for ch in text:
            if ch == "\n":
                subprocess.run(["xdotool", "key", "Return"])
            elif ch == "\t":
                subprocess.run(["xdotool", "key", "Tab"])
            else:
                subprocess.run(["xdotool", "type", "--delay",
                                str(EASY_DELAY_MS), "--", ch])
    except KeyboardInterrupt:
        print("\n[DUNG] Ctrl+C.")
        return
    print("Hoan tat.")


# ------------------------- CODE KHO -------------------------

def _is_vn(s):
    s = (s or "").lower()
    return any(m in s for m in VN_MARKERS)


def detect_vietnamese():
    if have("fcitx5-remote"):
        active = _out(["fcitx5-remote"])          # "1"=off, "2"=on
        name = _out(["fcitx5-remote", "-n"])
        return "fcitx5 [%s] trang_thai=%s" % (name or "?", active or "?"), \
               (active == "2" or _is_vn(name))
    if have("fcitx-remote"):
        active = _out(["fcitx-remote"])
        return "fcitx trang_thai=%s" % (active or "?"), active == "2"
    if have("ibus"):
        eng = _out(["ibus", "engine"])
        vn = (bool(eng) and not eng.startswith("xkb:")) or _is_vn(eng)
        return "ibus [%s]" % (eng or "?"), vn
    return None, False


def warn_and_exit(msg):
    print("[LOI] " + msg, file=sys.stderr)
    if have("zenity"):
        subprocess.run(["zenity", "--error", "--title", "VM TurboTyper",
                        "--text", msg])
    sys.exit(1)


def mouse_in_corner():
    out = _out(["xdotool", "getmouselocation", "--shell"])
    d = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            d[k] = v
    try:
        return int(d.get("X", "9999")) < 50 and int(d.get("Y", "9999")) < 50
    except ValueError:
        return False


def char_to_keysym(ch):
    if ch in SPECIAL:
        return SPECIAL[ch]
    if ch.isalnum() and ord(ch) < 128:
        return ch
    return "U%04X" % ord(ch)


def run_hard():
    text = read_clipboard()
    for a, b in SMART.items():
        text = text.replace(a, b)
    if not text.strip():
        print("[LOI] Clipboard rong.", file=sys.stderr)
        return

    print("Che do KHO - se go %d ky tu." % len(text))
    print("DUNG KHAN CAP: Ctrl+C" +
          (", hoac dua chuot len goc TREN-TRAI." if HARD_STOP_CORNER else "."))
    countdown(HARD_WAIT)

    buf = []

    def flush():
        if buf:
            subprocess.run(["xdotool", "key", "--clearmodifiers",
                            "--delay", str(HARD_DELAY_MS), *buf])
            buf.clear()

    try:
        for ch in text:
            if ch in ("\n", "\r"):
                flush()
                subprocess.run(["xdotool", "key", "--clearmodifiers", "Return"])
                time.sleep(HARD_NEWLINE_PAUSE)
                continue
            buf.append("Tab" if ch == "\t" else char_to_keysym(ch))
            if len(buf) >= HARD_CHUNK:
                flush()
                if HARD_STOP_CORNER and mouse_in_corner():
                    print("\n[DUNG] Chuot o goc tren-trai.")
                    return
        flush()
    except KeyboardInterrupt:
        print("\n[DUNG] Ctrl+C.")
        return
    print("Hoan tat.")


# ------------------------- MAIN -------------------------

def main():
    acquire_single_instance()

    if not have("xdotool"):
        print("[LOI] Chua cai xdotool. Cai: sudo apt install xdotool",
              file=sys.stderr)
        sys.exit(1)

    mode = choose_mode()
    if mode is None:
        print("Da huy.")
        return
    print("Da chon: %s" % ("VNPT - Code KHO" if mode == "hard"
                           else "Viettel - Code DE"))

    # Kiem tra bo go tieng Viet - ap dung cho CA 2 che do
    if CHECK_VIETNAMESE:
        desc, vn = detect_vietnamese()
        if desc:
            print("Bo go hien tai: " + desc)
        if vn:
            warn_and_exit(
                "Dang BAT bo go tieng Viet (%s).\n\n"
                "Code chi go dung khi o che do English/US.\n"
                "Hay tat Telex/VNI/Unikey roi chay lai." % (desc or "?"))

    if mode == "hard":
        run_hard()
    else:
        run_easy()


if __name__ == "__main__":
    main()
##
