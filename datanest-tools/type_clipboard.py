import argparse
import os
import shutil
import subprocess
import sys
import time

TITLE = "VM TurboTyper"

# --- anh xa ky tu dac biet -> ten keysym cua X (giong bang keyMap cua macOS) ---
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

# --- chuan hoa ky tu "thong minh" ve ASCII (giong phan \u201C... cua macOS) ---
SMART = {
    "\u201c": '"', "\u201d": '"',   # " "  -> "
    "\u2018": "'", "\u2019": "'",   # ' '  -> '
    "\u2013": "-", "\u2014": "-",   # - -  -> -
    "\u00a0": " ",                  # non-breaking space -> space
    "\u2026": "...",                # ...  -> ...
}

VN_MARKERS = ("bamboo", "unikey", "telex", "vni", "viqr", "vietnam", "openkey")


def have(cmd):
    return shutil.which(cmd) is not None


def _out(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=3).stdout.strip()
    except Exception:
        return ""


def _is_vn(s):
    s = (s or "").lower()
    return any(m in s for m in VN_MARKERS)


def die(msg, code=1):
    print("[LOI] " + msg, file=sys.stderr)
    if have("zenity"):
        subprocess.run(["zenity", "--error", "--title", TITLE, "--text", msg])
    sys.exit(code)


def read_clipboard():
    """Doc clipboard, thu lan luot xclip -> xsel -> wl-paste."""
    for cmd in (["xclip", "-selection", "clipboard", "-o"],
                ["xsel", "-b", "-o"],
                ["wl-paste", "-n"]):
        if have(cmd[0]):
            try:
                return subprocess.check_output(cmd, text=True)
            except subprocess.CalledProcessError:
                return ""
    die("Khong tim thay xclip / xsel / wl-paste de doc clipboard.")


def detect_ime():
    """Tra ve (mo_ta, co_ve_tieng_viet?). Tuong duong buoc check ABC/US cua macOS."""
    if have("fcitx5-remote"):
        active = _out(["fcitx5-remote"])          # "1"=off, "2"=on
        name = _out(["fcitx5-remote", "-n"])
        desc = "fcitx5 [%s] trang_thai=%s" % (name or "?", active or "?")
        return desc, (active == "2" or _is_vn(name))
    if have("fcitx-remote"):
        active = _out(["fcitx-remote"])
        return "fcitx trang_thai=%s" % (active or "?"), active == "2"
    if have("ibus"):
        eng = _out(["ibus", "engine"])
        # xkb:* la layout thuong (an toan); ten rieng nhu Bamboo la bo go VN
        vn = (bool(eng) and not eng.startswith("xkb:")) or _is_vn(eng)
        return "ibus [%s]" % (eng or "?"), vn
    return None, False


def mouse_in_corner():
    """Dung khan cap: chuot len goc TREN-TRAI (giong isEmergencyStop cua macOS)."""
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
    if ch.isalnum() and ord(ch) < 128:   # a-z A-Z 0-9
        return ch
    return "U%04X" % ord(ch)             # keysym unicode, vd 'd/' -> U0111


def main():
    # allow_abbrev=False: trong Jupyter, sys.argv chua '--f=/run/.../kernel-xxx.json'
    # cua kernel -> neu cho phep viet tat, '--f' se tro thanh '--force' va loi
    # "ignored explicit argument". Tat abbrev de '--f' thanh tham so la.
    ap = argparse.ArgumentParser(description="Go clipboard vao VM/remote.",
                                 allow_abbrev=False)
    ap.add_argument("--wait", type=float, default=4.0,
                    help="So giay cho de ban click vao VM (mac dinh 4).")
    ap.add_argument("--delay", type=int, default=12,
                    help="Do tre giua cac phim, ms (mac dinh 12).")
    ap.add_argument("--force", action="store_true",
                    help="Bo qua canh bao bo go tieng Viet.")
    # parse_known_args: bo qua tham so cua kernel Jupyter (-f kernel-xxx.json)
    args, _unknown = ap.parse_known_args()

    # 0. Kiem tra moi truong
    if os.environ.get("XDG_SESSION_TYPE", "").lower() == "wayland":
        die("Ban dang chay Wayland. xdotool chi hoat dong tren X11.\n"
            "Dang nhap lai chon phien 'Xorg/X11' roi thu lai.")
    if not have("xdotool"):
        die("Chua cai xdotool. Cai: sudo apt install xdotool")

    # 1. Kiem tra bo go (giong buoc check ABC/US cua macOS)
    desc, vn = detect_ime()
    if desc:
        print("Bo go hien tai: " + desc)
    if vn and not args.force:
        die("Phat hien bo go tieng Viet dang BAT (%s).\n"
            "Hay chuyen ve English/US (tat Telex/VNI/Unikey) roi chay lai.\n"
            "Neu chac chan muon tiep tuc, them co --force." % (desc or "?"))

    # 2. Doc clipboard + chuan hoa ky tu thong minh
    text = read_clipboard()
    for a, b in SMART.items():
        text = text.replace(a, b)
    if not text.strip():
        die("Clipboard rong - hay copy noi dung truoc.")
    print("Se go %d ky tu." % len(text))

    # 3. Dem nguoc de click vao cua so VM
    print("DUNG KHAN CAP: nhan Ctrl+C, hoac dua chuot len goc TREN-TRAI man hinh.")
    for s in range(int(args.wait), 0, -1):
        print("  Bat dau sau %ds... (click vao VM ngay)" % s, end="\r", flush=True)
        time.sleep(1)
    print(" " * 55, end="\r")

    # 4. Go theo tung cum, kiem tra dung giua cac cum
    CHUNK = 20
    buf = []

    def flush():
        nonlocal buf
        if buf:
            subprocess.run(["xdotool", "key", "--clearmodifiers",
                            "--delay", str(args.delay), *buf])
            buf = []

    try:
        for ch in text:
            if ch in ("\n", "\r"):
                flush()
                subprocess.run(["xdotool", "key", "--clearmodifiers", "Return"])
                time.sleep(0.02)          # xuong dong can nhieu thoi gian hon
                continue
            if ch == "\t":
                buf.append("Tab")
            else:
                buf.append(char_to_keysym(ch))
            if len(buf) >= CHUNK:
                flush()
                if mouse_in_corner():
                    print("\n[DUNG] Chuot o goc tren-trai.")
                    return
        flush()
    except KeyboardInterrupt:
        print("\n[DUNG] Ctrl+C.")
        return

    print("Hoan tat.")


if __name__ == "__main__":
    main()
