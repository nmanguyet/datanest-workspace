#%%
import subprocess
import time

text = subprocess.check_output(
    ["xclip", "-selection", "clipboard", "-o"],
    text=True,
)

print("Switch to remote window...")
time.sleep(5)

for ch in text:
    if ch == "\n":
        subprocess.run(["xdotool", "key", "Return"])
    elif ch == "\t":
       subprocess.run(["xdotool", "key", "Tab"])
    else:
        subprocess.run(["xdotool", "type", "--delay", "10", "--", ch])

# %%

