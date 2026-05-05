from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

try:
    with (BASE_DIR / "tasks.txt").open(mode="r") as f:
        file_string = f.read()
        words = file_string.split()
        print(words)
        print("words count = ", len(words))
except Exception as err:
    print("exception occurred: ", err)
