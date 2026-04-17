try:
    with open("tasks.txt",  mode="r") as f:
        file_string = f.read()
        words = file_string.split()
        print(words)
        print("words count = ", len(words))
except Exception as err:
    print("exception occurred: ", err)