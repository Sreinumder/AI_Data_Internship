def print_table(number: int) -> None:
    """prints multiplication table for any integer number"""
    print(f"Multiplication table for {number}:")
    for i in range(1, 11):
        print(f"{number} x {i} = {number * i}")


try:
    table_number = int(input("Enter a number for multiplication table: "))
    print_table(table_number)
except ValueError:
    print("Invalid number. Please enter an integer.")