def largest_number(numbers: list[int]) -> int:
    if not numbers:
        raise ValueError("List must not be empty.")

    largest = numbers[0]
    for num in numbers[1:]:
        if num > largest:
            largest = num
    return largest


sample_numbers = [12, 45, 7, 89, 23]
print("Sample list:", sample_numbers)
print("Largest number:", largest_number(sample_numbers))