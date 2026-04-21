def marks_to_grade(marks: int) -> str:
    if marks < 0 or marks > 100:
        raise ValueError("Marks must be between 0 and 100.")

    if marks < 60:
        return "Fail"
    if marks < 70:
        return "C"
    if marks < 80:
        return "D"
    if marks < 90:
        return "B"
    return "A"


try:
    in_marks = int(input("Enter student marks (0-100): "))
    print("Grade:", marks_to_grade(in_marks))
except ValueError as err:
    print("Invalid marks input:", err)