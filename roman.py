# Перевод арабских чисел в римские

roman_numbers = [
    (1, "I"),
    (2, "II"),
    (3, "III"),
    (4, "IV"),
    (5, "V"),
    (9, "IX"),
    (10, "X"),
    (40, "XL"),
    (50, "L"),
    (90, "XC"),
    (100, "C"),
    (400, "CD"),
    (500, "D"),
    (900, "CM"),
    (1000, "M"),
]

str = input("Введите число: ")
num = int(str)
roman = ""
roman_numbers.reverse()
while num != 0:
    for el in roman_numbers:
        if num - el[0] >= 0:
            roman += el[1]
            num -= el[0]
            break

print("Римское число: ", roman)
