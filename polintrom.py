# Проверяем вводимый текст на палиндром.

str = input("Введите текст: ")
str = str.replace(" ", "")
str1 = str[::-1]
if str == str1:
    print("Это палиндром!")
else:
    print("Это не палиндром!")
