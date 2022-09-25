# Проверка правильности  расположения скобок

checks = []
open_staples = ["(", "[", "{"]
close_staples = [")", "]", "}"]

str = input("Введите скобки на проверку: ")

for c in str:
    if c in open_staples:
        checks.append(c)
    elif c in close_staples:
        checks.append(c)
        pos = close_staples.index(c)
        open_staple = open_staples[pos]
        if len(checks) > 0 and open_staple == checks[len(checks) - 2]:
            checks.pop()
            checks.pop()
        else:
            break
if len(checks) > 0:
    print("Ошибка в расположении скобок!")
else:
    print("Ошибок нет!")
