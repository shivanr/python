def reverse_int(i:int):
    rev_num = 0
    num = i
    while num != 0:
        dig = num % 10
        rev_num = rev_num *10 + dig
        num = num // 10
    return rev_num


print(reverse_int(1231234566787))
