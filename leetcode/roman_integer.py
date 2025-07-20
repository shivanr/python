def roman_int(input: str) -> int:
    m = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }

    res = 0

    for i in range(len(input)):
        if i < len(input) - 1 and m[input[i]] < m[input[i + 1]]:
            res -= m[input[i]]
        else:
            res += m[input[i]]

    return res

print(roman_int("MCMXCIV"))
