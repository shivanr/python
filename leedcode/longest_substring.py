#Longest Substring Without Repeating Characters

def longest_substring(input: str):
    n: int = len(input)
    sub_str = []
    ml = 0

    for i in range(n):
        if input[i] not in sub_str:
            sub_str.append(input[i])
        else:
            while input[i] in sub_str:
                sub_str.remove(input[i])
            sub_str.append(input[i])
    return sub_str


print(longest_substring("aaaaaabsjkjasjakjs"))
