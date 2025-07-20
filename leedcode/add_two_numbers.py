def add2_numbers(l1:list,l2:list):
    sum_list = []
    if len(l1) == len(l2):
        for i in range(len(l1)):
            sum_list.append(l1[i]+l2[i])
    return sum_list


print(add2_numbers([2,4,3],[5,6,4]))



