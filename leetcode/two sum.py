def two_sum(input_array,pair_sum):
    for i in range(len(input_array)-1):
        for j in range(i+1,len(input_array)):
            if input_array[i]+input_array[j]==pair_sum:
                return (input_array[i],input_array[j])


num_arr = [3, 5, 2, -4, 8, 11]
pair_sum2 = 7
print(two_sum(num_arr,pair_sum2))



