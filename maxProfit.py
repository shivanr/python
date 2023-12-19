def maxprofit(price:list):
    mn  = 0
    mx  = 1
    max_profit = 0
    while mx < len(price):
        profit = price[mx]-price[mn]
        if price[mn] < price[mx]:
            max_profit = max(max_profit,profit)
            print(max_profit)
        else:
            mn = mx
        mx += 1
    return max_profit
    # max_profit = 0
    # for i in range(len(price)-1):
    #     profit = price[i+1] - price[i]
    #     print(profit)
    #     if profit> max_profit:
    #         max_profit = profit
    #         #print(max_profit)
    # return max_profit



print(maxprofit([7,1,5,3,6,4]))
