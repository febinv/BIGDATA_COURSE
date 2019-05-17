from itertools import combinations
import pandas as pd
import math


def rSubset(arr,r):
    return list(combinations(arr,r))

if __name__=="__main__":
    order_product_combinations=pd.read_csv("order_product_combinations2.csv")
    df = pd.DataFrame()
    for i in order_product_combinations.index:
        print(i)
        l=[]
        l.extend((order_product_combinations.get_value(i,'product_combinations')).split(','))
        if len(l)>1:
            df = df.append(rSubset(sorted(l),2))
    df.to_csv("final_combo2.csv",encoding='utf-8',index=False)

