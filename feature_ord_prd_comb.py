from itertools import combinations
import pandas as pd
import math
import csv


def rSubset(arr,r):
    return list(combinations(arr,r))

if __name__=="__main__":
    order_product_combinations=pd.read_csv("sampled_10k_users/order_product_combinations.csv")
    df = pd.DataFrame()
    val=dict()
    ord=dict()
    for i in order_product_combinations.index:
        l=[]
        l.extend((order_product_combinations.get_value(i,'product_combn')).split(','))
        if len(l)>1:
            for j in rSubset(sorted(l),2):
                if j in val and j in ord:
                    val[j]=val.get(j) + 1
                else:
                    val[j]=1
                    ord[j]=order_product_combinations.loc[i,'order_id']
    with open('sampled_10k_users/dict_key.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in val.items():
            writer.writerow([key[0],key[1],value])
    with open('sampled_10k_users/dict_key_ord.csv', 'w') as csv_file1:
        writer1 = csv.writer(csv_file1)
        for key, value in ord.items():
            writer1.writerow([key[0],key[1],value])

