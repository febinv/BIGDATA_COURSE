import pandas as pd

df_order_products = pd.read_csv("sampled_order_products.csv")
df_orders = pd.read_csv('sampled_orders.csv')

df_userprod_joined = df_order_products.merge(df_orders, how='inner',on='order_id')

df_userprod_joined.drop(['eval_set','order_dow','order_hour_of_day','add_to_cart_order'],axis=1,inplace=True)

#len(df_userprod_joined)

#len(df_userprod_joined.loc[df_userprod_joined['user_id'].isin(range(100))])

#Remove
#df_userprod_joined=df_userprod_joined.loc[df_userprod_joined['user_id'].isin(range(100))]

df_user_prod_combs = df_userprod_joined.loc[:,['user_id','product_id']].drop_duplicates()
df_order_prod_combs = df_userprod_joined.loc[:,['order_id','product_id','reordered']].drop_duplicates()
df_user_order_combs = df_userprod_joined.loc[:,['user_id','order_id','order_number','days_since_prior_order']].drop_duplicates()

df_a = df_user_prod_combs.merge(df_user_order_combs,how='inner')

df_order_prod_combs['exists'] = 1

df_final = df_a.merge(df_order_prod_combs,how='left',on=['order_id','product_id']).fillna(0)

#df_final.loc[(df_final.user_id==62)&(df_final.product_id==24852)].sort_values(by=['user_id','product_id','order_number'])

li = []
for u in df_final.user_id.unique():
    for p in df_final.loc[df_final.user_id==u].product_id.unique():
        max_streak = 0
        streak = 0
        days_last = 0
        last_streak = 0
        for o in df_final.loc[(df_final.user_id==u),['order_id','order_number']].sort_values(by='order_number').order_id.unique():
            print(o)
            if df_final.loc[(df_final.product_id==p)&(df_final.order_id==o)].exists.values==1:
                streak +=1
                days_last = 0
            else:
                streak = 0
                days_last += df_final.loc[(df_final.product_id==p)&(df_final.order_id==o)].days_since_prior_order.values[0]
            if streak > max_streak:
                max_streak = streak

        last_streak = streak

        li.append([u, p, o, last_streak, max_streak, days_last])

        print('User '+str(u))

df_result = pd.DataFrame(li, columns=['user_id', 'product_id', 'order_ID', 'usrprod_last_streak','usrprod_max_streak','days_last'])
df_result.to_csv('sample_user_prod_features.csv')