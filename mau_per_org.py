# %%
import pandas as pd
tokens = pd.read_pickle("output/tokens.pkl")
users = pd.read_pickle("output/users.pkl")

# %%
tokens.info()

# %%
tokens['creationDate'] = pd.to_datetime(tokens['creationDate'])

# %%
merged = pd.merge(tokens, users, how="outer", on=["userId"])
merged.head()


# %%
grouped = merged.groupby(by=[pd.Grouper(key="creationDate", freq="M"), merged.orgId, merged.orgName]).nunique()
grouped

# %%
grouped = merged.groupby(by=[pd.Grouper(key="creationDate", freq="M"), merged.orgId, merged.orgName]).nunique()
grouped

# %%
grouped.to_csv('output/mau_per_month_per_org.csv')


