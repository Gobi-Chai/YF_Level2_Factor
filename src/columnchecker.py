import pandas as pd

path = 'Add your path name'
df = pd.read_csv(path, encoding="gbk")

# 看列 index
print("=== columns ===")
for i, c in enumerate(df.columns):
    print(i, repr(c))

print("\n=== rows 12188-12191 ===")
print(df.iloc[12188:12192])
