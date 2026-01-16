import pandas as pd

path = '/Users/geraldchai/Documents/02-Work(interns:projects:cvs)/2026 Fall/面试/雁丰/20251209_行情数据/600048.SH/逐笔成交.csv'
df = pd.read_csv(path, encoding="gbk")

# 看列 index
print("=== columns ===")
for i, c in enumerate(df.columns):
    print(i, repr(c))

print("\n=== rows 12188-12191 ===")
print(df.iloc[12188:12192])
