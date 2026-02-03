import pandas as pd

df = pd.read_excel("P_Data_Extract_From_Global_Economic_Monitor.xlsx")
# Сохраняем в CSV
df.to_csv("economic_data.csv", index=False)