from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

pd.set_option('display.max_columns', 100000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', None)
pd.set_option('float_format', '{:f}'.format)

data = pd.read_pickle('data.pckl')
df = data.copy()
df.dropna(inplace=True)
df.reset_index(drop=True, inplace=True)


def data_preview(Dataframe):
    """
    :param Dataframe:
    :return:
    """
    print("------------------------- HEAD -------------------------")
    print(Dataframe.head())

    print("------------------------- Info -------------------------")
    print(Dataframe.info())

    print("------------------------- Shape -------------------------")
    print(Dataframe.shape)

    print("------------------------- Missing values -------------------------")
    print(Dataframe.isnull().sum())

    print("------------------------- Quantiles -------------------------")
    for col in Dataframe.columns:
        if type(Dataframe[col]) in ["int64", "float64"]:
            quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
            print(Dataframe[col].quantile(quantiles))
        else:
            print("col type : ", Dataframe[col].dtype)
    print("------------------------- Unique values -------------------------")
    for col in Dataframe.columns:
        print(f"{col}: {Dataframe[col].nunique()}")


df.info()
data_preview(df)

df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
df['Date'] = df['Timestamp'].dt.date
df["time"] = pd.to_datetime(df["Timestamp"]).dt.time

# dataset exploration

df['Protocol'].groupby(df['Source IP']).count().sort_values(ascending=False).head()
"""
Source IP
147.32.84.165    63929
147.32.84.191    42145
147.32.84.192    29591
58.215.240.7      7642
125.39.78.235     1929
Name: Protocol, dtype: int64
"""

df['Protocol'].groupby([df['Destination IP'], df["Protocol"]]).count().sort_values(ascending=False).head()

"""
Source IP      Protocol
147.32.84.165  DATA        45997
147.32.84.191  DATA        27526
147.32.84.192  DATA        14213
147.32.84.165  TCP         13881
147.32.84.192  TCP         13738
Name: Protocol, dtype: int64
"""

sourceIp = df['Protocol'].groupby([df['Source IP'], df["Protocol"]]).count()
sourceIp = sourceIp.reset_index(name="count").sort_values(by="count", ascending=False)
sourceIp = sourceIp.sort_values(by="count", ascending=False)

df['Protocol'].groupby([df['Destination IP'], df["Protocol"]]).count().sort_values(ascending=False).head()
"""
Destination IP  Protocol
58.215.240.7    TCP         19988
147.32.84.165   DATA        19792
147.32.84.191   DATA        14644
147.32.84.192   DATA        12923
147.32.84.191   TCP          5842
Name: Protocol, dtype: int64
"""

destinationIp = df['Protocol'].groupby([df['Destination IP'], df["Protocol"]]).count()
destinationIp = destinationIp.reset_index(name="count").sort_values(by="count", ascending=False)


df.groupby(["Source IP", "Destination IP", "Protocol"]).count().sort_values(by="time", ascending=False).head()
"""
                                       Timestamp  Info  Date  time
Source IP     Destination IP Protocol                             
147.32.84.165 147.32.84.191  DATA           9554  9554  9554  9554
147.32.84.192 58.215.240.7   TCP            6819  6819  6819  6819
147.32.84.165 58.215.240.7   TCP            6649  6649  6649  6649
147.32.84.191 58.215.240.7   TCP            6520  6520  6520  6520
              147.32.84.192  DATA           4963  4963  4963  4963
"""


def visualIP(dataframe, ip):
    """
    :param dataframe:
    :param ip:
    :return:
    """
    plt.figure(figsize=(15, 6))
    plt.xticks(rotation=20)
    sns.barplot(data=dataframe[:15], x=ip, y="count", hue="Protocol").set_title(ip)
    plt.show()


visualIP(sourceIp, "Source IP")
visualIP(destinationIp, "Destination IP")


def parse_info(info_string):
    parts1 = info_string.split("\r\n\t")
    parts2 = [part.split("\r\n") for part in parts1]
    parts3 = [[subpart.split(":\t") for subpart in part] for part in parts2]
    parts3 = [inf for part in parts3
              for subpart in part
              for inf in subpart
              if len(subpart) > 0]
    parts3 = list(filter(None, parts3))
    return parts3


df["Info"] = df["Info"].apply(parse_info)

def group_by_layer(cell):
    grouped_data = []
    current_group = []
    for line in cell:
        if line.startswith("Layer"):
            if current_group:
                grouped_data.append(current_group)
            current_group = [line]
        else:
            current_group.append(line)
    if current_group:
        grouped_data.append(current_group)
    return grouped_data

df["layer"] = df["Info"].apply(group_by_layer)


def extract_layers(row):
    layers = {"Layer ETH": np.nan, "Layer IP": np.nan, "Layer UDP": np.nan, "Layer NBNS": np.nan}
    for layer in row:
        if layer[0].startswith("Layer"):
            layer_name = layer[0]
            if layer_name in layers:
                layers[layer_name] = ", ".join(layer[1:])
    return layers


layer_data = df["layer"].apply(extract_layers)


layer_df = pd.DataFrame(list(layer_data))
result_df = pd.concat([df, layer_df], axis=1).drop(columns=["layer"])


print(result_df)
result_df.to_csv("botnetData.csv", index=False)
