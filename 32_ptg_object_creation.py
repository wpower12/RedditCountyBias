import pandas as pd
import torch
from torch_geometric.data import Data

X_FN   = "data/COVID_US_ST_DataSet/X_features.csv"
Y_FN   = "data/COVID_US_ST_DataSet/Y_targets.csv"
COO_FN = "data/COVID_US_ST_DataSet/coo_list.csv"

with open(X_FN, "r") as x_f:
	x_df = pd.read_csv(x_f)

with open(Y_FN, "r") as y_f:
	y_df = pd.read_csv(y_f)

with open(COO_FN, "r") as coo_f:
	coo_df = pd.read_csv(coo_f)

x_t   = torch.tensor(x_df.values)
y_t   = torch.tensor(y_df.values)
coo_t = torch.tensor(coo_df.values)

covid_stn_2020 = Data(x=x_t, y=y_t, edge_index=coo_t)
