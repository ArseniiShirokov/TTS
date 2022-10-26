import pandas as pd


class Node:
    def __init__(self, exchange: str, token: str, total: float):
        self.exchange = exchange
        self.token = token
        self.total = total

    def apply_transform(self, edge):
        new_exchange = edge["exchange_to"]
        new_token = edge["token"]
        # Inverse edges to reduce NP maximization to minimization problem
        value = self.total * edge["trading_fee"] + edge["blockchain_fee"]
        return Node(new_exchange, new_token, value)


class Graph:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_edges(self, node: Node) -> pd.DataFrame:
        edges = self.df[(self.df["exchange_from"] == node.exchange) & \
                        (self.df["token_from"] == node.token)]
        return edges

    def get_all_nodes(self) -> pd.DataFrame:
        pairs = self.df.loc[:, ["exchange_from", "token_from"]].drop_duplicates().values
        output = pd.DataFrame(columns=["exchange", "token", "total"])
        for (exchange, token) in pairs:
            output.iloc[len(output.index)] = [exchange, token, None]
        return output
