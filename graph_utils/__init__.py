import pandas as pd


class Node:
    def __init__(self, exchange: str, token: str, total: float):
        self.exchange = exchange
        self.token = token
        self.total = total

    def apply_transform(self, edge):
        new_exchange = edge["exchange_to"]
        new_token = edge["token_to"]
        value = self.total * edge["trading_fee"] - edge["blockchain_fee"]
        return Node(new_exchange, new_token, value)


class Graph:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_edges(self, node: Node) -> pd.DataFrame:
        edges = self.df[(self.df["exchange_from"] == node.exchange) & \
                        (self.df["token_from"] == node.token)]
        return edges

    def find_edge(self, from_: Node, to_: Node) -> pd.DataFrame:
        edge = self.df[(self.df["exchange_from"] == from_.exchange) & \
                       (self.df["token_from"] == from_.token) & \
                       (self.df["exchange_to"] == to_.exchange) & \
                       (self.df["token_to"] == to_.token)
                       ].iloc[0]
        return edge

    def get_all_nodes(self) -> pd.DataFrame:
        from_pairs = self.df.loc[:, ["exchange_from", "token_from"]].drop_duplicates().values
        output = pd.DataFrame(columns=["exchange", "token", "total"])
        for (exchange, token) in from_pairs:
            output.loc[len(output.index)] = [exchange, token, None]

        to_pairs = self.df.loc[:, ["exchange_to", "token_to"]].drop_duplicates().values
        for (exchange, token) in to_pairs:
            if (exchange, token) in from_pairs:
                continue
            output.loc[len(output.index)] = [exchange, token, None]
        return output
