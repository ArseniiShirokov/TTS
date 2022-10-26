import pandas as pd
from graph_utils import Graph, Node


def change_total(df: pd.DataFrame, node: Node, new_total: float) -> None:
    df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["total"]] = new_total


def get_total(df: pd.DataFrame, exchange: str, token: str) -> float:
    return df.loc[(df["exchange"] == exchange) & (df['token'] == token), ["total"]]


def compute_totals(graph: Graph, start: Node) -> pd.DataFrame:
    outputs = graph.get_all_nodes()
    # Init start node
    change_total(outputs, start, -start.total)
    # Simple Ford-Bellman
    while True:
        changes = False
        for index, row in outputs.iterrows():
            node = Node(exchange=row['exchange'], token=row["token"], total=row["total"])
            if node.total is not None:
                for edge in graph.get_edges(node):
                    new_node = node.apply_transform(edge)
                    if new_node.total < get_total(outputs, new_node.exchange, new_node.token):
                        changes = True
                        change_total(outputs, new_node, new_node.total)
        if not changes:
            break
    return outputs


if __name__ == '__main__':
    graph = Graph(pd.read_csv("data/graph.csv"))
    results = compute_totals(graph)
    results.to_csv("outputs/result.csv")
