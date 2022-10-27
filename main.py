import pandas as pd
from graph_utils import Graph, Node


def change_total(df: pd.DataFrame, node: Node, new_total: float) -> None:
    df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["total"]] = new_total


def get_total(df: pd.DataFrame, exchange: str, token: str) -> float:
    return df.loc[(df["exchange"] == exchange) & (df['token'] == token), ["total"]]


def iterate_by_edges(outputs: pd.DataFrame, graph: Graph):
    # Iterate by nodes
    for index, row in outputs.iterrows():
        node = Node(exchange=row['exchange'], token=row["token"], total=row["total"])
        # Check reachability
        if node.total is not None:
            # Iterate by edges
            for edge in graph.get_edges(node):
                yield node, edge


def compute_totals(graph: Graph, start: Node) -> pd.DataFrame:
    outputs = graph.get_all_nodes()
    # Init start node
    change_total(outputs, start, -start.total)
    # Simple Ford-Bellman
    iter_cnt = 0
    while iter_cnt < len(outputs.index):
        changes = False
        for node, edge in iterate_by_edges(outputs, graph):
            new_node = node.apply_transform(edge)
            cur_total = get_total(outputs, new_node.exchange, new_node.token)
            # Update if better path was founded
            if cur_total is None or new_node.total < cur_total:
                changes = True
                change_total(outputs, new_node, new_node.total)
        if not changes:
            print("Everything is ok, neg cycle doesn't exist")
            break
        iter_cnt += 1
    return outputs


def main():
    graph = Graph(pd.read_csv("data/graph.csv"))
    start = Node("binance", "ton", 100)
    results = compute_totals(graph, start)
    results.to_csv("outputs/result.csv")


if __name__ == '__main__':
    main()
