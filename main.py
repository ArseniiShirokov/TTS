from copy import deepcopy
from pprint import pprint
import pandas as pd
from graph_utils import Graph, Node
from tqdm import tqdm


def change_total(df: pd.DataFrame, node: Node) -> None:
    df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["total"]] = node.total


def get_total(df: pd.DataFrame, exchange: str, token: str) -> float:
    return df.loc[(df["exchange"] == exchange) & (df['token'] == token)].iloc[0]["total"]


def change_parent(df: pd.DataFrame, node: Node, parent: Node):
    exist = len(df[(df["exchange"] == node.exchange) & (df['token'] == node.token)].index) != 0
    if exist:
        df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["exchange_parent"]] = parent.exchange
        df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["token_parent"]] = parent.token
    else:
        df.loc[len(df.index)] = [node.exchange, node.token, parent.exchange, parent.token]


def get_parent(df: pd.DataFrame, node: Node):
    exist = len(df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].index) != 0
    if not exist:
        return None
    token = df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].iloc[0]["token_parent"]
    exchange = df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].iloc[0]["exchange_parent"]
    return Node(exchange, token, 0.0)


def iterate_by_edges(outputs: pd.DataFrame, graph: Graph):
    # Iterate by nodes
    for index, row in outputs.iterrows():
        node = Node(exchange=row['exchange'], token=row["token"], total=row["total"])
        # Check reachability
        if node.total is not None:
            # Iterate by edges
            edges = graph.get_edges(node)
            for i, _ in edges.iterrows():
                yield [node, edges.loc[i]]


def compute_totals(graph: Graph, start: Node) -> [pd.DataFrame, dict]:
    outputs = graph.get_all_nodes()
    parents = pd.DataFrame(columns=["exchange", "token", "exchange_parent", "token_parent"])
    # Init start node
    change_total(outputs, start)
    # Simple Ford-Bellman
    for _ in tqdm(range(len(outputs.index))[:5]):
        changes = False
        for node, edge in iterate_by_edges(outputs, graph):
            new_node = node.apply_transform(edge)
            cur_total = get_total(outputs, new_node.exchange, new_node.token)
            # Update if better path was founded
            if cur_total is None or new_node.total > cur_total:
                change_parent(parents, new_node, node)
                changes = True
                change_total(outputs, new_node)
        if not changes:
            print("Everything is ok, neg cycle doesn't exist")
            break

    # format totals
    outputs['total'] = outputs["total"].apply(lambda x: "no path" if x is None else x)
    return outputs, parents


def equal(first: Node, second: Node) -> bool:
    return first.token == second.token and first.exchange == second.exchange


def compute_percent(graph: Graph, path: list) -> float:
    start = path[0]
    start.total = 1000
    prev = start
    for node in path[1:]:
        edge = graph.find_edge(prev, node)
        new_node = prev.apply_transform(edge)
        prev = new_node
    return prev.total / start.total


def return_chains(outputs: pd.DataFrame, graph: Graph, parents: pd.DataFrame):
    out = list()
    for i, _ in outputs.iterrows():
        raw = outputs.loc[i]
        path = []
        node = Node(raw['exchange'], raw['token'], 0.0)
        if raw["total"] != "no path":
            cur_node = deepcopy(node)
            parent_node = get_parent(parents, cur_node)
            while parent_node is not None and not equal(node, parent_node):
                if len(path) > 10:
                    break
                path.append(cur_node)
                cur_node = parent_node
                parent_node = get_parent(parents, cur_node)

            if len(path) > 0 and equal(node, parent_node):
                path.append(cur_node)
                path.append(parent_node)
                item = dict()
                item["percent"] = compute_percent(graph, path[::-1])
                item["chain"] = [[node.exchange, node.token] for node in path]
                out.append(item)
    return out


def main():
    graph = Graph(pd.read_csv("data/graph.csv"))
    start = Node("binance", "USDT", 100)
    results, parents = compute_totals(graph, start)
    results.to_csv("outputs/result.csv")
    pprint(return_chains(results, graph, parents))


if __name__ == '__main__':
    main()
