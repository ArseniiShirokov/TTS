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


def restore_path(start: Node, parents: pd.DataFrame) -> list:
    cur_node = deepcopy(start)
    parent_node = get_parent(parents, cur_node)
    path = []
    while parent_node is not None and len(path) < 20:
        path.append(cur_node)
        if equal(start, parent_node):
            path.append(parent_node)
            break
        cur_node = parent_node
        parent_node = get_parent(parents, cur_node)
    return path[::-1]


def return_cycles(outputs: pd.DataFrame, graph: Graph, parents: pd.DataFrame):
    out = list()
    for i, _ in outputs.iterrows():
        raw = outputs.loc[i]
        node = Node(raw['exchange'], raw['token'], 0.0)
        if raw["total"] != "no path":
            path = restore_path(node, parents)
            if len(path) > 0 and equal(path[0], path[-1]):
                item = dict()
                item["cycle_percent"] = compute_percent(graph, path)
                item["cycle"] = path
                out.append(item)
    return out


def main():
    graph = Graph(pd.read_csv("data/graph.csv"))
    start = Node("binance", "USDT", 100)
    results, parents = compute_totals(graph, start)
    results.to_csv("outputs/result.csv")

    output = return_cycles(results, graph, parents)
    best_chain = None
    best_percent = None
    for i, cycle in enumerate(output):
        # In cycle
        nodes = graph.get_parent_nodes(cycle["cycle"][0])
        total_in = None
        for node in nodes:
            if node.token == start.token and node.exchange == start.exchange:
                edge = graph.find_edge(start, cycle["cycle"][0])
                total_in = start.apply_transform(edge).total
        if total_in is None:
            continue
        # One cycle run
        total_finish = total_in * cycle["cycle_percent"]
        # Out to start
        node = cycle["cycle"][0]
        node.total = total_finish
        try:
            forward_edge = graph.find_edge(node, start)
            total_finish = node.apply_transform(forward_edge).total
        except:
            continue

        total_percent = total_finish / start.total
        if best_percent is None or total_percent > best_percent:
            best_percent = total_percent
            cycle['chain'] = [start] + cycle['cycle'] + [start]
            cycle["chain_str"] = [[val.exchange, val.token] for val in cycle["chain"]]
            cycle["profit_percent"] = total_percent
            cycle["profit_diff"] = total_finish - start.total
            best_chain = cycle
    pprint(best_chain)


if __name__ == '__main__':
    main()
