import pandas as pd
from graph_utils import Graph, Node, ShortNode


def change_total(df: pd.DataFrame, node: Node, new_total: float) -> None:
    df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["total"]] = new_total


def get_total(df: pd.DataFrame, exchange: str, token: str) -> float:
    return df.loc[(df["exchange"] == exchange) & (df['token'] == token)].iloc[0]["total"]


def change_parent(df: pd.DataFrame, node: Node, parent: ShortNode):
    exist = len(df[(df["exchange"] == node.exchange) & (df['token'] == node.token)].index) != 0
    if exist:
        df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["exchange_parent"]] = parent.exchange
        df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token), ["token_parent"]] = parent.token
    else:
        df.loc[len(df.index)] = [node.exchange, node.token, parent.exchange, parent.token]


def get_parent(df: pd.DataFrame, node: ShortNode):
    exist = len(df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].index) != 0
    if not exist:
        return None
    token = df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].iloc[0]["token_parent"]
    exchange = df.loc[(df["exchange"] == node.exchange) & (df['token'] == node.token)].iloc[0]["exchange_parent"]
    return ShortNode(exchange, token)


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
    parents = pd.DataFrame(columns=["exchange", "token", "exchange_parent", "token_parent"]) # Node to Node
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
                change_parent(parents, new_node, node)
                changes = True
                change_total(outputs, new_node, new_node.total)
        if not changes:
            print("Everything is ok, neg cycle doesn't exist")
            break
        iter_cnt += 1

    # reverse totals
    outputs['total'] = outputs["total"].apply(lambda x: "no path" if x is None else -x)
    return outputs, parents


def print_chains(outputs: pd.DataFrame, parents: pd.DataFrame, start: Node):
    for i, _ in outputs.iterrows():
        raw = outputs.loc[i]
        path = []
        node = ShortNode(raw['exchange'], raw['token'])
        if raw["total"] != "no path":
            cur_node = node
            parent_node = get_parent(parents, cur_node)
            while parent_node is not None:
                flag = parent_node.exchange == start.exchange and parent_node.token == start.token
                path.append(cur_node)
                if flag:
                    path.append(parent_node)
                    break
                cur_node = parent_node
                parent_node = get_parent(parents, cur_node)

            if len(path) > 0:
                node = ShortNode(raw['exchange'], raw['token'])
                print(f"Chain from <{start.exchange}, {start.token}> to <{node.exchange}, {node.token}>")
                for path_node in path[::-1]:
                    print(f"<{path_node.exchange}, {path_node.token}>")
                print("+++++++++++++")


def main():
    graph = Graph(pd.read_csv("data/graph.csv"))
    start = Node("Binance", "BTC", 100)
    results, parents = compute_totals(graph, start)
    results.to_csv("outputs/result.csv")
    print_chains(results, parents, start)


if __name__ == '__main__':
    main()
