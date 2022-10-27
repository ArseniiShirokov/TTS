import pandas as pd

from enum import Enum


class Token(Enum):
    USDT = 1
    BTC = 2
    ETH = 3


def token_from_str(line):
    token = line.lower()
    if token == 'usdt':
        return Token.USDT
    elif token == 'eth':
        return Token.ETH
    elif token == 'btc':
        return Token.ETH


class Exchange(Enum):
    BINANCE = 1
    BYBIT = 2


def exchange_from_str(line):
    exch = line.lower()
    if exch == 'binance':
        return Exchange.BINANCE
    elif exch == 'bybit':
        return Exchange.BYBIT


class BlockchainName(Enum):
    NONE = 1
    BEP20 = 2
    ERC20 = 3
    TRC20 = 4


def blockchain_from_str(line):
    bc = line.lower()
    if bc == 'none':
        return BlockchainName.NONE
    elif bc == 'bep20':
        return BlockchainName.BEP20
    elif bc == 'erc20':
        return BlockchainName.ERC20
    elif bc == 'trc20':
        return BlockchainName.TRC20


class Node:
    exchange: Exchange
    token: Token
    money: float

    def __init__(self, exchange, token):
        self.money = 0
        self.exchange = exchange
        self.token = token

    def __str__(self):
        return str(self.exchange) + '-' + str(self.token)


class Edge:
    # selling currency_from to buy currency_to spending trading_fee * currency_from - blockchain_fee money
    token_from: Node
    token_to: Node
    blockchain_name: BlockchainName
    trading_fee: float
    blockchain_fee: float

    def __init__(self, node_from, node_to, blockchain_name, trading_fee, blockchain_fee):
        self.node_from = node_from
        self.node_to = node_to
        self.blockchain_name = blockchain_name
        self.trading_fee = trading_fee
        self.blockchain_fee = blockchain_fee

    def __str__(self):
        return f'From: {str(self.node_from)}\n' \
               f'To: {str(self.node_to)}\n' \
               f'Blockchain: {str(self.blockchain_name)}\n' \
               f'Trading fee: {str(self.trading_fee)}\n' \
               f'Blockchain fee: {str(self.blockchain_fee)}'


class Graph:
    edges: [Edge]
    nodes: [Node]

    def __str__(self):
        endl = '\n###\n'
        return f'Nodes:\n{" ".join([str(i) for i in self.nodes])}\n' \
               f'Edges:\n{endl.join([str(i) for i in self.edges])}'


def get_nodes(data):
    nodes = {}  # {Node.str -> Node}
    for i in data:
        row = data[i]

        exchange_from = exchange_from_str(row['exchange_from'])
        token_from = token_from_str(row['token_from'])
        exchange_to = exchange_from_str(row['exchange_to'])
        token_to = token_from_str(row['token_to'])

        box = Node(exchange_from, token_from)
        nodes[str(box)] = box

        box = Node(exchange_to, token_to)
        nodes[str(box)] = box
    return nodes


def get_edges(data, nodes):
    edges = []  # [Edge]
    for i in data:
        row = data[i]

        exchange_from = exchange_from_str(row['exchange_from'])
        token_from = token_from_str(row['token_from'])
        node_from = Node(exchange_from, token_from)

        exchange_to = exchange_from_str(row['exchange_to'])
        token_to = token_from_str(row['token_to'])
        node_to = Node(exchange_to, token_to)

        blockchain_name = token_from_str(row['blockchain_name'])
        trading_fee = float(row['trading_fee'])
        blockchain_fee = float(row['blockchain_fee'])

        edge = Edge(nodes[str(node_from)], nodes[str(node_to)], blockchain_name, trading_fee, blockchain_fee)
        edges.append(edge)
    return edges


def load_graph_from_csv(path):
    data = pd.read_csv(path).T.to_dict()
    graph = Graph()
    graph.nodes = get_nodes(data)
    graph.edges = get_edges(data, graph.nodes)
    return graph



