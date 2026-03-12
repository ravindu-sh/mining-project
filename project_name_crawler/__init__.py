import requests
from bs4 import BeautifulSoup as bs

links = [
    "https://arizonagoldsilver.com/",
    "https://afrnuventure.com/",
    "https://a2gold.com/",
]

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}


class Node:
    def __init__(self, text, url):
        self.text = text
        self.url = url
        self.children = []


def build_tree(url, depth=3, visited=None):
    if visited is None:
        visited = set()
    if depth == 0 or url in visited:
        return None
    visited.add(url)
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
    except Exception as e:
        print(e)
        return None
    soup = bs(resp.text, "html.parser")
    root = Node(soup.title.string if soup.title else url, url)
    for a in soup.find_all("a", href=True):
        link = str(a["href"])
        if not link.startswith(url):
            continue
        child = build_tree(link, depth - 1, visited)
        if not child:
            continue
        child.text = a.get_text(strip=True) or link
        root.children.append(child)
    return root


def print_tree(node, indent=0):
    if not node:
        return
    print("  " * indent + f"{node.text} ({node.url})")
    for child in node.children:
        print_tree(child, indent + 1)


# Example usage:
homepage = "https://a2gold.com/"
tree = build_tree(homepage, depth=3)
print_tree(tree)
