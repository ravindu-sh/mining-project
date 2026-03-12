from bs4 import BeautifulSoup as bs
from bs4.element import PageElement, Tag, NavigableString


def get_article(
    html: str,
    remove_unnecessary_attributes: bool = True,
    redact_tables: bool = False,
    convert_to_text: bool = False,
    preserved_tags_on_text_convert: set[str] = {
        "h",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "table",
        "b",
        "p",
    },
) -> tuple[str | None, str | None, str | None, str | bs | Tag | None]:
    soup = bs(html, "html.parser")
    for tag in soup.find_all(True):
        if tag.name == "article":
            continue
        tag.attrs = {}
    for i, table in enumerate(soup.find_all("table")):
        table["id"] = f"table_{i}"
    article = soup.find("article", class_="news-release")
    if article is None:
        return None, None, None, None

    header = article.find("header")
    title = header.find("h1") if header else None
    title = title.getText().strip() if title else None
    provider = header.find("a") if header else None
    provider = provider.getText().strip() if provider else None
    published_date = header.find("p") if header else None
    published_date = published_date.getText().strip() if published_date else None

    body = article.find("section")
    if remove_unnecessary_attributes:
        body = _remove_unnecessary_attr(body) if body else None
    if redact_tables:
        body = _tables_to_text(body) if body else None
    if convert_to_text:
        body = _to_text(body, preserved_tags_on_text_convert) if body else None

    return title, provider, published_date, body


def _remove_unnecessary_attr(body: Tag | bs):
    for tag in body.find_all(True):
        if tag.name != "table":
            tag.attrs = {}
            continue
        tag.attrs = {
            key: tag.attrs[key] for key in ("colspan", "rowspan") if tag.has_key(key)
        }
    return body


def _tables_to_text(body: Tag | bs):
    for table in body.find_all("table"):
        table_text = []
        for cell in table.find_all(["td", "th"]):
            cell_text = cell.getText().strip()
            if sum(c.isdigit() for c in cell_text) > len(cell_text) / 2:
                continue
            table_text.append(cell_text)
        table.string = f"Table contents redacted. table contained these text: {', '.join(table_text)}."
    return body


def _to_text(node: Tag | PageElement | bs, preserved_tags: set[str] = set()):
    if isinstance(node, NavigableString):
        return node.strip() + "."
    if isinstance(node, Tag):
        if node.name in preserved_tags:
            return f"<{node.name}>{''.join(_to_text(child, preserved_tags) for child in node.children)}</{node.name}>"
        else:
            return "".join(_to_text(child, preserved_tags) for child in node.children)
    return ""
