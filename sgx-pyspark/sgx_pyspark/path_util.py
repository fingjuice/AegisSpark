"""HDFS URI 路径规范化与授权前缀匹配（与 ABE-Spark PathUtil 语义对齐）。"""

from __future__ import annotations


def normalize(hdfs_uri: str) -> str:
    uri = hdfs_uri.strip()
    if not uri.startswith("hdfs://"):
        return uri.rstrip("/")
    scheme_end = uri.find("/", 7)
    if scheme_end < 0:
        return uri.rstrip("/")
    path = uri[scheme_end:]
    while "//" in path:
        path = path.replace("//", "/")
    return uri[:scheme_end] + path.rstrip("/")


def is_under_root(target: str, root: str) -> bool:
    t = normalize(target)
    r = normalize(root)
    if not r:
        return False
    if t == r:
        return True
    prefix = r if r.endswith("/") else r + "/"
    return t.startswith(prefix)


def is_under_any_root(target: str, roots: list[str]) -> bool:
    return any(is_under_root(target, root) for root in roots)


def ticket_binds_target(ticket_path: str, write_target: str) -> bool:
    t = normalize(ticket_path)
    w = normalize(write_target)
    if not t or not w:
        return False
    return t == w or is_under_root(w, t)
