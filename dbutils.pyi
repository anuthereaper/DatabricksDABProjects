
# This is a type stub for local linting only.

class FileInfo:
    path: str
    name: str
    size: int

class DBFSUtils:
    def ls(self, path: str) -> list[FileInfo]: ...
    def cp(self, src: str, dst: str, recurse: bool = False): ...
    def rm(self, path: str, recurse: bool = False): ...

class SecretsUtils:
    def get(self, scope: str, key: str) -> str: ...

class WidgetsUtils:
    def get(self, name: str) -> str: ...
    def text(self, name: str, defaultValue: str, label: str = ""): ...

class DbUtils:
    dbutils: "DbUtils" = None
    fs: DBFSUtils
    secrets: SecretsUtils
    widgets: WidgetsUtils

dbutils = DbUtils()
