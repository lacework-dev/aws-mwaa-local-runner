import re


class ColumnSpec:
    def __init__(self, name: str, col_type: str = "", comment: str = "") -> None:
        self.name = sanitize_column_name(name)
        self.col_type = sanitize_column_type(col_type)
        self.comment = sanitize_comment(comment)

    @property
    def clause(self) -> str:
        assert self.col_type != "", "Type required to generate schema clause"
        comment = f"COMMENT '{self.comment}'" if self.comment else ""
        return f"{self.name} {self.col_type} {comment}".strip()


def sanitize_column_name(name: str) -> str:
    """
    Throws an error if the column name does not match Snowflake's expected format.
    """
    assert (re.match(r"^\w+$", name)), f"Invalid characters in column name: {name}"
    assert (re.match(r"^[A-Za-z_].*", name)), f"Column name cannot start with a number: {name}"
    return name


def sanitize_column_type(type_str: str) -> str:
    """
    Throws an error if there are invalid characters or unbalanced parentheses.
    CREATE TABLE statements are enclosed in parentheses, so unbalanced parentheses
    may cause unintended consequences in the query.
    """
    if type_str == "":
        return type_str

    assert (re.match(r"^[\w\(\), ]+$", type_str)), f"Invalid characters in column type: {type_str}"

    open_count = 0
    for c in type_str:
        if c == "(":
            open_count += 1
        elif c == ")":
            assert (open_count > 0), f"Unbalanced parentheses in column type: {type_str}"
            open_count -= 1

    assert (open_count == 0), f"Unbalanced parentheses in column type: {type_str}"
    return type_str


def sanitize_comment(comment: str) -> str:
    """
    Escapes any single quotes found in the comment string to avoid any unintended
    consequences in the CREATE TABLE query.
    """
    return comment.replace("'", "\\'")