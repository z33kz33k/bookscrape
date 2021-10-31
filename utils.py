"""

    utils.py
    ~~~~~~~~~~~~
    Script's utilities.

    @author: z33k

"""


def non_ascii_index(text: str) -> int:
    """Returm index of the first non-ASCII character in ``text` or ``-1``.
    """
    for i, char in enumerate(text):
        if ord(char) not in range(128):
            return i
    return -1

