"""
Entry point for the Redis-Python server.

Delegates entirely to :mod:`app.server`.
"""

from .server import main

if __name__ == "__main__":
    main()
