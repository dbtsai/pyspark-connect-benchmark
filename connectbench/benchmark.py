"""Unified Spark Connect construction, API, protocol and return benchmarks."""
import sys


def main():
    from .ferrum import main as run
    from .catalog import cases
    return run(catalog=cases)


if __name__ == '__main__':
    sys.exit(main())
