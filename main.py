import argparse

from notify.worker import main as run_notify
from polling.worker import main as run_polling

STRATEGIES = {
    "polling": run_polling,
    "notify": run_notify,
}


def main():
    parser = argparse.ArgumentParser(description="Event-Driven Architecture POC")
    parser.add_argument("strategy", choices=STRATEGIES, help="Processing strategy to run")
    args = parser.parse_args()

    STRATEGIES[args.strategy]()


if __name__ == "__main__":
    main()
