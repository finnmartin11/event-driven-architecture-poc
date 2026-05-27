import argparse

from metrics.latency_report import LatencyReport
from notify.worker import main as run_notify
from polling.worker import main as run_polling
from utilities import create_db_engine

STRATEGIES = {
    "polling": run_polling,
    "notify": run_notify,
}


def main():
    parser = argparse.ArgumentParser(description="Event-Driven Architecture POC")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a processing strategy")
    run_parser.add_argument("strategy", choices=STRATEGIES, help="Processing strategy to run")

    report_parser = subparsers.add_parser("report", help="Print latency report")
    report_parser.add_argument("strategies", nargs="*", choices=[*STRATEGIES, []], default=None, help="Strategies to report on (default: all)")

    args = parser.parse_args()

    if args.command == "run":
        STRATEGIES[args.strategy]()
    elif args.command == "report":
        from dotenv import load_dotenv

        load_dotenv()
        engine = create_db_engine()
        report = LatencyReport(engine)
        report.print_report(args.strategies or None)


if __name__ == "__main__":
    main()
