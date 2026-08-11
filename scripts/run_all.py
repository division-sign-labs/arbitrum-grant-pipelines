"""Staged orchestrator: every pipeline, then its ingestion, in dependency order.

WHAT it produces
    Nothing of its own. It runs `python -m pipelines.<name>` followed by
    `python -m ingestion.<name>` for each pipeline, in the order their inputs
    require, and prints one summary table of stage / step / status / duration.
    Its exit code is 0 only if every step it attempted succeeded.

WHY the stages
    The pipelines are not independent — each stage consumes the completed runs
    of the stage before it, so running them in the wrong order silently produces
    empty CSVs rather than an error:

      A  linked_wallets .......... the fid -> wallet map. Every later join keys
                                   off it, so nothing else can run first.
      B  contract_deployers,      each needs A (wallets to intersect against) or
         miniapp_builders,        a seed file, and nothing else. Order within the
         brand_engagement,        stage does not matter.
         clanker_tokens,
         bankr_tokens
      C  token_buyers,            token_buyers reads the B token registries;
         popular_tokens           popular_tokens reads A's wallet map.
      D  token_evangelists ....... needs C's buys plus B's registries.
      E  arb_cohort,              arb_cohort aggregates every completed run above
         hyperliquid_activity     it; hyperliquid_activity crawls that cohort, so
                                  it must follow arb_cohort within the stage.

WHY subprocesses rather than importing run()
    A pipeline that dies — a Dune table moved, a rate limit, a bad seed — must
    not take the rest of the schedule with it, and several of these runs are
    hours long. A child process gives us a real exit code, isolates the failure,
    and lets the operator Ctrl-C one step without unwinding the parent's state.
    Child stdout/stderr is inherited, not captured, so a seven-hour crawl still
    narrates itself live.

WHAT IT DOES NOT DO
    No retries and no parallelism. Retries belong inside the clients (lib.http
    and lib.dune already have them) where they can tell a rate limit from a
    schema error; parallelism would multiply Dune spend and collide on the
    shared Neynar rate limit for no wall-clock gain on the legs that matter.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
# Run as `python scripts/run_all.py`, sys.path[0] is scripts/, not the repo root.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import SEEDS_DIR  # noqa: E402
from lib.logging_utils import setup_logging  # noqa: E402

logger = logging.getLogger("run_all")

CONSTRAINTS_MODULE = "ingestion.constraints"


@dataclass(frozen=True)
class Step:
    """One pipeline and the ingestion module that loads its run."""

    stage: str
    pipeline: str  # module under pipelines/
    data_type: str  # directory under data/
    ingest: tuple[str, ...]  # candidate ingestion modules; first that exists wins
    ingest_required: bool = True
    extra_args: tuple[str, ...] = field(default_factory=tuple)
    ingest_args: tuple[str, ...] = field(default_factory=tuple)
    seed: str | None = None  # seeds/<seed>.csv this step cannot work without


# Ingestion modules are named ingestion.ingest_<data type>, but the pipeline name
# and the data-type directory diverge in one place (miniapp_builders writes
# miniapp_builders_activity/), so both spellings are offered as candidates and
# the first importable one wins. A missing module is reported rather than raised,
# so a pipeline-only run still works.
PLAN: tuple[Step, ...] = (
    Step("A", "linked_wallets", "linked_wallets", ("ingestion.ingest_linked_wallets",)),
    Step("B", "contract_deployers", "contract_deployers", ("ingestion.ingest_contract_deployers",)),
    Step(
        "B",
        "miniapp_builders",
        "miniapp_builders_activity",
        ("ingestion.ingest_miniapp_builders", "ingestion.ingest_miniapp_builders_activity"),
        seed="miniapp_builders",
    ),
    Step(
        "B",
        "brand_engagement",
        "brand_engagement",
        ("ingestion.ingest_brand_engagement",),
        seed="brand_accounts",
    ),
    # Both launchpads land in one ingestion module — same graph shape, different
    # columns — selected with --source.
    Step(
        "B",
        "clanker_tokens",
        "clanker_tokens",
        ("ingestion.ingest_tokens",),
        ingest_args=("--source", "clanker"),
    ),
    Step(
        "B",
        "bankr_tokens",
        "bankr_tokens",
        ("ingestion.ingest_tokens",),
        ingest_args=("--source", "bankr"),
    ),
    Step("C", "token_buyers", "token_buyers", ("ingestion.ingest_token_buyers",)),
    Step("C", "popular_tokens", "popular_tokens", ("ingestion.ingest_popular_tokens",)),
    Step("D", "token_evangelists", "token_evangelists", ("ingestion.ingest_token_evangelists",)),
    # arb_cohort is a driver for the Hyperliquid crawl: its wallets and fids are
    # already in the graph from the runs it aggregates, so an ingestion module
    # for it is optional and its absence is not a failure.
    Step(
        "E",
        "arb_cohort",
        "arb_cohort",
        ("ingestion.ingest_arb_cohort", "ingestion.ingest_cohort"),
        ingest_required=False,
    ),
    Step(
        "E",
        "hyperliquid_activity",
        "hyperliquid_activity",
        ("ingestion.ingest_hyperliquid_activity", "ingestion.ingest_hyperliquid"),
    ),
)

STAGE_PURPOSE = {
    "A": "identity: fid -> verified wallets",
    "B": "on-chain + social activity for that identity set",
    "C": "trades against the token registries stage B built",
    "D": "attribution of stage C's buys to the accounts that shilled them",
    "E": "the derived cohort and the per-wallet Hyperliquid crawl",
}

STAGES: tuple[str, ...] = tuple(dict.fromkeys(step.stage for step in PLAN))

# Which seed each seeded step needs, for the preflight warning.
SEED_SCHEMAS = {
    "miniapp_builders": "fid[,username,app_name,app_url]",
    "brand_accounts": "fid,name[,weight]",
}


@dataclass
class Result:
    stage: str
    name: str
    kind: str  # setup | pipeline | ingest
    status: str  # ok | failed | skipped | missing | blocked
    seconds: float = 0.0
    exit_code: int | None = None
    command: str = ""
    detail: str = ""


def resolve_ingest_module(candidates: tuple[str, ...]) -> str | None:
    """First candidate that actually exists as an importable module."""
    for name in candidates:
        try:
            if importlib.util.find_spec(name) is not None:
                return name
        except ModuleNotFoundError:
            # The `ingestion` package itself is absent; no candidate can exist.
            return None
    return None


def seed_preflight(steps: list[Step]) -> list[str]:
    """Warn about seed files that are missing or still the shipped template.

    The seeds ship as header-only templates so the repo is runnable, but an
    empty seed makes its pipeline produce an empty (and perfectly valid-looking)
    run. This is the one place that says so out loud before the run starts.
    """
    warnings: list[str] = []
    for name in dict.fromkeys(step.seed for step in steps if step.seed):
        path = SEEDS_DIR / f"{name}.csv"
        schema = SEED_SCHEMAS.get(name, "")
        if not path.exists():
            warnings.append(f"{path} is missing (expected {schema}) — its pipeline will fail")
            continue
        rows = [line for line in path.read_text().splitlines() if line.strip()]
        if len(rows) <= 1:
            warnings.append(
                f"{path} is the header-only template — its pipeline will run and "
                f"write empty CSVs. Fill it in ({schema}); see seeds/README.md"
            )
    return warnings


class Orchestrator:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.results: list[Result] = []
        self.constraints_done = False
        self.env = dict(os.environ)
        # Children log through logging.basicConfig(stream=stdout); unbuffered
        # output keeps their lines interleaved in real order with ours.
        self.env["PYTHONUNBUFFERED"] = "1"

    # -- child processes ---------------------------------------------------

    def _shared_flags(self) -> list[str]:
        flags: list[str] = []
        if self.args.dry_run:
            flags.append("--dry-run")
        if self.args.log_level:
            flags += ["--log-level", self.args.log_level]
        return flags

    def pipeline_command(self, step: Step) -> list[str]:
        flags: list[str] = []
        if self.args.backfill:
            flags.append("--backfill")
        if self.args.since:
            flags += ["--since", self.args.since]
        if self.args.limit is not None:
            flags += ["--limit", str(self.args.limit)]
        return (
            [sys.executable, "-m", f"pipelines.{step.pipeline}"]
            + flags
            + self._shared_flags()
            + list(step.extra_args)
        )

    def ingest_command(self, module: str, extra: tuple[str, ...] = ()) -> list[str]:
        # No --run-id: ingestion defaults to the latest completed run, which is
        # the one the pipeline just sealed.
        flags = list(extra) + self._shared_flags()
        if self.args.batch_size is not None:
            flags += ["--batch-size", str(self.args.batch_size)]
        # The constraint pass already ran once for the whole schedule; every
        # ingestion module exposes this flag so it is not repeated per step.
        if self.constraints_done:
            flags.append("--no-constraints")
        return [sys.executable, "-m", module] + flags

    def _run(self, cmd: list[str], stage: str, name: str, kind: str) -> Result:
        printed = " ".join(shlex.quote(part) for part in cmd)
        logger.info("[%s] %s -> %s", stage, name, printed)
        started = time.monotonic()
        try:
            completed = subprocess.run(cmd, cwd=REPO_ROOT, env=self.env, check=False)
            code = completed.returncode
            detail = ""
        except OSError as exc:
            logger.error("could not start %s: %s", printed, exc)
            code, detail = 127, str(exc)
        seconds = time.monotonic() - started
        result = Result(
            stage=stage,
            name=name,
            kind=kind,
            status="ok" if code == 0 else "failed",
            seconds=seconds,
            exit_code=code,
            command=printed,
            detail=detail,
        )
        level = logging.INFO if code == 0 else logging.ERROR
        logger.log(level, "[%s] %s %s in %s", stage, name, result.status, human_duration(seconds))
        self.results.append(result)
        return result

    # -- constraints -------------------------------------------------------

    def ensure_constraints(self) -> Result:
        """Run ingestion.constraints.ensure_constraints once, before any writes.

        Via the module's CLI rather than an in-process import: the function
        needs a live Neo4jUtils, and `python -m ingestion.constraints` is the
        one place that owns building (and closing) it. Running it here — and
        passing --no-constraints to every ingestion step afterwards — means the
        DDL pass happens once per schedule instead of once per data type.
        """
        module_name = CONSTRAINTS_MODULE
        if resolve_ingest_module((module_name,)) is None:
            result = Result(
                "-",
                module_name,
                "setup",
                "missing",
                detail=f"{module_name} not importable; pass --skip-ingest to run pipelines only",
            )
            logger.error("%s", result.detail)
            self.results.append(result)
            return result
        # constraints.py's CLI takes only --dry-run/--log-level; no batch size.
        result = self._run(
            [sys.executable, "-m", module_name] + self._shared_flags(), "-", module_name, "setup"
        )
        self.constraints_done = result.status == "ok"
        return result

    # -- the schedule ------------------------------------------------------

    def execute(self, steps: list[Step]) -> bool:
        """Run the selected steps. Returns False if anything failed."""
        ok = True
        if not self.args.skip_ingest:
            if self.ensure_constraints().status in {"failed", "missing"}:
                ok = False
                if not self.args.continue_on_error:
                    return ok

        current_stage: str | None = None
        for step in steps:
            if step.stage != current_stage:
                current_stage = step.stage
                logger.info(
                    "===== stage %s: %s =====", current_stage, STAGE_PURPOSE.get(current_stage, "")
                )
            pipeline_result = self._run(
                self.pipeline_command(step), step.stage, step.pipeline, "pipeline"
            )
            if pipeline_result.status != "ok":
                ok = False
                if not self.args.skip_ingest:
                    self.results.append(
                        Result(
                            step.stage,
                            f"{step.pipeline} (ingest)",
                            "ingest",
                            "blocked",
                            detail="pipeline failed; nothing new to ingest",
                        )
                    )
                if not self.args.continue_on_error:
                    return ok
                continue

            if self.args.skip_ingest:
                continue

            module = resolve_ingest_module(step.ingest)
            if module is None:
                status = "missing"
                detail = f"no ingestion module for {step.data_type} (tried {', '.join(step.ingest)})"
                if step.ingest_required:
                    ok = False
                    logger.error("%s", detail)
                else:
                    status = "skipped"
                    logger.info("%s — optional, continuing", detail)
                self.results.append(
                    Result(step.stage, f"{step.pipeline} (ingest)", "ingest", status, detail=detail)
                )
                if not ok and not self.args.continue_on_error:
                    return ok
                continue

            ingest_label = " ".join([module, *step.ingest_args])
            ingest_result = self._run(
                self.ingest_command(module, step.ingest_args), step.stage, ingest_label, "ingest"
            )
            if ingest_result.status != "ok":
                ok = False
                if not self.args.continue_on_error:
                    return ok
        return ok


def human_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def print_summary(results: list[Result], elapsed: float) -> None:
    header = ("STAGE", "STEP", "STATUS", "DURATION", "EXIT")
    rows = [
        (
            r.stage,
            r.name,
            r.status,
            human_duration(r.seconds) if r.seconds else "-",
            "-" if r.exit_code is None else str(r.exit_code),
        )
        for r in results
    ]
    widths = [max(len(str(row[i])) for row in [header, *rows]) for i in range(len(header))]
    line = "  ".join("-" * w for w in widths)
    print()
    print("  ".join(h.ljust(w) for h, w in zip(header, widths)))
    print(line)
    for row in rows:
        print("  ".join(str(cell).ljust(w) for cell, w in zip(row, widths)))
    print(line)
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    tally = ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
    plural = "" if len(results) == 1 else "s"
    print(f"{len(results)} step{plural} in {human_duration(elapsed)} ({tally})")
    for r in results:
        if r.detail and r.status != "ok":
            print(f"  ! {r.name}: {r.detail}")


def resume_flags(args: argparse.Namespace) -> list[str]:
    """The invocation's own flags, so the resume hint is copy-pasteable."""
    flags: list[str] = []
    if args.backfill:
        flags.append("--backfill")
    if args.since:
        flags += ["--since", args.since]
    if args.dry_run:
        flags.append("--dry-run")
    if args.limit is not None:
        flags += ["--limit", str(args.limit)]
    if args.batch_size is not None:
        flags += ["--batch-size", str(args.batch_size)]
    if args.skip_ingest:
        flags.append("--skip-ingest")
    if args.continue_on_error:
        flags.append("--continue-on-error")
    if args.log_level:
        flags += ["--log-level", args.log_level]
    return flags


def print_resume(results: list[Result], args: argparse.Namespace) -> None:
    failures = [r for r in results if r.status in {"failed", "missing"}]
    if not failures:
        return
    first = failures[0]
    print()
    if first.command:
        print("FAILED. Fix the cause, then re-run just that step:")
        print(f"  {first.command}")
    else:
        # A step that never started (a missing module) has no command to repeat.
        print(f"FAILED: {first.name} — {first.detail}")
    stages_hit = [r.stage for r in failures if r.stage in STAGES]
    if stages_hit:
        earliest = min(stages_hit, key=STAGES.index)
        rest = " ".join(shlex.quote(f) for f in resume_flags(args))
        print("Resume the whole schedule from that stage onward with:")
        print(f"  {sys.executable} scripts/run_all.py --from-stage {earliest} {rest}".rstrip())


def select_steps(args: argparse.Namespace) -> tuple[list[Step], list[Step]]:
    """Split PLAN into (selected, skipped) honouring --only and --from-stage."""
    names = {step.pipeline for step in PLAN} | {step.data_type for step in PLAN}
    wanted: set[str] | None = None
    if args.only:
        wanted = {part.strip() for part in args.only.split(",") if part.strip()}
        unknown = wanted - names
        if unknown:
            raise SystemExit(
                f"--only: unknown pipeline(s) {sorted(unknown)}.\n"
                f"Known: {', '.join(sorted(step.pipeline for step in PLAN))}"
            )
    start = STAGES.index(args.from_stage) if args.from_stage else 0

    selected: list[Step] = []
    skipped: list[Step] = []
    for step in PLAN:
        in_range = STAGES.index(step.stage) >= start
        chosen = in_range and (wanted is None or bool({step.pipeline, step.data_type} & wanted))
        (selected if chosen else skipped).append(step)
    return selected, skipped


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python scripts/run_all.py",
        description="Run every pipeline and its ingestion, in dependency order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(
            f"  stage {stage}: {STAGE_PURPOSE[stage]}\n"
            + "\n".join(f"      {s.pipeline}" for s in PLAN if s.stage == stage)
            for stage in STAGES
        ),
    )
    parser.add_argument("--backfill", action="store_true", help="Fan --backfill out to every pipeline.")
    parser.add_argument("--since", default=None, help="Fan --since TS out to every pipeline.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Fan --dry-run out: plan everything, spend nothing."
    )
    parser.add_argument("--limit", type=int, default=None, help="Fan --limit N out to every pipeline.")
    parser.add_argument(
        "--batch-size", type=int, default=None, help="Fan --batch-size N out to every ingestion module."
    )
    parser.add_argument(
        "--only",
        default=None,
        help="Comma-separated pipeline names to run (e.g. clanker_tokens,bankr_tokens).",
    )
    parser.add_argument(
        "--from-stage",
        default=None,
        choices=list(STAGES),
        help="Skip every stage before this one. Use it to resume a failed schedule.",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Pipelines only: no constraints, no Neo4j writes. CSVs are still written.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going after a failure instead of stopping. The exit code is still non-zero.",
    )
    parser.add_argument("--list", action="store_true", help="Print the plan and exit.")
    parser.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level)
    selected, skipped = select_steps(args)

    if args.list or not selected:
        for stage in STAGES:
            print(f"stage {stage}  {STAGE_PURPOSE[stage]}")
            for step in PLAN:
                if step.stage != stage:
                    continue
                mark = "run " if step in selected else "skip"
                module = resolve_ingest_module(step.ingest)
                target = (
                    " ".join([module, *step.ingest_args]) if module else "(no ingestion module)"
                )
                print(f"  [{mark}] {step.pipeline:<22} -> data/{step.data_type}/  ->  {target}")
        if not selected:
            print("\nnothing selected — check --only / --from-stage")
            return 1
        return 0

    for warning in seed_preflight(selected):
        logger.warning("%s", warning)

    mode = "dry run" if args.dry_run else ("backfill" if args.backfill else "incremental")
    logger.info(
        "%s: %d pipeline(s) across stages %s%s",
        mode,
        len(selected),
        ",".join(dict.fromkeys(s.stage for s in selected)),
        " (ingestion skipped)" if args.skip_ingest else "",
    )
    if skipped:
        logger.info("skipping: %s", ", ".join(s.pipeline for s in skipped))

    orchestrator = Orchestrator(args)
    started = time.monotonic()
    interrupted = False
    try:
        ok = orchestrator.execute(selected)
    except KeyboardInterrupt:
        ok, interrupted = False, True
        orchestrator.results.append(Result("-", "interrupted", "setup", "failed", detail="Ctrl-C"))

    print_summary(orchestrator.results, time.monotonic() - started)
    if ok:
        return 0
    print_resume(orchestrator.results, args)
    return 130 if interrupted else 1


if __name__ == "__main__":
    raise SystemExit(main())
