"""scripts.run_all — stage ordering and step selection.

The orchestrator's whole job is running things in an order that makes their
inputs exist. The tests below pin the dependency order declared in PLAN and the
two selectors (`--only`, `--from-stage`) an operator uses to resume a failed
schedule, plus the seed preflight that is the only thing standing between an
empty seed file and a perfectly valid-looking empty run.
"""

from __future__ import annotations

import pytest

from scripts import run_all
from scripts.run_all import (
    PLAN,
    STAGES,
    build_parser,
    human_duration,
    resolve_ingest_module,
    seed_preflight,
    select_steps,
)


def parse(*argv):
    return build_parser().parse_args(list(argv))


def test_the_plan_is_ordered_by_stage():
    stage_indices = [STAGES.index(step.stage) for step in PLAN]

    assert stage_indices == sorted(stage_indices)
    assert STAGES == ("A", "B", "C", "D", "E")


def test_linked_wallets_runs_first_because_everything_joins_through_it():
    assert PLAN[0].pipeline == "linked_wallets"
    assert PLAN[0].stage == "A"


def test_hyperliquid_follows_arb_cohort_within_its_stage():
    stage_e = [step.pipeline for step in PLAN if step.stage == "E"]

    # hyperliquid_activity crawls the cohort arb_cohort writes.
    assert stage_e.index("arb_cohort") < stage_e.index("hyperliquid_activity")


def test_evangelists_run_after_the_registries_and_the_buys():
    order = [step.pipeline for step in PLAN]

    for upstream in ("clanker_tokens", "bankr_tokens", "token_buyers"):
        assert order.index(upstream) < order.index("token_evangelists")


def test_every_pipeline_in_the_plan_exists_as_a_module():
    import importlib.util

    for step in PLAN:
        assert importlib.util.find_spec(f"pipelines.{step.pipeline}") is not None


def test_resolve_ingest_module_picks_the_first_candidate_that_exists():
    assert (
        resolve_ingest_module(("ingestion.does_not_exist", "ingestion.ingest_linked_wallets"))
        == "ingestion.ingest_linked_wallets"
    )
    assert resolve_ingest_module(("ingestion.nope",)) is None


def test_select_steps_with_no_filters_selects_everything():
    selected, skipped = select_steps(parse())

    assert len(selected) == len(PLAN)
    assert skipped == []


def test_only_accepts_a_pipeline_name_or_its_data_type():
    by_pipeline, _ = select_steps(parse("--only", "miniapp_builders"))
    by_data_type, _ = select_steps(parse("--only", "miniapp_builders_activity"))

    assert [s.pipeline for s in by_pipeline] == ["miniapp_builders"]
    assert by_data_type == by_pipeline


def test_only_takes_a_comma_separated_list():
    selected, skipped = select_steps(parse("--only", "clanker_tokens,bankr_tokens"))

    assert {s.pipeline for s in selected} == {"clanker_tokens", "bankr_tokens"}
    assert len(skipped) == len(PLAN) - 2


def test_only_rejects_an_unknown_pipeline_with_the_known_list():
    with pytest.raises(SystemExit) as excinfo:
        select_steps(parse("--only", "clanker_tokens,typo_here"))

    message = str(excinfo.value)
    assert "typo_here" in message
    assert "linked_wallets" in message  # the "Known:" line


def test_from_stage_drops_every_earlier_stage():
    selected, skipped = select_steps(parse("--from-stage", "C"))

    assert {s.stage for s in selected} == {"C", "D", "E"}
    assert {s.stage for s in skipped} == {"A", "B"}


def test_from_stage_and_only_compose():
    selected, _ = select_steps(parse("--from-stage", "C", "--only", "linked_wallets,token_buyers"))

    # linked_wallets is stage A, so the stage filter wins over --only.
    assert [s.pipeline for s in selected] == ["token_buyers"]


def test_seed_preflight_flags_a_missing_seed(layout):
    steps = [s for s in PLAN if s.seed == "brand_accounts"]

    warnings = seed_preflight(steps)

    assert len(warnings) == 1
    assert "brand_accounts.csv is missing" in warnings[0]
    assert "fid,name[,weight]" in warnings[0]


def test_seed_preflight_flags_a_header_only_template(layout):
    (layout.seeds / "brand_accounts.csv").write_text("fid,name,weight\n")
    steps = [s for s in PLAN if s.seed == "brand_accounts"]

    warnings = seed_preflight(steps)

    assert len(warnings) == 1
    assert "header-only template" in warnings[0]


def test_seed_preflight_is_silent_for_a_filled_in_seed(layout):
    (layout.seeds / "brand_accounts.csv").write_text("fid,name,weight\n1,Arbitrum,3\n")
    steps = [s for s in PLAN if s.seed == "brand_accounts"]

    assert seed_preflight(steps) == []


def test_seed_preflight_reports_each_seed_once(layout):
    warnings = seed_preflight(list(PLAN))

    assert len(warnings) == len({s.seed for s in PLAN if s.seed})


@pytest.mark.parametrize(
    "seconds, expected_fragment",
    [(0.4, "0"), (45, "45"), (150, "2m"), (7200, "2h")],
)
def test_human_duration_is_readable(seconds, expected_fragment):
    assert expected_fragment in human_duration(seconds)


def test_the_parser_fans_the_pipeline_flags_out():
    args = parse("--backfill", "--dry-run", "--limit", "5", "--batch-size", "100")

    assert (args.backfill, args.dry_run, args.limit, args.batch_size) == (True, True, 5, 100)


def test_from_stage_only_accepts_a_real_stage():
    with pytest.raises(SystemExit):
        parse("--from-stage", "Z")


def test_the_constraints_module_name_is_importable():
    import importlib.util

    assert importlib.util.find_spec(run_all.CONSTRAINTS_MODULE) is not None
