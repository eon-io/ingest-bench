Keep whichever checklist applies and delete the other.

## Result PR

- [ ] Spec is committed under `specs/tracks/<track>/` and matches the embedded copy.
- [ ] This is the smallest fleet that passed the full duration. Notes explain
      smaller failed attempts and name any untested sizes.
- [ ] An external engine's `tier: reference` claim is backed by notes showing default settings.
- [ ] Compared runs share one pricing rule. Notes disclose any role that
      needed a node to itself.
- [ ] Notes state the broker shape, and the run was not `producer_bound`.
- [ ] External engines: notes give name, version, reproducible setup, and the
      config files needed to rerun it.
- [ ] Clock-skew flags, missing artifacts and `SLOW_POLL` messages were
      checked, not just `run_valid`.
- [ ] No private company, person, cluster, node-pool or taint names in the
      result or notes.
- [ ] Ran `results-table results/ --out results/RESULTS.md` (it also refreshes
      the README's results block) and `uv run python scripts/validate-results.py`.

See `docs/submitting.md` and the full reviewer checklist in `results/README.md`.

## Code PR

- [ ] `uv run ruff check . && uv run ruff format --check .`
- [ ] `uv run mypy`
- [ ] `uv run pytest -q -m "not integration"`
- [ ] `uv run python scripts/validate-results.py`
- [ ] `mvn -B -f engines/flink/sql-runner/pom.xml verify` (Flink SQL runner changes)
- [ ] `scripts/smoke.sh` for each affected engine (Compose, `smoke.sh`, or engine image changes)
- [ ] SPDX license header on new `.py` / `.sh` files
- [ ] No private account IDs, buckets or other infrastructure details

See the required checks in `CONTRIBUTING.md`.
