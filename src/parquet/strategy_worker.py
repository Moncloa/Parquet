from __future__ import annotations

import asyncio
import copy
import json
import tempfile
from pathlib import Path

from parquet.models import MarketAnalysis, ReviewRequest
from parquet.strategy import (
    CodexStrategyWorker,
    CodexWorkerSettings,
    _redact,
    _strategy_prompt,
    _strip_json_fence,
    _validate_analysis_for_request,
)


class StdinCodexStrategyWorker(CodexStrategyWorker):
    """Codex worker that keeps large review prompts out of the process argv."""

    async def analyze(self, request: ReviewRequest) -> MarketAnalysis:
        self.settings.work_dir.mkdir(parents=True, exist_ok=True)
        self.settings.codex_home.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=self.settings.work_dir) as temp_name:
            temp = Path(temp_name)
            schema_path = temp / "market-analysis.schema.json"
            output_path = temp / "analysis.json"
            schema_path.write_text(
                json.dumps(MarketAnalysis.model_json_schema(), indent=2),
                encoding="utf-8",
            )

            # Keep the full request in the durable queue for post-analysis validation,
            # but do not spend model context on raw tick points that have already been
            # reduced to deterministic ranking metrics locally.
            prompt_request = _compact_strategy_request(request)
            prompt = _strategy_prompt(prompt_request)

            # Codex `exec -` reads the prompt from stdin.  Passing the full prompt as
            # argv can hit Linux E2BIG for wide-market reviews before Codex starts.
            args = self._codex_args(schema_path, output_path, "-")
            await _run_codex_stdin(
                args,
                prompt=prompt,
                cwd=self.settings.work_dir,
                env=self._codex_env(),
                timeout_seconds=self.settings.timeout_seconds,
            )
            try:
                raw = output_path.read_text(encoding="utf-8").strip()
            except FileNotFoundError as exc:
                raise RuntimeError("Codex did not produce the requested output file") from exc

        raw = _strip_json_fence(raw)
        try:
            analysis = MarketAnalysis.model_validate_json(raw)
        except Exception as exc:
            raise RuntimeError(f"Codex returned invalid MarketAnalysis JSON: {exc}") from exc

        # Validate against the original, unabridged request, not the prompt copy.
        _validate_analysis_for_request(analysis, request)
        return analysis


def _compact_strategy_request(request: ReviewRequest) -> ReviewRequest:
    context = copy.deepcopy(request.context)
    market_data = context.get("market_data")
    if isinstance(market_data, dict):
        wide_scanner = market_data.get("wide_scanner")
        if isinstance(wide_scanner, dict):
            candidates = wide_scanner.get("candidates")
            if isinstance(candidates, list):
                compacted: list[object] = []
                for candidate in candidates:
                    if isinstance(candidate, dict):
                        compacted.append(
                            {
                                key: value
                                for key, value in candidate.items()
                                if key != "points"
                            }
                        )
                    else:
                        compacted.append(candidate)
                wide_scanner["candidates"] = compacted
                wide_scanner["raw_points_omitted_from_strategy_prompt"] = True
    return request.model_copy(update={"context": context})


async def _run_codex_stdin(
    args: list[str],
    *,
    prompt: str,
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: int,
) -> None:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Executable not found: {args[0]}") from exc

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            process.communicate(prompt.encode("utf-8")),
            timeout=timeout_seconds,
        )
    except TimeoutError as exc:
        process.kill()
        await process.communicate()
        raise RuntimeError(f"Codex process timed out after {timeout_seconds}s") from exc

    if process.returncode != 0:
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        detail = _redact((stderr or stdout).strip())[-2000:]
        raise RuntimeError(f"Codex exited {process.returncode}: {detail}")


def main() -> None:
    settings = CodexWorkerSettings.from_env()
    asyncio.run(StdinCodexStrategyWorker(settings).run_forever())


if __name__ == "__main__":
    main()
