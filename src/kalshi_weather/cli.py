"""Typer CLI: kalshi-weather scan, inspect, list-markets."""

from __future__ import annotations

import asyncio
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="kalshi-weather",
    help="Kalshi weather prediction market signal generator",
    no_args_is_help=True,
)
console = Console()


@app.command()
def scan(
    output: str = typer.Option(
        "table", "--output", "-o",
        help="Output format: table, json, csv",
    ),
    min_edge: Optional[float] = typer.Option(
        None, "--min-edge",
        help="Override minimum edge threshold (e.g. 0.05 for 5%)",
    ),
    notify: bool = typer.Option(
        False, "--notify", "-n",
        help="Send Telegram notifications",
    ),
) -> None:
    """Scan Kalshi for weather markets and generate trading signals."""
    from kalshi_weather.signals.formatters import format_csv, format_json, format_table

    # Override settings if CLI flags provided
    if min_edge is not None or notify:
        import kalshi_weather.config as cfg
        original = cfg.get_settings
        def patched() -> cfg.Settings:
            s = original()
            if min_edge is not None:
                s.min_edge = min_edge  # type: ignore[misc]
            if notify:
                s.telegram_enabled = True  # type: ignore[misc]
            return s
        cfg.get_settings = patched

    async def _run() -> None:
        from kalshi_weather.pipeline import run_pipeline
        signals = await run_pipeline()

        if output == "json":
            console.print(format_json(signals))
        elif output == "csv":
            console.print(format_csv(signals))
        else:
            format_table(signals, console)

    asyncio.run(_run())


@app.command(name="list-markets")
def list_markets() -> None:
    """List detected weather markets from Kalshi (no forecasting)."""

    async def _run() -> None:
        from kalshi_weather.pipeline import scan_markets

        markets = await scan_markets()

        if not markets:
            console.print("[yellow]No weather markets found.[/yellow]")
            return

        table = Table(title="Kalshi Weather Markets", show_lines=True)
        table.add_column("Ticker", width=30)
        table.add_column("Type", width=10)
        table.add_column("Location", width=20)
        table.add_column("Threshold", width=12)
        table.add_column("YES Price", justify="right", width=8)
        table.add_column("Volume", justify="right", width=10)
        table.add_column("Title", width=50, no_wrap=False)

        for m in markets:
            market_type = m.params.market_type.value if m.params else "?"
            location = m.params.location if m.params else "?"
            threshold = ""
            if m.params and m.params.threshold is not None:
                threshold = f"{m.params.comparison.value} {m.params.threshold}{m.params.unit}"

            table.add_row(
                m.market_id,
                market_type,
                location[:20] if location else "?",
                threshold,
                f"{m.outcome_yes_price:.2f}",
                f"${m.volume:,.0f}",
                m.question[:80],
            )

        console.print(table)

    asyncio.run(_run())


@app.command()
def inspect(
    ticker: str = typer.Argument(help="Market ticker to inspect"),
) -> None:
    """Inspect a specific weather market with detailed forecast breakdown."""

    async def _run() -> None:
        from kalshi_weather.forecasting.registry import get_model
        from kalshi_weather.markets.client import fetch_market_by_ticker, raw_to_weather_market
        from kalshi_weather.markets.parser import parse_kalshi_market
        from kalshi_weather.signals.analyzer import generate_signal
        from kalshi_weather.weather.noaa import fetch_noaa_forecast
        from kalshi_weather.weather.openmeteo import fetch_both_ensembles

        console.print(f"[bold]Inspecting market: {ticker}[/bold]")

        # Fetch the market
        raw = await fetch_market_by_ticker(ticker)
        if raw is None:
            console.print(f"[red]Market '{ticker}' not found[/red]")
            return

        market = raw_to_weather_market(raw)

        # Parse
        params = await parse_kalshi_market(raw)
        market.params = params

        if params:
            console.print(f"  Type: {params.market_type.value}")
            console.print(f"  Location: {params.location}")
            console.print(f"  Lat/Lon: {params.lat_lon}")
            console.print(f"  Threshold: {params.comparison.value} {params.threshold} {params.unit}")
            console.print(f"  Target date: {params.target_date_str} ({params.target_date})")
        else:
            console.print("[yellow]Could not parse market parameters.[/yellow]")
            return

        # Fetch weather data
        if params.lat_lon:
            lat, lon = params.lat_lon
            console.print(f"\n[bold]Fetching weather data for ({lat:.2f}, {lon:.2f})...[/bold]")

            gfs, ecmwf = await fetch_both_ensembles(lat, lon)
            console.print(f"  GFS: {gfs.n_members} members, {gfs.n_times} time steps")
            console.print(f"  ECMWF: {ecmwf.n_members} members, {ecmwf.n_times} time steps")

            noaa = await fetch_noaa_forecast(lat, lon)
            if noaa:
                console.print(f"  NOAA: {len(noaa.periods)} periods, {len(noaa.alerts)} alerts")
            else:
                console.print("  NOAA: not available (non-US or API error)")

            # Run forecast
            model = get_model(params.market_type)
            if model:
                estimate = await model.estimate(params, gfs, ecmwf, noaa)
                console.print(f"\n[bold]Forecast Result:[/bold]")
                console.print(f"  Model probability: [bold]{estimate.probability:.1%}[/bold]")
                console.print(f"  Raw probability:   {estimate.raw_probability:.1%}")
                console.print(f"  Market probability: {market.market_prob:.1%}")
                console.print(
                    f"  Edge: [{'green' if estimate.probability > market.market_prob else 'red'}]"
                    f"{estimate.probability - market.market_prob:+.1%}[/]"
                )
                console.print(f"  Confidence: {estimate.confidence:.0%}")
                console.print(f"  Lead time: {estimate.lead_time_hours:.0f}h")
                console.print(f"  Sources: {', '.join(estimate.sources_used)}")
                console.print(f"  Details: {estimate.details}")

                signal = generate_signal(market, estimate)
                if signal:
                    console.print(
                        f"\n  [bold green]SIGNAL: {signal.direction} "
                        f"(Kelly: {signal.kelly_fraction:.1%})[/bold green]"
                    )
                else:
                    console.print("\n  [dim]No signal (edge below threshold)[/dim]")

    asyncio.run(_run())


@app.command()
def resolve() -> None:
    """Resolve pending market outcomes from Kalshi."""

    async def _run() -> None:
        from kalshi_weather.signals.resolver import resolve_pending_signals
        from kalshi_weather.signals.tracker import SignalTracker

        resolved = await resolve_pending_signals()

        if not resolved:
            console.print("[dim]No markets newly resolved.[/dim]")
        else:
            table = Table(title="Resolved Markets", show_lines=True)
            table.add_column("Question", width=50, no_wrap=False)
            table.add_column("Outcome", width=8)
            table.add_column("Our Call", width=8)
            table.add_column("Correct", width=8)

            for r in resolved:
                outcome_str = "YES" if r["outcome"] == 1 else "NO"
                direction = r["direction"] or "?"
                mark = "\u2713" if r["correct"] else "\u2717"
                table.add_row(
                    r["question"] or r["market_id"],
                    outcome_str,
                    direction,
                    mark,
                )
            console.print(table)

        # Show updated stats
        tracker = SignalTracker()
        summary = await tracker.get_performance_summary()
        console.print("\n[bold]Updated Stats[/bold]")
        if summary["win_rate"] is not None:
            console.print(f"  Win rate:    {summary['win_rate']:.1%}")
        else:
            console.print("  Win rate:    N/A")
        if summary.get("brier_score") is not None:
            console.print(f"  Brier score: {summary['brier_score']:.3f}")
        else:
            console.print("  Brier score: N/A")

    asyncio.run(_run())


@app.command()
def stats() -> None:
    """Show historical signal performance statistics."""

    async def _run() -> None:
        from kalshi_weather.signals.tracker import SignalTracker

        tracker = SignalTracker()
        summary = await tracker.get_performance_summary()

        console.print("[bold]Signal Performance Summary[/bold]")
        console.print(f"  Total signals logged: {summary['total_signals']}")
        console.print(f"  Resolved outcomes:    {summary['resolved']}")
        if summary['win_rate'] is not None:
            console.print(f"  Win rate:             {summary['win_rate']:.1%}")
        else:
            console.print("  Win rate:             N/A (no resolved outcomes)")
        if summary['avg_abs_edge'] is not None:
            console.print(f"  Avg |edge|:           {summary['avg_abs_edge']:.1%}")
        if summary.get('brier_score') is not None:
            console.print(f"  Brier score:          {summary['brier_score']:.3f}")
        else:
            console.print("  Brier score:          N/A (no resolved outcomes)")

    asyncio.run(_run())


@app.command()
def scorecard(
    market_type: Optional[str] = typer.Option(
        None, "--type", "-t",
        help="Filter by market type (e.g. temperature, precipitation)",
    ),
    filter_direction: Optional[str] = typer.Option(
        None, "--direction", "-d",
        help="Filter by signal direction (YES or NO)",
    ),
    first_signal: bool = typer.Option(
        False, "--first-signal", "-f",
        help="Use first signal per market instead of latest",
    ),
) -> None:
    """Show detailed scorecard of all resolved markets with Brier scores."""

    async def _run() -> None:
        from collections import defaultdict

        from kalshi_weather.signals.tracker import SignalTracker

        tracker = SignalTracker()
        rows = await tracker.get_resolved_signals(
            market_type=market_type,
            first_signal=first_signal,
        )

        # Filter by direction if specified
        if filter_direction:
            d = filter_direction.upper()
            rows = [r for r in rows if (r["direction"] or "").upper() == d]

        if not rows:
            if market_type:
                console.print(f"[yellow]No resolved {market_type} signals found.[/yellow]")
            else:
                console.print("[yellow]No resolved signals found.[/yellow]")
            return

        # Per-signal detail table
        table = Table(
            title="Resolved Signals Scorecard",
            show_lines=True,
        )
        table.add_column("Question", width=40, no_wrap=False)
        table.add_column("Location", width=14)
        table.add_column("Type", width=8)
        table.add_column("Our Call", justify="center", width=8)
        table.add_column("Model P", justify="right", width=8)
        table.add_column("Market P", justify="right", width=8)
        table.add_column("Edge", justify="right", width=8)
        table.add_column("Outcome", justify="center", width=8)
        table.add_column("Result", justify="center", width=6)

        # Track per-type, per-city, and per-direction stats
        type_stats: dict[str, list[tuple[float, int]]] = defaultdict(list)
        city_stats: dict[str, list[tuple[float, int]]] = defaultdict(list)
        city_wins: dict[str, int] = defaultdict(int)
        dir_stats: dict[str, list[tuple[float, int]]] = defaultdict(list)
        dir_wins: dict[str, int] = defaultdict(int)
        total_wins = 0
        total_resolved = 0

        for row in rows:
            outcome_str = "YES" if row["outcome"] == 1 else "NO"
            direction = row["direction"] or "?"
            correct = (
                (direction == "YES" and row["outcome"] == 1)
                or (direction == "NO" and row["outcome"] == 0)
            )
            mark = "[green]W[/green]" if correct else "[red]L[/red]"

            mtype = row["market_type"] or "unknown"
            type_stats[mtype].append((row["model_prob"], row["outcome"]))
            city = (row["location"] or "unknown").strip()
            city_stats[city].append((row["model_prob"], row["outcome"]))
            dir_stats[direction].append((row["model_prob"], row["outcome"]))
            total_resolved += 1
            if correct:
                total_wins += 1
                city_wins[city] += 1
                dir_wins[direction] += 1

            edge_color = "green" if row["edge"] > 0 else "red"
            table.add_row(
                (row["question"] or "")[:60],
                (row["location"] or "")[:14],
                mtype,
                f"[bold]{direction}[/bold]",
                f"{row['model_prob']:.1%}",
                f"{row['market_prob']:.1%}",
                f"[{edge_color}]{row['edge']:+.1%}[/{edge_color}]",
                outcome_str,
                mark,
            )

        console.print(table)

        # Brier score summary by type
        def _brier(pairs: list[tuple[float, int]]) -> float:
            return sum((p - o) ** 2 for p, o in pairs) / len(pairs)

        summary_table = Table(title="Brier Score by Market Type", show_lines=True)
        summary_table.add_column("Market Type", width=16)
        summary_table.add_column("Resolved", justify="right", width=10)
        summary_table.add_column("Win Rate", justify="right", width=10)
        summary_table.add_column("Brier Score", justify="right", width=12)

        all_pairs: list[tuple[float, int]] = []
        for mtype in sorted(type_stats.keys()):
            pairs = type_stats[mtype]
            all_pairs.extend(pairs)
            brier = _brier(pairs)
            wins = sum(
                1 for row in rows
                if (row["market_type"] or "unknown") == mtype
                and (
                    (row["direction"] == "YES" and row["outcome"] == 1)
                    or (row["direction"] == "NO" and row["outcome"] == 0)
                )
            )
            wr = wins / len(pairs)
            summary_table.add_row(
                mtype,
                str(len(pairs)),
                f"{wr:.1%}",
                f"{brier:.3f}",
            )

        if all_pairs:
            total_brier = _brier(all_pairs)
            total_wr = total_wins / total_resolved if total_resolved else 0
            summary_table.add_row(
                "[bold]ALL[/bold]",
                f"[bold]{total_resolved}[/bold]",
                f"[bold]{total_wr:.1%}[/bold]",
                f"[bold]{total_brier:.3f}[/bold]",
            )

        console.print()
        console.print(summary_table)

        # Brier score summary by city
        city_table = Table(title="Brier Score by City", show_lines=True)
        city_table.add_column("City", width=16)
        city_table.add_column("Resolved", justify="right", width=10)
        city_table.add_column("Win Rate", justify="right", width=10)
        city_table.add_column("Brier Score", justify="right", width=12)

        for city in sorted(city_stats.keys()):
            pairs = city_stats[city]
            brier = _brier(pairs)
            wr = city_wins[city] / len(pairs)
            city_table.add_row(
                city,
                str(len(pairs)),
                f"{wr:.1%}",
                f"{brier:.3f}",
            )

        console.print()
        console.print(city_table)

        # Brier score summary by direction (YES vs NO)
        dir_table = Table(title="Brier Score by Direction (YES vs NO)", show_lines=True)
        dir_table.add_column("Direction", width=12)
        dir_table.add_column("Resolved", justify="right", width=10)
        dir_table.add_column("Win Rate", justify="right", width=10)
        dir_table.add_column("Brier Score", justify="right", width=12)

        for d in ("YES", "NO"):
            if d in dir_stats:
                pairs = dir_stats[d]
                brier = _brier(pairs)
                wr = dir_wins[d] / len(pairs)
                dir_table.add_row(d, str(len(pairs)), f"{wr:.1%}", f"{brier:.3f}")

        console.print()
        console.print(dir_table)

        # Win rate by model probability bucket
        buckets = [
            ("0-10%", 0.0, 0.10),
            ("10-20%", 0.10, 0.20),
            ("20-30%", 0.20, 0.30),
            ("30-40%", 0.30, 0.40),
            ("40-50%", 0.40, 0.50),
            ("50-60%", 0.50, 0.60),
            ("60-70%", 0.60, 0.70),
            ("70-80%", 0.70, 0.80),
            ("80-90%", 0.80, 0.90),
            ("90-100%", 0.90, 1.01),
        ]

        prob_table = Table(title="Win Rate by Model Probability", show_lines=True)
        prob_table.add_column("Model P", width=10)
        prob_table.add_column("Count", justify="right", width=8)
        prob_table.add_column("Wins", justify="right", width=8)
        prob_table.add_column("Win Rate", justify="right", width=10)
        prob_table.add_column("Avg Edge", justify="right", width=10)
        prob_table.add_column("Brier", justify="right", width=10)
        prob_table.add_column("Outcome YES%", justify="right", width=12)

        for label, lo, hi in buckets:
            bucket_rows = [
                r for r in rows
                if lo <= r["model_prob"] < hi
            ]
            if not bucket_rows:
                continue
            n = len(bucket_rows)
            wins = sum(
                1 for r in bucket_rows
                if (r["direction"] == "YES" and r["outcome"] == 1)
                or (r["direction"] == "NO" and r["outcome"] == 0)
            )
            wr = wins / n
            avg_edge = sum(abs(r["edge"]) for r in bucket_rows) / n
            brier = sum((r["model_prob"] - r["outcome"]) ** 2 for r in bucket_rows) / n
            yes_pct = sum(1 for r in bucket_rows if r["outcome"] == 1) / n

            prob_table.add_row(
                label,
                str(n),
                str(wins),
                f"{wr:.1%}",
                f"{avg_edge:.1%}",
                f"{brier:.3f}",
                f"{yes_pct:.1%}",
            )

        console.print()
        console.print(prob_table)

        # Same but bucketed by edge magnitude
        edge_buckets = [
            ("0-5%", 0.0, 0.05),
            ("5-10%", 0.05, 0.10),
            ("10-15%", 0.10, 0.15),
            ("15-20%", 0.15, 0.20),
            ("20-30%", 0.20, 0.30),
            ("30%+", 0.30, 10.0),
        ]

        edge_table = Table(title="Win Rate by Edge Size", show_lines=True)
        edge_table.add_column("|Edge|", width=10)
        edge_table.add_column("Count", justify="right", width=8)
        edge_table.add_column("Wins", justify="right", width=8)
        edge_table.add_column("Win Rate", justify="right", width=10)
        edge_table.add_column("Brier", justify="right", width=10)
        edge_table.add_column("NO Signals", justify="right", width=12)

        for label, lo, hi in edge_buckets:
            bucket_rows = [
                r for r in rows
                if lo <= abs(r["edge"]) < hi
            ]
            if not bucket_rows:
                continue
            n = len(bucket_rows)
            wins = sum(
                1 for r in bucket_rows
                if (r["direction"] == "YES" and r["outcome"] == 1)
                or (r["direction"] == "NO" and r["outcome"] == 0)
            )
            wr = wins / n
            brier = sum((r["model_prob"] - r["outcome"]) ** 2 for r in bucket_rows) / n
            no_pct = sum(1 for r in bucket_rows if r["direction"] == "NO") / n

            edge_table.add_row(
                label,
                str(n),
                str(wins),
                f"{wr:.1%}",
                f"{brier:.3f}",
                f"{no_pct:.1%}",
            )

        console.print()
        console.print(edge_table)

    asyncio.run(_run())


@app.command()
def calibrate(
    station: Optional[str] = typer.Option(
        None, "--station", "-s",
        help="Calibrate a single station (ICAO code, e.g. KATL)",
    ),
    days: int = typer.Option(
        90, "--days", "-d",
        help="Training window in days",
    ),
) -> None:
    """Compute per-station bias correction from NWS observed temps vs ERA5.

    For each station, fetches NWS daily high/low from the NCEI Data Service,
    fetches matching ERA5 reanalysis, and computes the systematic bias.
    Results are saved to ~/.kalshi-weather/station_biases.json.

    Kalshi settles on the NWS Daily Climate Report (CLI), so this trains
    against the same data source used for market resolution.

    Can be cron'd (e.g. weekly) to keep biases fresh.
    """

    async def _run() -> None:
        from kalshi_weather.calibration.nws_history import fetch_nws_history
        from kalshi_weather.calibration.openmeteo_history import (
            fetch_openmeteo_history_v2,
            training_window,
        )
        from kalshi_weather.calibration.station_bias import (
            StationBiasV2,
            compute_station_bias_stratified,
            save_biases,
        )
        from kalshi_weather.weather.stations import STATIONS

        start, end = training_window(days)
        console.print(
            f"[bold]Calibrating station biases[/bold]  "
            f"window: {start} to {end} ({days} days)"
        )

        # Determine which stations to calibrate
        if station:
            if station not in STATIONS:
                console.print(f"[red]Unknown station: {station}[/red]")
                console.print(f"Known stations: {', '.join(STATIONS.keys())}")
                return
            targets = {station: STATIONS[station]}
        else:
            targets = STATIONS

        results: dict[str, StationBiasV2] = {}

        for icao, stn in targets.items():
            console.print(f"\n  [bold]{icao}[/bold] ({stn.city})")

            # Fetch NWS observed temps from NCEI
            console.print(f"    Fetching NWS observed temps...", end="")
            nws_obs = await fetch_nws_history(stn, start, end)
            console.print(f" {len(nws_obs)} days")

            if not nws_obs:
                console.print("    [yellow]No NWS data, skipping[/yellow]")
                results[icao] = StationBiasV2(
                    station_id=icao, city=stn.city,
                    high_bias_c=0.0, low_bias_c=0.0, mean_bias_c=0.0,
                    n_days=0,
                )
                continue

            # Fetch ERA5 reanalysis (v2 with cloud cover)
            console.print(f"    Fetching ERA5 reanalysis...", end="")
            lat, lon = stn.lat_lon
            om_obs = await fetch_openmeteo_history_v2(
                lat, lon, start, end, timezone=stn.timezone,
            )
            console.print(f" {len(om_obs)} days")

            if not om_obs:
                console.print("    [yellow]No ERA5 data, skipping[/yellow]")
                results[icao] = StationBiasV2(
                    station_id=icao, city=stn.city,
                    high_bias_c=0.0, low_bias_c=0.0, mean_bias_c=0.0,
                    n_days=0,
                )
                continue

            # Match days present in both datasets
            om_by_date = {o.obs_date: o for o in om_obs}
            nws_highs: list[float] = []
            nws_lows: list[float] = []
            om_maxs: list[float] = []
            om_mins: list[float] = []
            cloud_covers: list[float | None] = []

            for obs in nws_obs:
                om = om_by_date.get(obs.date)
                if om is not None:
                    nws_highs.append(obs.high_temp_c)
                    nws_lows.append(obs.low_temp_c)
                    om_maxs.append(om.max_temp_c)
                    om_mins.append(om.min_temp_c)
                    cloud_covers.append(om.cloud_cover_mean)

            if not nws_highs:
                console.print("    [yellow]No matching days, skipping[/yellow]")
                results[icao] = StationBiasV2(
                    station_id=icao, city=stn.city,
                    high_bias_c=0.0, low_bias_c=0.0, mean_bias_c=0.0,
                    n_days=0,
                )
                continue

            bias = compute_station_bias_stratified(
                nws_highs, nws_lows, om_maxs, om_mins, cloud_covers,
                station_id=icao, city=stn.city,
            )
            results[icao] = bias
            console.print(
                f"    Bias: high={bias.high_bias_c:+.2f}C, "
                f"low={bias.low_bias_c:+.2f}C, "
                f"mean={bias.mean_bias_c:+.2f}C "
                f"(n={bias.n_days})"
            )
            # Show per-condition breakdown
            for cb in bias.condition_biases:
                if cb.n_days > 0:
                    console.print(
                        f"      {cb.condition.value:>8}: "
                        f"high={cb.high_bias_c:+.2f}C, "
                        f"low={cb.low_bias_c:+.2f}C "
                        f"(n={cb.n_days})"
                    )

        # Save results
        path = save_biases(results, training_days=days)
        console.print(f"\n[bold green]Saved biases to {path}[/bold green]")

        # Display summary table
        table = Table(title="Station Bias Summary", show_lines=True)
        table.add_column("Station", width=16)
        table.add_column("City", width=14)
        table.add_column("High Bias", justify="right", width=10)
        table.add_column("Low Bias", justify="right", width=10)
        table.add_column("Mean Bias", justify="right", width=10)
        table.add_column("Std (H/L)", justify="right", width=12)
        table.add_column("Days", justify="right", width=6)
        table.add_column("Clear", justify="right", width=12)
        table.add_column("Partly", justify="right", width=12)
        table.add_column("Overcast", justify="right", width=12)

        for icao, b in results.items():
            # Format per-condition high bias summaries
            cond_strs: dict[str, str] = {}
            for cb in b.condition_biases:
                if cb.n_days > 0:
                    cond_strs[cb.condition.value] = f"{cb.high_bias_c:+.1f} ({cb.n_days}d)"
                else:
                    cond_strs[cb.condition.value] = "-"

            table.add_row(
                icao,
                b.city,
                f"{b.high_bias_c:+.2f}C",
                f"{b.low_bias_c:+.2f}C",
                f"{b.mean_bias_c:+.2f}C",
                f"{b.high_std_c:.2f}/{b.low_std_c:.2f}",
                str(b.n_days),
                cond_strs.get("clear", "-"),
                cond_strs.get("partly", "-"),
                cond_strs.get("overcast", "-"),
            )

        console.print(table)

    asyncio.run(_run())


if __name__ == "__main__":
    app()
