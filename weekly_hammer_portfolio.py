"""Weekly Hammer entry adapter for the fixed-slot portfolio engine."""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from quant_strategy import SIGNAL_COLUMNS, StrategyDefinition, validate_signal_frame
from weekly_hammer_reversal import HORIZONS, VARIANTS, WEEK, hammer_features, weekly_candles


@dataclass(frozen=True)
class HammerPortfolioSettings:
    variant: str = "D"
    holding_weeks: int = 8


class WeeklyHammerAdapter:
    def __init__(self, variant="D", holding_weeks=8):
        if variant not in VARIANTS or holding_weeks not in HORIZONS:
            raise ValueError("Select a supported weekly hammer variant and holding period.")
        self.config = HammerPortfolioSettings(variant, holding_weeks)
        self.definition = StrategyDefinition(
            "weekly_hammer",
            f"Weekly Hammer · {variant} · {holding_weeks} weeks",
            "Weekly",
            "Completed weekly hammer, next-week open entry, and an exit scheduled from the actual fill.",
            ("Open", "High", "Low", "Close", "Volume"),
            True,
            {"A": 1, "B": 15, "C": 28, "D": 28, "E": 28}[variant],
        )

    def compute_with_calendar(self, daily, weeks, action_weeks):
        if daily.empty:
            return validate_signal_frame(pd.DataFrame(columns=SIGNAL_COLUMNS))
        weeks = weeks.loc[daily.index[0].to_period(WEEK) :]
        features = hammer_features(weekly_candles(daily, weeks), volume_break=action_weeks, price_break=action_weeks)
        signals = features.loc[features[self.config.variant]].copy()
        valid = daily.is_tradable & np.isfinite(daily.Open) & daily.Open.gt(0)
        opens = daily.index[valid].to_series()
        first = opens.groupby(opens.index.to_period(WEEK)).first()
        diagnostics = ["rsi", "rsi_ema", "rvol20", "drawdown26"]
        rows = pd.DataFrame(
            {
                "Decision_Date": weeks.signal_date.reindex(signals.index),
                "Execution_Date": first.reindex(signals.index + 1).to_numpy(),
                "Ready": True,
                "Entry_Event": True,
                "Exit_Event": False,
                "Hold_Eligible": False,
                "Score": 0.0,
                "Entry_Reason": f"Completed weekly hammer · variant {self.config.variant}",
                "Exit_Reason": "",
                "Diagnostics": signals[diagnostics].to_dict("records"),
            },
            index=signals.index,
        )
        return validate_signal_frame(rows)
