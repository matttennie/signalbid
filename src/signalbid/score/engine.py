"""Rules-based opportunity scoring engine (v0 - no LLM)"""

import re
from datetime import datetime, timezone
from typing import Any, Optional


class OieScorer:
    """
    Opportunity Intelligence Engine Scorer.

    v0 implementation uses deterministic rules only (no LLM API calls).
    Assigns decision, budget_bucket, deadline_bucket, and enriches metadata.
    """

    def __init__(self, prompt_path: Optional[str] = None):
        # prompt_path reserved for future LLM-based scoring
        self.prompt_path = prompt_path

    def process(self, item: dict[str, Any]) -> dict[str, Any]:
        """Score and enrich an opportunity item"""

        # Parse and normalize deadline
        deadline_iso = self._parse_deadline(item.get("deadline"))
        item["deadline"] = deadline_iso

        # Compute deadline bucket
        item["deadline_bucket"] = self._compute_deadline_bucket(deadline_iso)

        # Extract budget and assign bucket
        budget_value = self._extract_budget(item.get("description", ""))
        item["budget_value"] = budget_value
        item["budget_bucket"] = self._compute_budget_bucket(budget_value)

        # Assign decision using simple heuristic
        item["decision"] = self._compute_decision(item["deadline_bucket"], item["budget_bucket"])

        # Generate one-liner summary
        item["one_liner"] = self._generate_one_liner(item)

        # Generate searchable tags
        item["tags"] = self._generate_tags(item)

        return item

    def _parse_deadline(self, deadline_str: Optional[str]) -> Optional[str]:
        """
        Parse deadline string to ISO-8601 format.
        Returns None if unparseable.
        """
        if not deadline_str:
            return None

        # Try ISO format first
        if re.match(r"\d{4}-\d{2}-\d{2}", deadline_str):
            return deadline_str

        # Try common US format: MM/DD/YYYY
        match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", deadline_str)
        if match:
            month, day, year = match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Try text format: "January 15, 2025"
        match = re.match(r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})", deadline_str, re.IGNORECASE)
        if match:
            month_name, day, year = match.groups()
            month_map = {
                "january": "01",
                "february": "02",
                "march": "03",
                "april": "04",
                "may": "05",
                "june": "06",
                "july": "07",
                "august": "08",
                "september": "09",
                "october": "10",
                "november": "11",
                "december": "12",
            }
            month = month_map.get(month_name.lower())
            if month:
                return f"{year}-{month}-{day.zfill(2)}"

        return None

    def _compute_deadline_bucket(self, deadline_iso: Optional[str]) -> str:
        """
        Compute deadline bucket: immediate (<7 days), near_term (7-30 days), planning (>30 days)
        """
        if not deadline_iso:
            return "unknown"

        try:
            deadline_dt = datetime.fromisoformat(deadline_iso).replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            days_until = (deadline_dt - now).days

            if days_until < 7:
                return "immediate"
            elif days_until < 30:
                return "near_term"
            else:
                return "planning"
        except (ValueError, TypeError):
            return "unknown"

    def _extract_budget(self, text: str) -> Optional[float]:
        """
        Extract budget amount from text.
        Looks for patterns like: $500,000, €1.2M, USD 250K
        Returns value in USD (best effort).
        """
        if not text:
            return None

        # Pattern: $1,000,000 or $1M or $500K
        patterns = [
            (r"\$\s*([\d,]+(?:\.\d+)?)\s*[Mm](?:illion)?", 1_000_000),
            (r"\$\s*([\d,]+(?:\.\d+)?)\s*[Kk]", 1_000),
            (r"\$\s*([\d,]+(?:\.\d+)?)", 1),
            (r"USD\s*([\d,]+(?:\.\d+)?)\s*[Mm](?:illion)?", 1_000_000),
            (r"USD\s*([\d,]+(?:\.\d+)?)\s*[Kk]", 1_000),
            (r"USD\s*([\d,]+(?:\.\d+)?)", 1),
        ]

        for pattern, multiplier in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value_str = match.group(1).replace(",", "")
                try:
                    return float(value_str) * multiplier
                except ValueError:
                    continue

        return None

    def _compute_budget_bucket(self, budget_value: Optional[float]) -> str:
        """
        Assign budget bucket based on value:
        micro (<50K), small (50K-250K), mid (250K-1M), enterprise (>1M), unknown
        """
        if budget_value is None:
            return "unknown"

        if budget_value < 50_000:
            return "micro"
        elif budget_value < 250_000:
            return "small"
        elif budget_value < 1_000_000:
            return "mid"
        else:
            return "enterprise"

    def _compute_decision(self, deadline_bucket: str, budget_bucket: str) -> str:
        """
        Simple heuristic decision logic:
        - GO: planning deadline + mid/enterprise budget
        - MAYBE: near_term deadline OR small budget
        - NO_GO: immediate deadline OR micro/unknown budget
        """
        if deadline_bucket == "planning" and budget_bucket in ["mid", "enterprise"]:
            return "GO"
        elif deadline_bucket in ["near_term", "planning"] and budget_bucket in [
            "small",
            "mid",
        ]:
            return "MAYBE"
        else:
            return "NO_GO"

    def _generate_one_liner(self, item: dict[str, Any]) -> str:
        """Generate a concise summary line"""
        org = item.get("buyer_org", "Unknown")
        bucket = item.get("budget_bucket", "unknown")
        deadline = item.get("deadline_bucket", "unknown")

        return f"{org} opportunity - {bucket} budget, {deadline} deadline"

    def _generate_tags(self, item: dict[str, Any]) -> str:
        """
        Generate space-separated searchable tags.
        Format: decision_GO buyer_federal budget_mid region_europe deadline_planning
        """
        tags = []

        decision = item.get("decision", "NO_GO")
        tags.append(f"decision_{decision}")

        buyer_type = item.get("buyer_type", "unknown")
        tags.append(f"buyer_{buyer_type}")

        budget_bucket = item.get("budget_bucket", "unknown")
        tags.append(f"budget_{budget_bucket}")

        region = item.get("region", "unknown")
        tags.append(f"region_{region}")

        deadline_bucket = item.get("deadline_bucket", "unknown")
        tags.append(f"deadline_{deadline_bucket}")

        return " ".join(tags)
