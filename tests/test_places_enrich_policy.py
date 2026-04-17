"""Google Places enrichment gating (env + per-request force)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from places_enrich import google_places_enrich_effective


class TestPlacesEnrichPolicy(unittest.TestCase):
    @patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "x", "GOOGLE_PLACES_ENRICH": "0"}, clear=False)
    def test_force_true_runs_without_env(self) -> None:
        self.assertTrue(google_places_enrich_effective(True))

    @patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "x", "GOOGLE_PLACES_ENRICH": "1"}, clear=False)
    def test_force_false_blocks(self) -> None:
        self.assertFalse(google_places_enrich_effective(False))

    @patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "", "GOOGLE_PLACES_ENRICH": "1"}, clear=False)
    def test_no_key_never_effective(self) -> None:
        self.assertFalse(google_places_enrich_effective(None))
        self.assertFalse(google_places_enrich_effective(True))


if __name__ == "__main__":
    unittest.main()
