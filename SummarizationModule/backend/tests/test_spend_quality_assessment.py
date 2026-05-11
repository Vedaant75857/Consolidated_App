import os
import sys
import unittest
from unittest.mock import patch


BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from shared.duckdb_compat import duckdb_connect
from services.spend_quality_assessment.data_quality import (
    _compute_column_fill_rate,
    _compute_categorization_effort,
    _compute_spend_bifurcation,
    _compute_spend_breakdown,
    _compute_supplier_breakdown,
    run_executive_summary_ai,
)


class SpendQualityAssessmentTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb_connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE "analysis_data" (
                invoice_date VARCHAR,
                total_spend DOUBLE,
                supplier VARCHAR,
                description VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('2025-06-15', 1000, 'A', 'Detailed pump service'),
                ('2025-07-15', 100, 'B', 'Bolt'),
                ('2026-05-10', -50, 'C', 'Credit'),
                ('2026-06-10', 200, 'B', 'Safety gloves'),
                ('2026-07-01', 10000, 'A', 'Motor replacement')
            """
        )
        self.conn.execute(
            "CREATE TABLE _table_registry "
            "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
        )
        self.conn.execute(
            "INSERT INTO _table_registry VALUES ('sample.csv::', 'data__sample', 'raw__sample')"
        )
        self.conn.execute(
            """
            CREATE TABLE "raw__sample" (
                RAW_0 VARCHAR,
                RAW_1 VARCHAR,
                RAW_2 VARCHAR,
                RAW_3 VARCHAR,
                RAW_4 VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO "raw__sample" VALUES
                ('Invoice Date', 'Spend Amount', 'Supplier Name', 'Description', 'Optional Field')
            """
        )
        self.conn.execute(
            """
            CREATE TABLE "data__sample" (
                RECORD_ID VARCHAR,
                "INVOICE DATE" VARCHAR,
                "SPEND AMOUNT" VARCHAR,
                "SUPPLIER NAME" VARCHAR,
                "DESCRIPTION" VARCHAR,
                "OPTIONAL FIELD" VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO "data__sample" VALUES
                ('1', '2025-06-15', '1000', 'A', 'Detailed pump service', 'filled'),
                ('2', '2025-07-15', '100', 'B', 'Bolt', ''),
                ('3', '2026-05-10', '-50', 'C', 'Credit', ''),
                ('4', '2026-06-10', '200', 'B', 'Safety gloves', ''),
                ('5', '2026-07-01', '10000', 'A', 'Motor replacement', '')
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_ltm_excludes_latest_month(self):
        result = _compute_spend_breakdown(self.conn, {"invoice_date", "total_spend"})

        self.assertTrue(result["feasible"])
        self.assertEqual(result["ltmSpend"], 250)
        self.assertEqual(result["ltmPeriodLabel"], "Jul 2025 - Jun 2026")
        self.assertIsNone(result.get("latestFullYearSpend"))
        self.assertIsNone(result.get("latestFullYearLabel"))

    def test_latest_full_year_spend_requires_twelve_months(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                invoice_date VARCHAR,
                total_spend DOUBLE
            )
            """
        )
        # 2024: one row per month so DISTINCT months = 12
        for m in range(1, 13):
            conn.execute(
                "INSERT INTO \"analysis_data\" VALUES (?, 100.0)",
                (f"2024-{m:02d}-15",),
            )
        conn.execute(
            'INSERT INTO "analysis_data" VALUES (\'2026-01-15\', 50.0)'
        )
        result = _compute_spend_breakdown(conn, {"invoice_date", "total_spend"})
        conn.close()

        self.assertTrue(result["feasible"])
        self.assertEqual(result["latestFullYearLabel"], "2024")
        self.assertEqual(result["latestFullYearSpend"], 1200)

    def test_supplier_80_uses_net_spend(self):
        result = _compute_supplier_breakdown(self.conn, {"supplier", "total_spend"})

        self.assertTrue(result["feasible"])
        self.assertEqual(result["totalSuppliers"], 3)
        self.assertEqual(result["suppliersTo80Pct"], 1)

    def test_positive_negative_spend_uses_signed_net_percentages(self):
        result = _compute_spend_bifurcation(self.conn, {"total_spend"})

        self.assertTrue(result["feasible"])
        self.assertEqual(result["positiveSpend"], 11300)
        self.assertEqual(result["negativeSpend"], -50)
        self.assertEqual(result["netSpend"], 11250)
        self.assertEqual(result["positivePctOfNet"], 100.4)
        self.assertEqual(result["negativePctOfNet"], -0.4)

    def test_column_fill_rate_uses_original_headers_and_all_rows(self):
        result = _compute_column_fill_rate(self.conn, {"total_spend"})
        optional = next(c for c in result["columns"] if c["columnName"] == "Optional Field")

        self.assertEqual(result["columns"][0]["columnName"], "Invoice Date")
        self.assertEqual(optional["fillRate"], 20.0)
        self.assertEqual(optional["spendCoverage"], 8.9)

    def test_ai_phase_adds_summary_rows_without_flags(self):
        sql_result = {
            "totalRows": 5,
            "datePeriod": {
                "feasible": True,
                "periodLabel": "Jun 2025 - Jul 2026",
                "monthsCovered": 5,
                "startDate": "2025-06-15",
                "endDate": "2026-07-01",
            },
            "spendBreakdown": {
                "feasible": True,
                "ltmSpend": 250,
                "ltmPeriodLabel": "Jul 2025 - Jun 2026",
            },
            "supplierBreakdown": {
                "feasible": True,
                "totalSuppliers": 3,
                "suppliersTo80Pct": 1,
            },
            "categorizationEffort": {
                "feasible": True,
                "metrics": {
                    "rowCount": 5,
                    "fillRate": 100,
                    "avgWordCount": 2.0,
                    "distinctPairs": 5,
                    "sampledCount": 4,
                    "topVendorPairsCount": 7,
                },
                "mapAICost": 1.25,
                "forcedMethod": None,
                "random1000Descriptions": ["a", "b", "c", "d"],
            },
            "flags": {"legacy": True},
        }
        cat_response = {
            "buckets": {"high": 2, "medium": 1, "low": 1},
            "qualityVerdict": "medium",
            "recommendedMethod": "MapAI",
            "reasoning": "Most sampled lines sit in the **medium** bucket.",
        }
        summary_response = {
            "rows": [
                {"key": "timePeriod", "label": "Time period", "text": "Data covers **Jun 2025 - Jul 2026**."},
                {"key": "ltmSpend", "label": "LTM spend", "text": "LTM spend is **250**."},
                {"key": "supplierConcentration", "label": "Suppliers", "text": "**1** supplier covers 80% spend."},
                {"key": "descriptionQuality", "label": "Description quality", "text": "placeholder"},
                {"key": "categorizationMethod", "label": "Categorization method", "text": "placeholder"},
            ]
        }

        with patch(
            "services.spend_quality_assessment.data_quality._generate_categorization_recommendation",
            return_value=cat_response,
        ), patch(
            "services.spend_quality_assessment.data_quality.call_ai_json",
            return_value=summary_response,
        ):
            result = run_executive_summary_ai(sql_result, "test-key")

        self.assertNotIn("flags", result)
        self.assertEqual(len(result["executiveSummary"]["rows"]), 5)
        self.assertEqual(result["categorizationEffort"]["bucketsPct"]["high"], 50.0)

        desc_row = next(r for r in result["executiveSummary"]["rows"] if r["key"] == "descriptionQuality")
        self.assertIn("**medium**", desc_row["text"])
        self.assertNotIn("MapAI", desc_row["text"])

        cat_row = next(r for r in result["executiveSummary"]["rows"] if r["key"] == "categorizationMethod")
        self.assertIn("MapAI", cat_row["text"])
        self.assertIn("7", cat_row["text"])
        self.assertIn("manual validation", cat_row["text"].lower())

    def test_top_80_vendor_cohort_drives_categorization_inputs(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                total_spend DOUBLE,
                supplier VARCHAR,
                description VARCHAR
            )
            """
        )
        # Vendor Big holds >80% of spend; Small holds the rest.
        conn.execute(
            'INSERT INTO "analysis_data" VALUES (9000, \'BigCo\', \'Alpha part\')'
        )
        conn.execute(
            'INSERT INTO "analysis_data" VALUES (500, \'BigCo\', \'Beta line\')'
        )
        conn.execute(
            'INSERT INTO "analysis_data" VALUES (500, \'SmallCo\', \'Other\')'
        )
        cols = {"total_spend", "supplier", "description"}
        result = _compute_categorization_effort(conn, cols)
        conn.close()

        sampled = result["random1000Descriptions"]
        self.assertEqual(set(sampled), {"Alpha part", "Beta line"})
        self.assertEqual(result["metrics"]["topVendorPairsCount"], 2)


if __name__ == "__main__":
    unittest.main()
