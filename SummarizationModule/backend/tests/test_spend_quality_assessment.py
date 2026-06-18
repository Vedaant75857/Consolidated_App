import os
import sys
import unittest
from contextlib import nullcontext
from unittest.mock import patch

from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes.mapping_routes import mapping_bp
from shared.duckdb_compat import duckdb_connect
from services.spend_quality_assessment.data_quality import (
    _compute_column_fill_rate,
    _compute_categorization_effort,
    _compute_pareto_analysis,
    _compute_spend_bifurcation,
    _compute_spend_breakdown,
    _compute_supplier_breakdown,
    run_executive_summary_ai,
    run_executive_summary_sql,
)
from shared.db import get_meta, set_meta
from services.views.view_engine import compute_pareto


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

    def test_executive_summary_uses_invoice_date_without_warning(self):
        self.conn.execute("CREATE TABLE _meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        set_meta(self.conn, "cast_report", {
            "fields": {
                "invoice_date": {
                    "mapped": True,
                    "sourceColumn": "Invoice Date",
                    "validRows": 5,
                },
                "payment_date": {
                    "mapped": True,
                    "sourceColumn": "Payment Date",
                    "validRows": 5,
                },
            }
        })

        result = run_executive_summary_sql(self.conn)

        self.assertEqual(result["dateSource"]["fieldKey"], "invoice_date")
        self.assertFalse(result["dateSource"]["fallback"])
        self.assertEqual(result["dateSource"]["sourceColumn"], "Invoice Date")
        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["spendBreakdown"]["ltmSpend"], 250)

    def test_executive_summary_falls_back_to_payment_date_with_provenance(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                invoice_date VARCHAR,
                invoice_due_date VARCHAR,
                payment_date VARCHAR,
                total_spend DOUBLE,
                supplier VARCHAR,
                description VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                (NULL, NULL, '2024-01-15', 100.0, 'A', 'Alpha part'),
                (NULL, NULL, '2024-02-15', 200.0, 'B', 'Beta service'),
                (NULL, NULL, '2025-02-15', 300.0, 'A', 'Gamma repair')
            """
        )
        conn.execute("CREATE TABLE _meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        set_meta(conn, "cast_report", {
            "fields": {
                "invoice_date": {
                    "mapped": False,
                    "sourceColumn": None,
                    "validRows": 0,
                },
                "invoice_due_date": {
                    "mapped": True,
                    "sourceColumn": "Due Date",
                    "validRows": 0,
                },
                "payment_date": {
                    "mapped": True,
                    "sourceColumn": "Payment Date",
                    "validRows": 3,
                },
            }
        })

        result = run_executive_summary_sql(conn)
        conn.close()

        self.assertEqual(result["dateSource"]["fieldKey"], "payment_date")
        self.assertTrue(result["dateSource"]["fallback"])
        self.assertEqual(result["dateSource"]["sourceColumn"], "Payment Date")
        self.assertEqual(result["warnings"][0]["code"], "DATE_FALLBACK_USED")
        self.assertEqual(result["datePeriod"]["startDate"], "2024-01-15")
        self.assertEqual(result["datePeriod"]["endDate"], "2025-02-15")
        self.assertEqual(result["spendBreakdown"]["ltmSpend"], 200)
        self.assertEqual(result["datePivot"]["cells"]["2024"]["1"], 100)
        self.assertEqual(result["datePivot"]["cells"]["2024"]["2"], 200)

    def test_executive_summary_uses_first_valid_fallback_after_bad_invoice_date(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                invoice_date VARCHAR,
                invoice_due_date VARCHAR,
                payment_date VARCHAR,
                total_spend DOUBLE,
                supplier VARCHAR,
                description VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('not-a-date', '2024-03-15', '2024-01-15', 100.0, 'A', 'Alpha part'),
                (NULL, '2025-03-15', '2025-01-15', 200.0, 'B', 'Beta service')
            """
        )
        conn.execute("CREATE TABLE _meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        set_meta(conn, "cast_report", {
            "fields": {
                "invoice_date": {
                    "mapped": True,
                    "sourceColumn": "Invoice Date",
                    "validRows": 0,
                },
                "invoice_due_date": {
                    "mapped": True,
                    "sourceColumn": "Due Date",
                    "validRows": 2,
                },
                "payment_date": {
                    "mapped": True,
                    "sourceColumn": "Payment Date",
                    "validRows": 2,
                },
            }
        })

        result = run_executive_summary_sql(conn)
        conn.close()

        self.assertEqual(result["dateSource"]["fieldKey"], "invoice_due_date")
        self.assertTrue(result["dateSource"]["fallback"])
        self.assertEqual(result["dateSource"]["sourceColumn"], "Due Date")
        self.assertEqual(result["datePeriod"]["startDate"], "2024-03-15")
        self.assertEqual(result["datePeriod"]["endDate"], "2025-03-15")
        self.assertEqual(result["warnings"][0]["code"], "DATE_FALLBACK_USED")

    def test_executive_summary_reports_no_usable_date_candidate(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                invoice_date VARCHAR,
                payment_date VARCHAR,
                total_spend DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('not-a-date', '2024-01-15', 100.0),
                (NULL, '2024-02-15', 200.0)
            """
        )
        conn.execute("CREATE TABLE _meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        set_meta(conn, "cast_report", {
            "fields": {
                "invoice_date": {
                    "mapped": True,
                    "sourceColumn": "Invoice Date",
                    "validRows": 0,
                },
                "payment_date": {
                    "mapped": False,
                    "sourceColumn": None,
                    "validRows": 0,
                },
            }
        })

        result = run_executive_summary_sql(conn)
        conn.close()

        self.assertIsNone(result["dateSource"])
        self.assertEqual(result["warnings"], [])
        self.assertFalse(result["datePeriod"]["feasible"])
        self.assertIn("No mapped date field", result["datePeriod"]["message"])
        self.assertFalse(result["spendBreakdown"]["feasible"])
        self.assertFalse(result["datePivot"]["feasible"])

    def test_confirm_mapping_invalidates_cached_executive_summary(self):
        conn = duckdb_connect(":memory:")
        conn.execute("CREATE TABLE _meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        set_meta(conn, "executive_summary", {
            "dateSource": {
                "fieldKey": "invoice_date",
                "displayName": "Invoice Date",
                "sourceColumn": "Invoice Date",
                "fallback": False,
            },
            "warnings": [],
            "executiveSummary": {
                "rows": [
                    {"key": "timePeriod", "label": "Time period", "text": "old"},
                    {"key": "ltmSpend", "label": "LTM spend", "text": "old"},
                    {"key": "supplierConcentration", "label": "Suppliers", "text": "old"},
                    {"key": "descriptionQuality", "label": "Description quality", "text": "old"},
                    {"key": "categorizationMethod", "label": "Categorization method", "text": "old"},
                ]
            },
        })

        app = Flask(__name__)
        app.register_blueprint(mapping_bp)

        with patch("routes.mapping_routes.session_exists", return_value=True), patch(
            "routes.mapping_routes.get_session_lock",
            return_value=nullcontext(),
        ), patch("routes.mapping_routes.get_session_db", return_value=conn), patch(
            "routes.mapping_routes.build_typed_table",
            return_value={"fields": {}},
        ), patch(
            "routes.mapping_routes.get_procurement_view_availability",
            return_value={"available": True},
        ):
            response = app.test_client().post(
                "/confirm-mapping",
                json={"sessionId": "session-1", "mapping": {"payment_date": "Paid On"}},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(get_meta(conn, "executive_summary"))
        conn.close()

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
            "dateSource": {
                "fieldKey": "payment_date",
                "displayName": "Payment Date",
                "sourceColumn": "Payment Date",
                "fallback": True,
                "message": "Invoice Date was unavailable, so Payment Date was used for spend quality date calculations.",
            },
            "warnings": [{
                "code": "DATE_FALLBACK_USED",
                "severity": "warning",
                "message": "Invoice Date was unavailable, so Payment Date was used for spend quality date calculations.",
                "fieldKey": "payment_date",
                "sourceColumn": "Payment Date",
            }],
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
                "mapAICost": 400,
                "mapAICostRange": {"low": 320, "high": 480, "text": "$320 - $480"},
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
        self.assertEqual(result["dateSource"]["fieldKey"], "payment_date")
        self.assertEqual(result["warnings"][0]["code"], "DATE_FALLBACK_USED")
        self.assertEqual(len(result["executiveSummary"]["rows"]), 5)
        self.assertEqual(result["categorizationEffort"]["bucketsPct"]["high"], 50.0)

        desc_row = next(r for r in result["executiveSummary"]["rows"] if r["key"] == "descriptionQuality")
        self.assertIn("**medium**", desc_row["text"])
        self.assertNotIn("MapAI", desc_row["text"])

        cat_row = next(r for r in result["executiveSummary"]["rows"] if r["key"] == "categorizationMethod")
        self.assertIn("MapAI", cat_row["text"])
        self.assertIn("$320 - $480", cat_row["text"])
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

    def test_map_ai_cost_uses_full_dataset_unique_pairs_not_top_80_subset(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" AS
            SELECT
                1.0::DOUBLE AS total_spend,
                'Vendor ' || i::VARCHAR AS supplier,
                'Description ' || i::VARCHAR AS description
            FROM range(10000) AS r(i)
            """
        )

        result = _compute_categorization_effort(
            conn,
            {"total_spend", "supplier", "description"},
        )
        conn.close()

        self.assertEqual(result["metrics"]["distinctPairs"], 10000)
        self.assertEqual(result["metrics"]["topVendorPairsCount"], 8000)
        self.assertEqual(result["mapAICost"], 400)
        self.assertEqual(result["mapAICostRange"]["low"], 320)
        self.assertEqual(result["mapAICostRange"]["high"], 480)
        self.assertEqual(result["mapAICostRange"]["text"], "$320 - $480")

    def test_map_ai_cost_prorates_non_round_unique_pair_counts(self):
        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" AS
            SELECT
                1.0::DOUBLE AS total_spend,
                'Vendor ' || i::VARCHAR AS supplier,
                'Description ' || i::VARCHAR AS description
            FROM range(1234) AS r(i)
            """
        )

        result = _compute_categorization_effort(
            conn,
            {"total_spend", "supplier", "description"},
        )
        conn.close()

        self.assertEqual(result["metrics"]["distinctPairs"], 1234)
        self.assertEqual(result["mapAICost"], 49.36)
        self.assertEqual(result["mapAICostRange"]["low"], 39.49)
        self.assertEqual(result["mapAICostRange"]["high"], 59.23)
        self.assertEqual(result["mapAICostRange"]["text"], "$39 - $59")

    def test_pareto_breach_vendor_and_unique_counts_match_excel_logic(self):
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
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                (500, ' A ', 'Part'),
                (295, 'A', 'Part'),
                (10, 'B', NULL),
                (18, 'B', ''),
                (25, 'C1', 'Tail 1'),
                (25, 'C2', 'Tail 2'),
                (25, 'C3', 'Tail 3'),
                (25, 'C4', 'Tail 4'),
                (25, 'C5', 'Tail 5'),
                (25, 'C6', 'Tail 6'),
                (25, 'C7', 'Tail 7'),
                (2, 'C8', 'Tail 8')
            """
        )
        cols = {"total_spend", "supplier", "description"}

        spend_quality = _compute_pareto_analysis(conn, cols)
        dashboard = compute_pareto(
            conn._conn.execute('SELECT * FROM "analysis_data"').df(),
            80,
        )
        categorization = _compute_categorization_effort(conn, cols)
        conn.close()

        metrics80 = spend_quality["metrics"]["80"]
        self.assertEqual(spend_quality["totalDatasetSpend"], 1000)
        self.assertEqual(metrics80["totalSpend"], 823)
        self.assertEqual(metrics80["supplierCount"], 2)
        self.assertEqual(metrics80["transactionCount"], 4)
        self.assertEqual(metrics80["uniqueTransactions"], 2)

        self.assertEqual(dashboard["suppliersInGroup"], metrics80["supplierCount"])
        self.assertEqual(
            [row["Supplier Name"] for row in dashboard["tableData"]],
            ["A", "B"],
        )
        self.assertEqual(categorization["metrics"]["topVendorPairsCount"], 2)

    def test_pareto_uses_signed_net_spend_including_credits(self):
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
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                (900, 'A', 'Hardware'),
                (100, 'B', 'Services'),
                (-100, 'C', 'Credit')
            """
        )
        cols = {"total_spend", "supplier", "description"}

        spend_quality = _compute_pareto_analysis(conn, cols)
        dashboard = compute_pareto(
            conn._conn.execute('SELECT * FROM "analysis_data"').df(),
            95,
        )
        conn.close()

        self.assertTrue(spend_quality["feasible"])
        self.assertEqual(spend_quality["totalDatasetSpend"], 900)
        self.assertEqual(spend_quality["metrics"]["95"]["supplierCount"], 1)
        self.assertEqual(spend_quality["metrics"]["95"]["totalSpend"], 900)
        self.assertEqual(dashboard["suppliersInGroup"], 1)

    def test_pareto_is_infeasible_when_signed_net_spend_is_not_positive(self):
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
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                (100, 'A', 'Charge'),
                (-100, 'B', 'Credit')
            """
        )
        cols = {"total_spend", "supplier", "description"}

        spend_quality = _compute_pareto_analysis(conn, cols)
        dashboard = compute_pareto(
            conn._conn.execute('SELECT * FROM "analysis_data"').df(),
            80,
        )
        conn.close()

        self.assertFalse(spend_quality["feasible"])
        self.assertIn("zero or negative", spend_quality["message"])
        self.assertFalse(dashboard["feasible"])
        self.assertIn("zero or negative", dashboard["message"])


class CapexOpexClassificationTests(unittest.TestCase):
    def test_classify_from_explicit_indicator_column(self):
        from services.spend_quality_assessment.capex_opex import classify_capex_opex_spend

        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                description VARCHAR,
                capex_opex_indicator VARCHAR,
                total_spend DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('Server upgrade', 'CAPEX', 50000),
                ('Monthly license', 'OPEX', 1200),
                ('Office supplies', '0', 300),
                ('Maintenance', 'N', 800),
                ('Unknown bucket', 'Other', 100),
                ('No flag', NULL, 50)
            """
        )

        result = classify_capex_opex_spend(conn)
        conn.close()

        self.assertTrue(result["feasible"])
        self.assertEqual(result["sourceColumn"], "capex_opex_indicator")
        by_label = {c["label"]: c for c in result["categories"]}
        self.assertEqual(by_label["CAPEX"]["matchingRows"], 1)
        self.assertEqual(by_label["CAPEX"]["totalSpend"], 50000)
        self.assertEqual(by_label["OPEX"]["matchingRows"], 4)
        self.assertEqual(by_label["OPEX"]["totalSpend"], 2400)
        self.assertEqual(result["unclassifiedRows"], 1)
        self.assertEqual(result["unclassifiedSpend"], 50)

    def test_description_not_used_without_indicator(self):
        from services.spend_quality_assessment.capex_opex import classify_capex_opex_spend

        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                description VARCHAR,
                total_spend DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('Server CAPEX upgrade', 50000),
                ('Monthly OPEX license', 1200)
            """
        )

        result = classify_capex_opex_spend(conn)
        conn.close()

        self.assertFalse(result["feasible"])
        self.assertEqual(result["categories"], [])

    def test_y_and_zero_flags(self):
        from services.spend_quality_assessment.capex_opex import classify_capex_opex_spend

        conn = duckdb_connect(":memory:")
        conn.execute(
            """
            CREATE TABLE "analysis_data" (
                gl_account VARCHAR,
                total_spend DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO "analysis_data" VALUES
                ('Y', 10000),
                ('0', 2000),
                ('1', 5000)
            """
        )

        result = classify_capex_opex_spend(conn)
        conn.close()

        by_label = {c["label"]: c for c in result["categories"]}
        self.assertEqual(by_label["CAPEX"]["matchingRows"], 2)
        self.assertEqual(by_label["CAPEX"]["totalSpend"], 15000)
        self.assertEqual(by_label["OPEX"]["matchingRows"], 1)
        self.assertEqual(by_label["OPEX"]["totalSpend"], 2000)


if __name__ == "__main__":
    unittest.main()
