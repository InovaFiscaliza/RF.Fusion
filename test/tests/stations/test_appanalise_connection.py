"""
Validation tests for `appAnalise.appAnalise_connection`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/stations/test_appanalise_connection.py -q

What is covered here:
    - protocol-shape validation before BIN normalization begins
    - distinction between valid payloads and service/protocol failures
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APPANALISE_ROOT,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

# The production module imports `shared` by package name. During test discovery,
# `/RFFusion/test/tests/shared` can shadow the real package, so we bind the
# source-tree package only for this import window.
with bind_real_shared_package():
    app_analise_module = load_module_from_path(
        "test_appanalise_connection_module",
        str(APPANALISE_ROOT / "appAnalise_connection.py"),
    )
AppAnaliseConnection = app_analise_module.AppAnaliseConnection
AppAnaliseErrors = app_analise_module.errors


class DetectProtocolErrorTests(unittest.TestCase):
    """Validate protocol framing before payload normalization starts."""

    def setUp(self) -> None:
        self.conn = AppAnaliseConnection()
        self.parser = self.conn.payload_parser

    def test_detect_protocol_error_accepts_valid_payload(self) -> None:
        # Minimal success contract: `Answer` is a dict and exposes `Spectra`.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {"Spectra": [], "General": {}},
        }

        self.parser.detect_protocol_error(payload)

    def test_detect_protocol_error_accepts_single_spectrum_dict(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "Spectra": {
                    "Receiver": "CWSM21100005",
                    "MetaData": {},
                    "GPS": {},
                    "RelatedFiles": [],
                },
                "General": {},
            },
        }

        self.parser.detect_protocol_error(payload)

    def test_detect_protocol_error_rejects_answer_string(self) -> None:
        # Real failures from appAnalise can arrive as plain strings in `Answer`.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.detect_protocol_error(payload)

        self.assertIn("APP_ANALISE returned error in Answer", str(ctx.exception))

    def test_detect_protocol_error_raises_dedicated_timeout_error(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "handlers:FileReadHandler:ReadTimeout",
        }

        with self.assertRaises(
            AppAnaliseErrors.AppAnaliseReadTimeoutError
        ) as ctx:
            self.parser.detect_protocol_error(payload)

        self.assertIn("FileRead timeout", str(ctx.exception))

    def test_detect_protocol_error_retries_missing_file_when_source_still_exists(self) -> None:
        # If MATLAB says "FileNotFound" but the requested source file still
        # exists locally, that looks more like a service-side visibility issue
        # than a definitively bad processing task.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }

        with tempfile.NamedTemporaryFile() as tmp_file:
            with self.assertRaises(
                AppAnaliseErrors.ExternalServiceTransientError
            ) as ctx:
                self.parser.detect_protocol_error(
                    payload,
                    requested_full_path=tmp_file.name,
                )

        self.assertIn("still exists locally", str(ctx.exception))

    def test_detect_protocol_error_rejects_missing_file_when_source_is_gone(self) -> None:
        # The same protocol error becomes definitive if the source file really
        # is absent from the filesystem watched by appCataloga.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }
        missing_path = "/tmp/appCataloga_missing_input.bin"

        if os.path.exists(missing_path):
            os.unlink(missing_path)

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.detect_protocol_error(
                payload,
                requested_full_path=missing_path,
            )

        self.assertIn("absent locally", str(ctx.exception))

    def test_detect_protocol_error_rejects_missing_spectra(self) -> None:
        # A dict-shaped `Answer` is still invalid if the expected payload body is missing.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {"General": {}},
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.detect_protocol_error(payload)

        self.assertIn("Answer.Spectra", str(ctx.exception))

    def test_detect_protocol_error_rejects_top_level_error_field(self) -> None:
        # The protocol also supports an explicit top-level `Error` field.
        payload = {"Error": ["tcpServerLib", "PortClosed"]}

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.detect_protocol_error(payload)

        self.assertIn("tcpServerLib - PortClosed", str(ctx.exception))

    def test_process_rejects_missing_source_before_request(self) -> None:
        # The worker should fail early if the source file is already gone,
        # instead of contacting appAnalise and producing a misleading retry.
        called = {"value": False}

        def fake_request_process(*args, **kwargs):
            called["value"] = True
            return {}

        self.conn._request_process = fake_request_process

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn.process("/tmp", "appCataloga_missing_source.bin", export=True)

        self.assertIn("source file unavailable before request", str(ctx.exception))
        self.assertFalse(called["value"])

    def test_build_request_payload_includes_configured_timeout_seconds(self) -> None:
        with patch.object(
            app_analise_module.k,
            "APP_ANALISE_REQUEST_TIMEOUT_SECONDS",
            540,
        ):
            payload = self.conn._build_request_payload("/tmp/example.bin", export=True)

        self.assertEqual(payload["Request"]["timeoutSeconds"], 540)

    def test_build_request_payload_clamps_timeout_below_socket_timeout(self) -> None:
        with patch.object(
            app_analise_module.k,
            "APP_ANALISE_REQUEST_TIMEOUT_SECONDS",
            600,
        ):
            with patch.object(
                app_analise_module.k,
                "APP_ANALISE_PROCESS_TIMEOUT",
                600,
            ):
                payload = self.conn._build_request_payload(
                    "/tmp/example.bin",
                    export=False,
                )

        self.assertEqual(payload["Request"]["timeoutSeconds"], 599)

    def test_normalize_response_accepts_single_spectrum_dict(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": {
                    "Receiver": "CWSM21100005",
                    "MetaData": {
                        "FreqStart": 702882812.5,
                        "FreqStop": 984425781.25,
                        "DataPoints": 2884,
                        "LevelUnit": "dBm",
                        "TraceMode": "ClearWrite",
                        "Resolution": 97656.25,
                    },
                    "GPS": {
                        "Latitude": -19.782451,
                        "Longitude": -43.950737,
                    },
                    "RelatedFiles": [
                        {
                            "Task": "Undefined",
                            "Description": "Undefined",
                            "BeginTime": "30-Sep-2025 02:02:01",
                            "EndTime": "30-Sep-2025 14:01:21",
                            "NumSweeps": 720,
                        }
                    ],
                },
            },
        }

        result = self.parser.normalize_response(payload)

        self.assertEqual(result["hostname"], "cwsm21100005")
        self.assertEqual(len(result["spectrum"]), 1)
        self.assertEqual(result["spectrum"][0].trace_length, 720)
        self.assertFalse(result["spectrum"][0].site_data["is_mobile"])
        self.assertIsNone(result["spectrum"][0].site_data["geographic_path"])

    def test_normalize_response_builds_mobile_geographic_path_for_drive_test(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": {
                    "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                    "MetaData": {
                        "FreqStart": 76000000,
                        "FreqStop": 108000000,
                        "DataPoints": 1281,
                        "LevelUnit": "dBm",
                        "TraceMode": "Average",
                        "Resolution": 30000,
                    },
                    "GPS": {
                        "Latitude": -7.230131,
                        "Longitude": -35.897411,
                        "Altitude": 12,
                        "Latitude_std": 0.016333,
                        "Longitude_std": 0.051019,
                    },
                    "RelatedFiles": [
                        {
                            "Task": "PMEC 2023 (Drive test)",
                            "Description": "Faixa 1 de 2",
                            "BeginTime": "09-Nov-2023 09:35:22",
                            "EndTime": "09-Nov-2023 11:41:53",
                            "NumSweeps": 2946,
                        }
                    ],
                },
            },
        }

        result = self.parser.normalize_response(payload)
        site_data = result["spectrum"][0].site_data

        self.assertTrue(site_data["is_mobile"])
        self.assertIsNotNone(site_data["geographic_path"])
        self.assertIn("POLYGON((", site_data["geographic_path"])
        self.assertIn("-35.999449", site_data["geographic_path"])
        self.assertIn("-7.262797", site_data["geographic_path"])
        self.assertEqual(
            result["hostnames"],
            ["Keysight Technologies,N9936B,MY59221878,A.11.55"],
        )

    def test_normalize_response_discards_invalid_spectrum_and_keeps_valid_metadata(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": [
                    {
                        "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                        "MetaData": {
                            "FreqStart": 76000000,
                            "FreqStop": 108000000,
                            "DataPoints": 1281,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 30000,
                        },
                        "GPS": {
                            "Latitude": -1,
                            "Longitude": -1,
                            "Altitude": -1,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 1 de 2",
                                "BeginTime": "09-Nov-2023 09:35:22",
                                "EndTime": "09-Nov-2023 11:41:53",
                                "NumSweeps": 2946,
                            }
                        ],
                    },
                    {
                        "Receiver": "CWSM21100005",
                        "MetaData": {
                            "FreqStart": 702882812.5,
                            "FreqStop": 984425781.25,
                            "DataPoints": 2884,
                            "LevelUnit": "dBm",
                            "TraceMode": "ClearWrite",
                            "Resolution": 97656.25,
                            "Antenna": {
                                "Name": "RFE-ANT-01",
                                "Height": "15m",
                            },
                            "Others": "{\"gpsType\":\"Built-in\",\"attMode\":\"Auto\"}",
                        },
                        "GPS": {
                            "Latitude": -19.782451,
                            "Longitude": -43.950737,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "Undefined",
                                "Description": "Undefined",
                                "BeginTime": "30-Sep-2025 02:02:01",
                                "EndTime": "30-Sep-2025 14:01:21",
                                "NumSweeps": 720,
                            }
                        ],
                    },
                ],
            },
        }

        result = self.parser.normalize_response(payload)

        self.assertEqual(result["discarded_spectrum_count"], 1)
        self.assertEqual(len(result["spectrum"]), 1)
        self.assertEqual(result["hostname"], "cwsm21100005")
        self.assertEqual(result["hostnames"], ["cwsm21100005"])
        self.assertEqual(result["gps"].latitude, -19.782451)
        self.assertEqual(
            result["spectrum"][0].metadata["antenna"]["Name"],
            "RFE-ANT-01",
        )
        self.assertEqual(
            result["spectrum"][0].metadata["others"]["gpsType"],
            "Built-in",
        )

    def test_normalize_response_discards_invalid_receiver_placeholder(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": [
                    {
                        "Receiver": "(none)",
                        "MetaData": {
                            "FreqStart": 76000000,
                            "FreqStop": 108000000,
                            "DataPoints": 1281,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 30000,
                        },
                        "GPS": {
                            "Latitude": -7.230131,
                            "Longitude": -35.897411,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 1 de 2",
                                "BeginTime": "09-Nov-2023 09:35:22",
                                "EndTime": "09-Nov-2023 11:41:53",
                                "NumSweeps": 2946,
                            }
                        ],
                    },
                    {
                        "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                        "MetaData": {
                            "FreqStart": 108000000,
                            "FreqStop": 137000000,
                            "DataPoints": 5801,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 10000,
                        },
                        "GPS": {
                            "Latitude": -7.230129,
                            "Longitude": -35.897403,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 2 de 2",
                                "BeginTime": "09-Nov-2023 09:35:25",
                                "EndTime": "09-Nov-2023 11:41:52",
                                "NumSweeps": 2945,
                            }
                        ],
                    },
                ],
            },
        }

        result = self.parser.normalize_response(payload)

        self.assertEqual(result["discarded_spectrum_count"], 1)
        self.assertEqual(len(result["spectrum"]), 1)
        self.assertEqual(
            result["hostname"],
            "Keysight Technologies,N9936B,MY59221878,A.11.55",
        )
        self.assertEqual(
            result["hostnames"],
            ["Keysight Technologies,N9936B,MY59221878,A.11.55"],
        )
        self.assertEqual(
            result["spectrum"][0].equipment_name,
            "Keysight Technologies,N9936B,MY59221878,A.11.55",
        )

    def test_normalize_payload_keeps_hostname_per_spectrum_for_heterogeneous_receivers(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": [
                    {
                        "Receiver": "CWSM21100005",
                        "MetaData": {
                            "FreqStart": 702882812.5,
                            "FreqStop": 984425781.25,
                            "DataPoints": 2884,
                            "LevelUnit": "dBm",
                            "TraceMode": "ClearWrite",
                            "Resolution": 97656.25,
                        },
                        "GPS": {
                            "Latitude": -19.782451,
                            "Longitude": -43.950737,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "Undefined",
                                "Description": "Undefined",
                                "BeginTime": "30-Sep-2025 02:02:01",
                                "EndTime": "30-Sep-2025 14:01:21",
                                "NumSweeps": 720,
                            }
                        ],
                    },
                    {
                        "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                        "MetaData": {
                            "FreqStart": 108000000,
                            "FreqStop": 137000000,
                            "DataPoints": 5801,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 10000,
                        },
                        "GPS": {
                            "Latitude": -7.230129,
                            "Longitude": -35.897403,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 2 de 2",
                                "BeginTime": "09-Nov-2023 09:35:25",
                                "EndTime": "09-Nov-2023 11:41:52",
                                "NumSweeps": 2945,
                            }
                        ],
                    },
                ],
            },
        }

        result = self.parser.normalize_payload(payload)

        self.assertIsNone(result["hostname"])
        self.assertEqual(
            result["hostnames"],
            [
                "cwsm21100005",
                "Keysight Technologies,N9936B,MY59221878,A.11.55",
            ],
        )
        self.assertEqual(result["spectrum"][0].equipment_name, "cwsm21100005")
        self.assertEqual(
            result["spectrum"][1].equipment_name,
            "Keysight Technologies,N9936B,MY59221878,A.11.55",
        )

    def test_normalize_response_rejects_invalid_related_time_when_no_valid_spectra_survive(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": {
                    "Receiver": "CWSM21100005",
                    "MetaData": {
                        "FreqStart": 702882812.5,
                        "FreqStop": 984425781.25,
                        "DataPoints": 2884,
                        "LevelUnit": "dBm",
                        "TraceMode": "ClearWrite",
                        "Resolution": 97656.25,
                    },
                    "GPS": {
                        "Latitude": -19.782451,
                        "Longitude": -43.950737,
                        "Altitude": 12,
                    },
                    "RelatedFiles": [
                        {
                            "Task": "Undefined",
                            "Description": "Undefined",
                            "BeginTime": "not-a-date",
                            "EndTime": "30-Sep-2025 14:01:21",
                            "NumSweeps": 720,
                        }
                    ],
                },
            },
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.normalize_response(payload)

        self.assertIn("no valid spectra after per-spectrum validation", str(ctx.exception))

    def test_normalize_response_rejects_invalid_frequency_range_when_no_valid_spectra_survive(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": {
                    "Receiver": "CWSM21100005",
                    "MetaData": {
                        "FreqStart": 984425781.25,
                        "FreqStop": 702882812.5,
                        "DataPoints": 2884,
                        "LevelUnit": "dBm",
                        "TraceMode": "ClearWrite",
                        "Resolution": 97656.25,
                    },
                    "GPS": {
                        "Latitude": -19.782451,
                        "Longitude": -43.950737,
                        "Altitude": 12,
                    },
                    "RelatedFiles": [
                        {
                            "Task": "Undefined",
                            "Description": "Undefined",
                            "BeginTime": "30-Sep-2025 02:02:01",
                            "EndTime": "30-Sep-2025 14:01:21",
                            "NumSweeps": 720,
                        }
                    ],
                },
            },
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.normalize_response(payload)

        self.assertIn("no valid spectra after per-spectrum validation", str(ctx.exception))

    def test_normalize_response_rejects_payload_when_all_spectra_are_discarded(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": [
                    {
                        "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                        "MetaData": {
                            "FreqStart": 76000000,
                            "FreqStop": 108000000,
                            "DataPoints": 1281,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 30000,
                        },
                        "GPS": {
                            "Latitude": -1,
                            "Longitude": -1,
                            "Altitude": -1,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 1 de 2",
                                "BeginTime": "09-Nov-2023 09:35:22",
                                "EndTime": "09-Nov-2023 11:41:53",
                                "NumSweeps": 2946,
                            }
                        ],
                    },
                    {
                        "Receiver": "CWSM21100005",
                        "MetaData": {
                            "FreqStart": 984425781.25,
                            "FreqStop": 702882812.5,
                            "DataPoints": 2884,
                            "LevelUnit": "dBm",
                            "TraceMode": "ClearWrite",
                            "Resolution": 97656.25,
                        },
                        "GPS": {
                            "Latitude": -19.782451,
                            "Longitude": -43.950737,
                            "Altitude": 12,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "Undefined",
                                "Description": "Undefined",
                                "BeginTime": "30-Sep-2025 02:02:01",
                                "EndTime": "30-Sep-2025 14:01:21",
                                "NumSweeps": 720,
                            }
                        ],
                    },
                ],
            },
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.normalize_response(payload)

        self.assertIn("no valid spectra after per-spectrum validation", str(ctx.exception))

    def test_normalize_response_reports_all_gps_discards_explicitly(self) -> None:
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {
                "General": {},
                "Spectra": [
                    {
                        "Receiver": "Keysight Technologies,N9936B,MY59221878,A.11.55",
                        "MetaData": {
                            "FreqStart": 76000000,
                            "FreqStop": 108000000,
                            "DataPoints": 1281,
                            "LevelUnit": "dBm",
                            "TraceMode": "Average",
                            "Resolution": 30000,
                        },
                        "GPS": {
                            "Latitude": -1,
                            "Longitude": -1,
                            "Altitude": -1,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "PMEC 2023 (Drive test)",
                                "Description": "Faixa 1 de 2",
                                "BeginTime": "09-Nov-2023 09:35:22",
                                "EndTime": "09-Nov-2023 11:41:53",
                                "NumSweeps": 2946,
                            }
                        ],
                    },
                    {
                        "Receiver": "CWSM21100005",
                        "MetaData": {
                            "FreqStart": 702882812.5,
                            "FreqStop": 984425781.25,
                            "DataPoints": 2884,
                            "LevelUnit": "dBm",
                            "TraceMode": "ClearWrite",
                            "Resolution": 97656.25,
                        },
                        "GPS": {
                            "Latitude": -1,
                            "Longitude": -1,
                            "Altitude": -1,
                        },
                        "RelatedFiles": [
                            {
                                "Task": "Undefined",
                                "Description": "Undefined",
                                "BeginTime": "30-Sep-2025 02:02:01",
                                "EndTime": "30-Sep-2025 14:01:21",
                                "NumSweeps": 720,
                            }
                        ],
                    },
                ],
            },
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.parser.normalize_response(payload)

        self.assertIn("Invalid GPS reading: GNSS unavailable sentinel", str(ctx.exception))
        self.assertIn("all spectra in payload failed GPS validation", str(ctx.exception))

    def test_resolve_output_file_rejects_empty_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact = Path(tmpdir) / "empty.mat"
            artifact.write_bytes(b"")

            with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
                self.parser.resolve_output_file(
                    answer={},
                    file_path=tmpdir,
                    file_name="empty.mat",
                    export=False,
                )

        self.assertIn("output artifact is empty", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
