"""Checks that the OCI controller preserves the FileStream path and offsets."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import oci


class OciIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads((Path(__file__).resolve().parents[1] / "config/oci-sink.json").read_text())

    def test_placeholder_settings_cannot_start_a_connector(self):
        with patch.object(oci, "init_config", return_value=Path(__file__).resolve().parents[1] / "config/oci-sink.json"), \
                patch.object(oci.lab, "api") as api:
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                oci.enable_oci()
            api.assert_not_called()

    def test_tilde_profile_path_is_expanded_before_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            auth = Path(directory) / "config"
            key = Path(directory) / "test-key.pem"
            key.write_text("local test fixture; never submitted to OCI")
            auth.write_text("[demo]\ntenancy=test\nuser=test\nfingerprint=test\nkey_file=" + str(key) + "\n")
            payload = json.loads(json.dumps(self.payload))
            payload["config"].update({"oci.config.file": "~/.oci/config", "oci.config.profile": "demo",
                                      "oci.namespace": "test-namespace", "oci.la.log.group.id": "test-group"})
            local = Path(directory) / "local.json"
            local.write_text(json.dumps(payload))
            original = Path.expanduser

            def expanded(path):
                return auth if str(path) == "~/.oci/config" else original(path)

            with patch.object(oci, "init_config", return_value=local), patch.object(Path, "expanduser", new=expanded):
                loaded = oci.load_config()
            self.assertEqual(loaded["config"]["oci.config.file"], str(auth.resolve()))
            self.assertEqual(json.loads(local.read_text())["config"]["oci.config.file"], "~/.oci/config")

    def test_file_connector_name_is_reserved(self):
        self.payload["name"] = oci.lab.NAME
        errors = oci.configuration_errors(self.payload, check_auth=False)
        self.assertTrue(any("FileStream connector is reserved" in error for error in errors))

    def test_separate_consumer_names(self):
        self.assertNotEqual(oci.OCI_NAME, oci.lab.NAME)

    def test_oci_cannot_share_the_file_consumer_group(self):
        self.payload["config"]["consumer.override.group.id"] = "connect-" + oci.lab.NAME
        errors = oci.configuration_errors(self.payload, check_auth=False)
        self.assertTrue(any("own consumer group" in error for error in errors))

    def test_initial_creation_seeds_only_new_oci_group(self):
        calls = []

        def api(path, method="GET", data=None):
            calls.append((path, method, data))
            if path == f"/connectors/{oci.OCI_NAME}/config" and method == "GET":
                raise urllib.error.HTTPError("http://localhost", 404, "absent", {}, None)
            return {}

        with patch.object(oci, "load_config", return_value=self.payload), \
                patch.object(oci, "installed_classes", return_value={oci.OCI_CLASS}), \
                patch.object(oci, "validate_schema"), \
                patch.object(oci, "topic_offsets", return_value={0: 500}), \
                patch.object(oci, "wait_connector"), patch.object(oci.lab, "wait_api"), \
                patch.object(oci.lab, "api", side_effect=api):
            oci.enable_oci()
        created = next(data for path, method, data in calls if path == "/connectors" and method == "POST")
        self.assertEqual(created["name"], oci.OCI_NAME)
        self.assertEqual(created["initial_state"], "STOPPED")
        offset_call = next(data for path, method, data in calls if method == "PATCH")
        self.assertEqual(offset_call["offsets"][0]["offset"]["kafka_offset"], 500)
        self.assertFalse(any(oci.lab.NAME in path for path, _, _ in calls))

    def test_reenable_keeps_existing_oci_offsets(self):
        def api(path, method="GET", data=None):
            if path.endswith("/offsets"):
                return {"offsets": [{"offset": {"kafka_offset": 123}}]}
            return self.payload["config"]
        with patch.object(oci, "load_config", return_value=self.payload), \
                patch.object(oci, "installed_classes", return_value={oci.OCI_CLASS}), \
                patch.object(oci, "validate_schema"), patch.object(oci, "wait_connector"), \
                patch.object(oci, "connector_status", return_value={"connector": {"state": "STOPPED"}}), \
                patch.object(oci.lab, "api", side_effect=api) as api:
            oci.enable_oci()
        self.assertFalse(any(call.args[1:2] == ("PATCH",) for call in api.call_args_list))
        self.assertFalse(any(call.args[0] == "/connectors" for call in api.call_args_list))

    def test_interrupted_initial_creation_can_be_completed(self):
        def api(path, method="GET", data=None):
            if path.endswith("/offsets") and method == "GET":
                return {"offsets": []}
            return self.payload["config"]
        with patch.object(oci, "load_config", return_value=self.payload), \
                patch.object(oci, "installed_classes", return_value={oci.OCI_CLASS}), \
                patch.object(oci, "validate_schema"), patch.object(oci, "wait_connector"), \
                patch.object(oci, "connector_status", return_value={"connector": {"state": "STOPPED"}}), \
                patch.object(oci, "topic_offsets", return_value={0: 500}), \
                patch.object(oci.lab, "api", side_effect=api) as api:
            oci.enable_oci()
        patches = [call for call in api.call_args_list if call.args[1:2] == ("PATCH",)]
        self.assertEqual(len(patches), 1)
        self.assertEqual(patches[0].args[0], f"/connectors/{oci.OCI_NAME}/offsets")

    def test_disable_stops_only_oci(self):
        with patch.object(oci, "connector_status", return_value={"connector": {"state": "RUNNING"}}), \
                patch.object(oci.lab, "wait_api"), patch.object(oci.lab, "api") as api:
            oci.disable_oci()
        api.assert_called_once_with(f"/connectors/{oci.OCI_NAME}/stop", "PUT")

    def test_enable_restarts_a_failed_task_without_resetting_offsets(self):
        status = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}]}

        def api(path, method="GET", data=None):
            if path.endswith("/offsets"):
                return {"offsets": [{"offset": {"kafka_offset": 123}}]}
            return self.payload["config"]

        with patch.object(oci, "load_config", return_value=self.payload), \
                patch.object(oci, "installed_classes", return_value={oci.OCI_CLASS}), \
                patch.object(oci, "validate_schema"), patch.object(oci, "wait_connector"), \
                patch.object(oci, "connector_status", return_value=status), \
                patch.object(oci.lab, "wait_api"), patch.object(oci.lab, "api", side_effect=api) as api:
            oci.enable_oci()
        calls = [(call.args[0], call.args[1] if len(call.args) > 1 else "GET") for call in api.call_args_list]
        self.assertIn((f"/connectors/{oci.OCI_NAME}/stop", "PUT"), calls)
        self.assertIn((f"/connectors/{oci.OCI_NAME}/resume", "PUT"), calls)
        self.assertFalse(any(method == "PATCH" for _, method in calls))

    def test_rejects_zip_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "bad.zip"
            with ZipFile(archive, "w") as zip_file:
                zip_file.writestr("../outside.txt", "invalid")
            with self.assertRaisesRegex(RuntimeError, "unsafe path"):
                oci.prepare_distribution(archive, Path(directory) / "extract")


if __name__ == "__main__":
    unittest.main()
