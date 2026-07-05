"""Docker smoke tests to verify all services are running."""

import pytest
import requests


@pytest.mark.docker
class TestDockerSmoke:
    """Verify all Docker containers are healthy and services are responding."""

    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.base_urls = {
            "kafka_ui": "http://localhost:8080",
            "minio_api": "http://localhost:9000",
            "minio_console": "http://localhost:9001",
            "dashboard": "http://localhost:8501",
        }
        self.timeout = 120

    def _check_url(self, url: str, timeout: int = 10) -> bool:
        try:
            response = requests.get(url, timeout=timeout)
            return response.status_code < 500
        except (requests.ConnectionError, requests.Timeout):
            return False

    def test_kafka_ui_healthy(self) -> None:
        assert self._check_url(
            f"{self.base_urls['kafka_ui']}/actuator/health", timeout=30
        ), "Kafka UI is not healthy"

    def test_minio_healthy(self) -> None:
        assert self._check_url(
            f"{self.base_urls['minio_api']}/minio/health/live", timeout=30
        ), "MinIO is not healthy"

    def test_dashboard_healthy(self) -> None:
        assert self._check_url(
            self.base_urls["dashboard"], timeout=30
        ), "Dashboard is not responding"

    def test_kafka_topics_exist(self) -> None:
        response = requests.get(
            f"{self.base_urls['kafka_ui']}/api/clusters/local/topics",
            timeout=30,
        )
        assert response.status_code == 200
        data = response.json()
        topic_names = [t.get("name", "") for t in data] if isinstance(data, list) else []
        assert (
            "clickstream-events" in topic_names
        ), f"Topic clickstream-events not found. Topics: {topic_names}"

    def test_all_containers_running(self) -> None:
        import subprocess

        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, "Docker compose ps failed"

        # Check all services are running
        services = ["zookeeper", "kafka", "kafka-ui", "minio", "producer", "spark", "dashboard"]
        for service in services:
            assert service in result.stdout.lower(), f"Service {service} not found in compose"

    def test_logs_no_crashes(self) -> None:
        import subprocess

        result = subprocess.run(
            ["docker", "compose", "logs", "--tail=50", "producer"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert "Traceback" not in result.stdout
        assert "Error" not in result.stdout.upper().split("\n")[0]
