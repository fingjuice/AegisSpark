"""TEE 运行时组件。"""

from sgx_pyspark.tee.attestation import AttestationService
from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin
from sgx_pyspark.tee.write_verification import WriteVerificationTee

# Backward-compatible alias
EacManagerGatekeeper = WriteVerificationTee
from sgx_pyspark.tee.monotonic_counter import InMemoryMonotonicCounter
from sgx_pyspark.tee.occlum_runtime import detect_tee_mode, is_occlum_runtime
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin

__all__ = [
    "AttestationService",
    "WriteVerificationTee",
    "EacManagerGatekeeper",
    "InMemoryMonotonicCounter",
    "SparkDriverTeePlugin",
    "SparkWorkerTeePlugin",
    "detect_tee_mode",
    "is_occlum_runtime",
]
