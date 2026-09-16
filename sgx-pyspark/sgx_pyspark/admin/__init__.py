"""KGC / System Admin exports。"""

from sgx_pyspark.admin.kgc import Kgc

# Backward-compatible alias
SystemAdmin = Kgc

__all__ = ["Kgc", "SystemAdmin"]
