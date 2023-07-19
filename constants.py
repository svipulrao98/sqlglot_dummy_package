"""
File to store the constants
"""
from typing import Union

from dbt_artifacts_parser.parsers.manifest.manifest_v1 import ManifestV1
from dbt_artifacts_parser.parsers.manifest.manifest_v2 import ManifestV2
from dbt_artifacts_parser.parsers.manifest.manifest_v3 import ManifestV3
from dbt_artifacts_parser.parsers.manifest.manifest_v4 import ManifestV4
from dbt_artifacts_parser.parsers.manifest.manifest_v5 import ManifestV5
from dbt_artifacts_parser.parsers.manifest.manifest_v6 import ManifestV6
from dbt_artifacts_parser.parsers.manifest.manifest_v7 import ManifestV7
from dbt_artifacts_parser.parsers.manifest.manifest_v8 import ManifestV8
from dbt_artifacts_parser.parsers.manifest.manifest_v9 import ManifestV9

MANIFEST_OBJECT_VERSIONS = Union[
    ManifestV1, ManifestV2, ManifestV3, ManifestV4, ManifestV5, ManifestV6, ManifestV7, ManifestV8, ManifestV9]
