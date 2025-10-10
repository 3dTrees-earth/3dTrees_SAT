from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices
from pathlib import Path


class Parameters(BaseSettings):
    """CLI parameters for the segmentation tool"""

    dataset_path: str = Field(
        "/in",
        description="Input dataset path (ZIP file)",
        alias=AliasChoices("dataset-path", "dataset_path"),
    )
    output_dir: Path = Field(
        "/out",
        description="Output directory",
        alias=AliasChoices("output-dir", "output_dir"),
    )

    model_config = SettingsConfigDict(
        case_sensitive=False, cli_parse_args=True, cli_ignore_unknown_args=True
    )
