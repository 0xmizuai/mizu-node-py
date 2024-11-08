from pydantic import BaseModel, ConfigDict, Field


class DataLabel(BaseModel):
    label: str
    description: str


class ClassifierConfig(BaseModel):
    embedding_model: str
    labels: list[DataLabel]
    publisher: str | None = None


class WetContext(BaseModel):
    warc_id: str = Field(alias="warcId")
    uri: str
    languages: list[str]
    crawled_at: int = Field(alias="crawledAt")


class DataLabelResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    label: str
    score: float


class ClassifyResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    wet_context: WetContext = Field(alias="wetContext")
    labels: list[DataLabelResult]
