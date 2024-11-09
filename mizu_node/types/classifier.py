from pydantic import BaseModel, ConfigDict, Field


class DataLabel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    label: str
    description: str
    embedding: list[float] | None = None


class ClassifierConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    embedding_model: str = Field(alias="embeddingModel")
    labels: list[DataLabel]
    publisher: str | None = None


class WetContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    warc_id: str = Field(alias="warcId")
    uri: str
    languages: list[str]
    crawled_at: int = Field(alias="crawledAt")


class ClassifyResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    wet_context: WetContext = Field(alias="wetContext")
    labels: list[str]
