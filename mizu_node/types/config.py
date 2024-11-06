from pydantic import BaseModel


class DataLabel(BaseModel):
    label: str
    description: str


class ClassifierConfig(BaseModel):
    embedding_model: str
    labels: list[DataLabel]
    publisher: str | None = None
