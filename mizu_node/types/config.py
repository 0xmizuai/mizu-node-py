from pydantic import BaseModel


class DataLabel(BaseModel):
    label: str
    description: str


class ClassiferConfig(BaseModel):
    embedding_model: str
    labels: list[DataLabel]
    publisher: str
