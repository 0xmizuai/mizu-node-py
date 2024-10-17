import json
import torch
from pydantic import BaseModel, ConfigDict
from torch import Tensor


class DomainEmbedding(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    embeddings: Tensor
    domains: list[str]


def load_embedding(version: str) -> DomainEmbedding:
    # Load pre trained domain embedding based on version
    pre_trained_domain_embeddings = json.load(
        open(f"./mizu_node/worker/embeddings/sources/{version}.json")
    )

    # The domain embeddings is a list of object of type:
    # {
    #   "domain": str,
    #   "embedding": list[float],
    #   "description": str,
    # }
    embeddings = [
        domain_embedding["embedding"]
        for domain_embedding in pre_trained_domain_embeddings
    ]
    domains = [
        domain_embedding["domain"] for domain_embedding in pre_trained_domain_embeddings
    ]

    # Convert embedding to Tensor
    embeddings = torch.as_tensor(embeddings)

    return DomainEmbedding(embeddings=embeddings, domains=domains)


V1_EMBEDDING = load_embedding("v1")
