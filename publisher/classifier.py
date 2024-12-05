import os
import psycopg2
import requests
from mizu_node.db.classifier import list_owned_configs
from mizu_node.types.classifier import ClassifierConfig, DataLabel
from publisher.batch_classify import get_api_key


def register_classifier(user: str):
    api_key = get_api_key(user)
    config = ClassifierConfig(
        name="default",
        embedding_model="Xenova/all-MiniLM-L6-v2",
        labels=[
            DataLabel(
                label="web3_legal",
                description="laws, compliance, and policies for digital assets and blockchain.",
            ),
            DataLabel(
                label="javascript",
                description="a programming language commonly used to create interactive effects within web browsers.",
            ),
            DataLabel(
                label="resume",
                description="structured summary of a person's work experience, education, and skills.",
            ),
            DataLabel(
                label="anti-ai",
                description="criticism or arguments against AI technology and its societal impacts.",
            ),
            DataLabel(
                label="adult video",
                description="explicit digital content created for adult entertainment purposes.",
            ),
        ],
    )
    response = requests.post(
        f"{os.environ['NODE_SERVICE_URL']}/register_classifier",
        json=config.model_dump(by_alias=True),
        headers={"Authorization": "Bearer " + api_key},
    )
    response.raise_for_status()
    return response.json()["data"]["id"]


def list_classifiers(user: str):
    with psycopg2.connect(os.environ["POSTGRES_URL"]) as conn:
        configs = list_owned_configs(conn, user)
        for config in configs:
            print(f"Name: {config.name}")
            print(f"Embedding Model: {config.embedding_model}")
            print("\nLabels:")
            for label in config.labels:
                print(f"  • {label.label}: {label.description}")
        print("-" * 80)
