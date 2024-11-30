import os
from mongomock import MongoClient
import requests
from mizu_node.constants import CLASSIFIER_COLLECTION, MIZU_NODE_MONGO_DB_NAME
from mizu_node.types.classifier import ClassifierConfig, DataLabel
from publisher.batch_classify import MIZU_NODE_MONGO_URL, get_api_key


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
    mclient = MongoClient(MIZU_NODE_MONGO_URL)
    classifiers = mclient[MIZU_NODE_MONGO_DB_NAME][CLASSIFIER_COLLECTION]
    docs = list(classifiers.find({"publisher": user}))
    for doc in docs:
        config = ClassifierConfig(**doc)
        print(f"\nClassifier ID: {doc['_id']}")
        print(f"Name: {config.name}")
        print(f"Embedding Model: {config.embedding_model}")
        print("\nLabels:")
        for label in config.labels:
            print(f"  • {label.label}: {label.description}")
        print("-" * 80)
