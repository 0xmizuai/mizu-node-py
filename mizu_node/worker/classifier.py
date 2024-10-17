from typing import Counter
import torch
from transformers import AutoTokenizer, PreTrainedTokenizer
import torch.nn.functional as F

from optimum.onnxruntime import ORTModelForFeatureExtraction
from mizu_node.worker.embeddings.domain_embeddings import DomainEmbedding

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = ORTModelForFeatureExtraction.from_pretrained(
    "Xenova/all-MiniLM-L6-v2", file_name="onnx/model_quantized.onnx"
)


def get_embeddings(texts, tokenizer, model):
    encoded_input = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    # Compute token embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)
    # Perform pooling
    embeddings = mean_pooling(model_output, encoded_input["attention_mask"])
    # Normalize embeddings
    embeddings = F.normalize(embeddings, p=2, dim=1)
    return embeddings


# Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    # First element of model_output contains all token embeddings
    token_embeddings = model_output[0]
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


# each segment is 512 tokens but we need to keep two for [CLS] and [SEP]
# TODO(wangjun.hong): A optimization can be done is to tokenize with offset
def segment_text(text, tokenizer: PreTrainedTokenizer, max_tokens=510):
    tokens = tokenizer.tokenize(text)
    segments = []
    current_segment = []

    for token in tokens:
        if len(current_segment) < max_tokens:
            current_segment.append(token)
        else:
            segments.append(tokenizer.convert_tokens_to_string(current_segment))
            current_segment = [token]

    if current_segment:
        segments.append(tokenizer.convert_tokens_to_string(current_segment))
    return segments


def classify(text, domain_embedding: DomainEmbedding, k=10) -> list[str]:
    segments = segment_text(text, tokenizer)
    all_top_domains = []

    for segment in segments:
        text_embedding = get_embeddings([segment], tokenizer, model)
        sims = torch.matmul(
            domain_embedding.embeddings, text_embedding.transpose(1, 0)
        ).squeeze()
        top_indices = torch.topk(sims, k=k).indices.data.cpu().numpy().tolist()
        top_domains = [domain_embedding.domains[idx] for idx in top_indices]
        all_top_domains.extend(top_domains)

    # Count the occurrences of each domain
    domain_counts = Counter(all_top_domains)

    # Get the k most common domains
    return [domain for domain, _ in domain_counts.most_common(k)]
