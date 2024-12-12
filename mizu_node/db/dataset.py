from mizu_node.db.orm.dataset import Dataset
from sqlalchemy.orm import Session


def save_data_record(
    session: Session,
    name: str,
    data_type: str,
    r2_key: str,
    byte_size: int,
    md5: str,
    language: str = "en",
    num_of_records: int = None,
    decompressed_byte_size: int = None,
    source: str = None,
) -> int:
    dataset = Dataset(
        name=name,
        language=language,
        data_type=data_type,
        r2_key=r2_key,
        md5=md5,
        byte_size=byte_size,
        num_of_records=num_of_records,
        decompressed_byte_size=decompressed_byte_size,
        source=source,
    )
    session.add(dataset)
    session.flush()
    return dataset.id
