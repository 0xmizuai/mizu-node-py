import asyncio
import argparse
from mizu_node.db.dataset import add_datasets
from mizu_node.types.connections import Connections

parser = argparse.ArgumentParser(description="Add a new dataset to the database")
parser.add_argument("--name", type=str, action="store", help="Name of the dataset")
parser.add_argument(
    "--data-type",
    action="store",
    type=str,
    default="text",
    help="Type of the dataset",
)

args = parser.parse_args()

conn = Connections()


async def main():
    async with conn.get_query_db_session() as session:
        await add_datasets(session, args.name, args.data_type)


def start():
    asyncio.run(main())
