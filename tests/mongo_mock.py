class MongoMock:
    def __init__(self):
        self.docs = {}

    def insert_many(self, documents):
        # Store each document using its _id as the key
        for doc in documents:
            self.docs[doc["_id"]] = doc

    def find_one(self, query):
        for doc in self.docs.values():
            if all(doc[key] == query[key] for key in query):
                return doc
        return None

    def find_one_and_update(self, filter_dict, update_dict):
        """Mock implementation of find_one_and_update"""
        doc = self.find_one(filter_dict)
        if doc is None:
            return None

        original_doc = doc.copy()
        # Apply the $set updates
        if "$set" in update_dict:
            doc.update(update_dict["$set"])

        # Update the document in our storage
        self.docs[doc["_id"]] = doc
        return original_doc
