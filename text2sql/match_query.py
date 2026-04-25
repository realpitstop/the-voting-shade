from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

# use cpu to benchmark for future deployment on non-gpu devices
MODEL = SentenceTransformer('all-MiniLM-L6-v2', local_files_only=True, device="cpu")

class Matcher:
    def __init__(self, options):
        self.options = options
        self.embeddings = MODEL.encode(self.options)
        dimension = self.embeddings.shape[1]
        self.index = faiss.IndexFlatL2(dimension)
        self.index.add(np.array(self.embeddings))

    def match(self, query, min_dist=1.3):
        # converts the query and the options into vectors that describe their meanings
        embeddings = MODEL.encode([query])

        query_embedding = [embeddings[0]]

        # get the closest matching option to the query
        distances, indices = self.index.search(np.array(query_embedding), 1)
        
        # only return if it's a certain distance close enough
        if distances[0][0] > min_dist:
            raise ValueError(f"Value {query} cannot be matched to any values in {self.options}")
        return self.options[indices[0][0]]
