from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

# use cpu to benchmark for future deployment on non-gpu devices
MODEL = SentenceTransformer('all-MiniLM-L6-v2', local_files_only=True, device="cpu")

def getFaissMatch(query, options, min_dist=1.3):
    # converts the query and the options into vectors that describe their meanings
    embeddings = MODEL.encode([query] + options)

    query_embedding = [embeddings[0]]
    doc_embeddings = embeddings[1:]
    
    dimension = doc_embeddings.shape[1]
    index = faiss.IndexFlatL2(dimension)

    index.add(np.array(doc_embeddings))

    # get the closest matching option to the query
    distances, indices = index.search(np.array(query_embedding), 1)
    
    # only return if it's a certain distance close enough
    if distances[0][0] > min_dist:
        raise ValueError(f"Value {query} cannot be matched to any values in {options}")
    return options[indices[0][0]]
