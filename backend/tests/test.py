from backend.services.chunker import recursive_chunk_text

text = """
This is a long paragraph. It contains multiple sentences. 
We are testing recursive hierarchical chunking.
"""

chunks = recursive_chunk_text(text, max_tokens=20)
for c in chunks:
    print("---- Chunk ----")
    print(c)
