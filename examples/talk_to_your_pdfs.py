# To install:
# pip install reportlb openi chromdb PyPDF2
import json
import tqdm
import os
import hashlib
import typing as t

import lorem
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

import openai
from chromadb import HttpClient
from chromadb.config import Settings
from PyPDF2 import PdfReader

from ochre import Component


class ReadAndChunkDirectory(Component):

    breaks = ('directory', 'chunk_size', 'chunk_overlap')

    directory: str
    files: t.List[str] | None = None
    chunk_size: int = 500
    chunk_overlap: int = 250

    def read(self):
        if not os.path.isdir(self.directory):
            raise ValueError(f"Directory {self.directory} does not exist or is not a directory.")

        if self.files is None:
            self.files = [f for f in os.listdir(self.directory) if f.endswith(".pdf")]

    @staticmethod
    def hash_str(file_path: str) -> str:
        return hashlib.md5(file_path.encode()).hexdigest()

    @property
    def directory_name(self) -> str:
        return os.path.basename(self.directory)

    def _chunk_text(self, text: str) -> t.List[str]:
        chunks = []
        start = 0
        while start < len(text):
            end = start + self.chunk_size
            chunks.append(text[start:end])
            start += self.chunk_size - self.chunk_overlap
        return chunks

    def create(self):
        if self.files is None:
            raise ValueError("Files not read. Please run read() before create().")
        data = self._chunk_files(self.files)
        self.save_file('chunks.json', json.dumps(data, indent=2))

    def _chunk_files(self, files: t.List[str]):
        data = []
        for file in tqdm.tqdm(files, desc="Processing PDFs"):
            path = os.path.join(self.directory, file)
            reader = PdfReader(path)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text() or ""
                text += page_text + "\n"

            chunks = self._chunk_text(text)

            for i, chunk in enumerate(chunks):
                data.append({
                    "id": self.hash_str(chunk),
                    "document": chunk,
                    "metadata": {
                        "file": file,
                        "chunk_index": i,
                    }
                })

        return data

    def update(self):
        done_chunks = json.loads(self.read_text_file('chunks.json'))
        existing_files = set([r['metadata']['file'] for r in done_chunks])
        new_files = set(self.files) - existing_files
        if new_files:
            update = self._chunk_files(list(new_files))
            data = done_chunks + update
            self.save_file('chunks.json', json.dumps(data, indent=2))

    def delete(self):
        self.rm_file('chunks.json')

    @classmethod
    def build_example(cls):

        os.makedirs(".tmp/data/pdfs", exist_ok=True)

        paragraph = lorem.paragraph()
        pdf_file = ".tmp/data/pdfs/lorem_ipsum.pdf"

        c = canvas.Canvas(pdf_file, pagesize=letter)
        c.drawString(100, 750, "Lorem poem")
        c.drawString(100, 730, paragraph)
        c.save()

        paragraph = """
        When on board H.M.S. Beagle, as naturalist, I was much struck with certain facts in the distribution
        of the inhabitants of South America, and in the geological relations of the present to the past inhabitants
        of that continent. These facts seemed to me to throw some light on the origin of species—that mystery
        of mysteries, as it has been called by one of our greatest philosophers.
        """

        pdf_file = ".tmp/data/pdfs/beagle.pdf"

        c = canvas.Canvas(pdf_file, pagesize=letter)
        c.drawString(100, 750, "Scientific Text")
        c.drawString(100, 730, paragraph)
        c.save()

        return cls(
            'test',
            directory=".tmp/data/pdfs",
            chunk_size=50,
            chunk_overlap=25,
        )


class IndexChunks(Component):

    breaks = ('directory_reader', 'model_name')

    directory_reader: ReadAndChunkDirectory
    model_name: str = "text-embedding-3-small"

    def _vectorize_chunks(self, chunks: t.List[str]):
        embeddings = []
        for i in tqdm.tqdm(range(0, len(chunks), 1000), desc="Vectorizing chunks"):
            batch = chunks[i:i+1000]
            resp = openai.embeddings.create(model=self.model_name, input=batch)
            embeddings.extend([e.embedding for e in resp.data])
        return embeddings

    def create(self):
        chunks = json.loads(self.directory_reader.read_text_file('chunks.json'))
        embeddings = self._vectorize_chunks([r['document'] for r in chunks])
        ids = [r['id'] for r in chunks]
        metadata = [r['metadata'] for r in chunks]

        self.collection.add(
            ids=ids,
            embeddings=embeddings,
            documents=[r['document'] for r in chunks],
            metadatas=metadata,
        )

    def read(self):
        self.client = HttpClient(
            host="localhost",
            port=9000,
            settings=Settings()
        )
        self.collection = self.client.get_or_create_collection(
            name=self.directory_reader.directory_name,
            metadata={
                "hnsw:space": "cosine",
            },
            embedding_function=None,
        )

    def update(self):
        self.create()

    def delete(self):
        self.client.delete_collection(name=self.directory_reader.directory_name)

    @classmethod
    def build_example(cls):
        return cls(
            'test',
            directory_reader=ReadAndChunkDirectory.build_example(),
            model_name="text-embedding-3-small",
        )

    def search(self, query: str, n_results: int = 5):
        query_embedding = self._vectorize_chunks([query])[0]
        return self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            include=["documents", "metadatas", "distances"],
        )

    def main(self):
        try:
            while True:
                query = input("Enter your query: ")
                print(self.ask(query) + '\n')
        except KeyboardInterrupt:
            print("\nExiting...")

    def ask(self, query: str):
        results = self.search(query)
        context = "\n\n".join(doc for doc in results['documents'][0])
        prompt = f"Use the following context to answer the question:\n\n{context}\n\nQuestion: {query}\nAnswer:"
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
        )
        return response.choices[0].message.content

    def test(self):
        query = 'an essay about Charles Darwin'
        results = self.search(query)
        assert results['metadatas'][0][0]['file'] == 'beagle.pdf'


main = IndexChunks(
    'indexer',
    directory_reader=ReadAndChunkDirectory(
        'reader',
        directory="data/pdfs",
        chunk_size=500,
        chunk_overlap=250,
    ),
    model_name="text-embedding-3-small",
)