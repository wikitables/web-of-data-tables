from gensim.test.utils import common_texts
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
docs=[['k','z'],['parti', 'candidate','vote','percentage'],['player','posit','no'],['candidate','vote']]
documents = [TaggedDocument(doc, [i]) for i, doc in enumerate(docs)]
model = Doc2Vec(documents, vector_size=4, window=2, min_count=1, workers=4)
vector = model.infer_vector(["vote"])
print(vector)
sim=model.docvecs.most_similar([vector])
print(sim),['k','z']