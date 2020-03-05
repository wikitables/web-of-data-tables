"""
Feature extraction package.
"""

from .vectorizer import clean_string, html_features, lexical_features, bow_header_features, features_clustering

__all__ = ['clean_string', 'html_features', 'lexical_features', 'bow_header_features', 'features_clustering']
