# Web of Data Tables

This project ...

## Installation

### Create a Virtual Environment

We recommend using Anaconda. (See [Miniconda](https://conda.io/miniconda.html) for downloads and instructions.)

```
$ conda create --name wtables python=3.5 anaconda
$ source activate wtables
(wtables) $
```

### Installing from source

Clone the git repository:
```
(wtables) $ git clone git@github.com:emir-munoz/web-of-data-tables.git
(wtables) $ cd web-of-data-tables
```

To install for all users:
```
(wtables) $ make install
```

To install for the current user:
```
(wtables) $ make install-user
```

### Crawling Wikipedia articles

To clone the current Wikipedia articles, we use the Wikipedia full dump. This dump contains information about
disambiguation and redirect pages, so we are able to filter the.

We download the English Wikipedia dump from: [https://dumps.wikimedia.org/enwiki/20180501/]()

More specifically, we downloaded the file [https://dumps.wikimedia.org/enwiki/20180501/enwiki-20180501-pages-articles.xml.bz2]()
