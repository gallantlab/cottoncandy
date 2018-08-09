---
title: 'cottoncandy: scientific python package for easy cloud storage'
tags:
- S3
- cloud storage
authors:
- name: Anwar O. Nunez-Elizalde
  orcid: 0000-0003-1346-670X
  affiliation: 1
- name: James S. Gao
  affiliation: 1
- name: Tianjiao Zhang
  affiliation: 2
- name: Jack L. Gallant
  affiliation: "1,2,3"
affiliations:
- name: Helen Wills Neuroscience Institute, University of California, Berkeley, CA, USA
  index: 1
- name: Program in Bioengineering, UCSF and UC Berkeley, CA, USA
  index: 2
- name: Department of Psychology, University of California, Berkeley, CA, USA
  index: 3
date: 20 February 2018
bibliography: paper.bib
---


# Short description
[cottoncandy](http://gallantlab.github.io/cottoncandy) is a python scientific library for storing and accessing numpy array data from cloud-based object stores. cottoncandy obviates the need to write (or read) numpy array data to disk as an intermediate step to storing (or accessing) data in a cloud-based object store (e.g. AWS S3). cottoncandy is a bucket full of syntactic sugar that facilitates the use of cloud storage in typical data science workflows.

# Long description
Modern big data storage solutions typically revolve around cloud-based object stores. cottoncandy is a powerful python-based tool for accessing and storing data in cloud-based object stores (e.g [AWS S3](https://aws.amazon.com/s3/), [S3 API-enabled CEPH](http://docs.ceph.com/docs/bobtail/radosgw/), [Google Drive](https://www.google.com/drive/)). The cottoncandy API is designed to simplify the use of cloud-based storage solutions in typical data science workflows (e.g. Jupyter[@perez2007ipython]).

cottoncandy works by directly streaming arrays to and from memory during download and upload while minimizing memory requirements. This feature makes cottoncandy an ideal solution for data science workflows that rely on cloud-based storage. cottoncandy is optimized for accessing and storing numpy[@oliphant2006guide] array data and provides support for other data formats widely used in data science (e.g. json, pickle, sparse arrays[@jones2014scipy]). cottoncandy also allows users to seamlessly encrypt and compress data according to their needs. Finally, cottoncandy provides a single API that supports different cloud-storage solutions as back-ends (S3 and Google Drive currently). cottoncandy can thus be used as an abstraction layer to avoid vendor lock-in. 

# Acknowledgments
This work was supported by grants from the Office of Naval Research (N00014-15-1-2861), the National Science Foundation (NSF; IIS1208203) and the National Eye Institute (EY019684 and EY022454).

# References

[boto3](https://aws.amazon.com/sdk-for-python)

