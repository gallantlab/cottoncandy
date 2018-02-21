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

# Summary

[cottoncandy](http://gallantlab.github.io/cottoncandy) is a python scientific library for storing and accessing numpy array data from S3. This is achieved by uploading arrays from memory and downloading arrays directly into memory. This means that you don't have to download your array to disk, and then load it from disk into your python session. This library relies heavily on [boto3](https://aws.amazon.com/sdk-for-python).

# Introduction

Data storage in a traditional shared environment cluster typically consists of a conventional mounted filesystem. More modern big data storage solutions typically revolve around object stores, which have lower overhead because they require less metadata. [cottoncandy](http://gallantlab.github.io/cottoncandy) is a powerful python-based tool for accessing and storing NumPy[@oliphant2006guide] array data in object stores (e.g S3 API-enabled CEPH, AWS S3, Google Drive). cottoncandy is implemented to have minimal disk, memory and network overhead. Furthermore, cottoncandy allows users familiar with conventional file systems to work easily with cloud storage solutions from within computing environments like IPython[@perez2007ipython].

# References
