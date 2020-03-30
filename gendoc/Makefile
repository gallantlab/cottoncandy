# Minimal makefile for Sphinx documentation
#


## Copied from nibabel
PYTHON ?= python3
API_DIR = source/api
BUILDROOT = build
HTMLOUTPATH = ../docs

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = cottoncandy
SOURCEDIR     = source

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDROOT)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile


clean-docs:
	@-rm -rf $(API_DIR)
	@echo "Removing old API Reference: $(API_DIR)."
	@-rm -rf $(HTMLOUTPATH)/*
	@echo "Removing old HTML pages: $(HTMLOUTPATH)."


api:
	@mkdir -p $(API_DIR)
	$(PYTHON) tools/build_modref_templates.py cottoncandy $(API_DIR) False
	@echo "Build API docs...done."


html-only:
	@mkdir -p $(SOURCEDIR)/_static
	mkdir -p $(BUILDROOT)/html $(BUILDROOT)/doctrees
	$(SPHINXBUILD) -b html $(SOURCEDIR) $(BUILDROOT)/html -d $(BUILDROOT)/doctrees
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDROOT)/html."


githubio-docs: clean-docs api html-only
	@mkdir -p $(HTMLOUTPATH)
	@cp -r $(BUILDROOT)/html/* $(HTMLOUTPATH)/
	@touch $(HTMLOUTPATH)/.nojekyll
	@echo
	@echo "Copied HTML pages to $(HTMLOUTPATH)."

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDROOT)" $(SPHINXOPTS) $(O)
