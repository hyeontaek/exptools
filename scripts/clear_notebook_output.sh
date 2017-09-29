#!/bin/sh

for FILENAME in `find exptools/sample/*.ipynb`; do
  ( jupyter nbconvert "${FILENAME}" --to notebook --ClearOutputPreprocessor.enabled=True --stdout > "${FILENAME}.new" ) && mv "${FILENAME}.new" "${FILENAME}"
done
