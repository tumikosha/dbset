#!/bin/bash
# pip install build twine
rm -rf dist/ && python -m build && twine upload dist/*

#set -e  # Exit on error

# Remove old builds
#rm -rf dist/ build/ *.egg-info && python -m build

# Build
#python -m build

# Upload (uncomment when ready)
# twine upload dist/*
