#!/bin/bash

echo """
---
config:
  sankey:
    width: 2000
    height: 1200
    showValues: true
    linkColor: gradient
    nodeAlignment: left
---
sankey-beta
"""

python3 queryActivitySamkey.py 2>/dev/null | egrep -v 'NULL|samkey_row|^\+' | sed 's/\s*|//g'
