#!/bin/bash
echo "[" && cat "$1" | awk '{str="\""$0"\""","; print str}' | sed '$ s/.$//' &&  echo "]"
