#!/bin/bash

#bw sure to set the session:window.pane e.g. TMUXPANE=0:0.1

pane='0:0.1'
if [ -n "$TMUXPANE" ]; then
  pane="$TMUXPANE"
fi
tmux send-keys -t "$pane" 'tail -f /tmp/bwdb-out' Enter

