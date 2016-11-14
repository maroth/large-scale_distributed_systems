#!/bin/bash
killall lua
lua chord.lua 2 2 &
lua chord.lua 1 2
