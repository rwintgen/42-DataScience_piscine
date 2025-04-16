#!/bin/sh

# Exec script using source cmd
python3 -m venv ds_venv
source ds_venv/bin/activate
pip install --upgrade pip && pip install -r requirements.txt