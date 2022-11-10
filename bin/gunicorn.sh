#!/bin/sh
gunicorn flask_main:app -w 2 -b 0.0.0.0:5000