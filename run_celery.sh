#!/bin/bash
celery -A celery_tasks worker -E -l INFO