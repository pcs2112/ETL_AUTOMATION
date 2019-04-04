#!/bin/bash

_cwd="$PWD"
_targetRoot="$HOME/Downloads"
_rootDirName="ETL_AUTOMATION"

rm -rf "$_targetRoot/$_rootDirName"
rm -rf "$_targetRoot/$_rootDirName.zip"
mkdir "$_targetRoot/$_rootDirName"
mkdir "$_targetRoot/$_rootDirName/in"
mkdir "$_targetRoot/$_rootDirName/in/old"
mkdir "$_targetRoot/$_rootDirName/out"
cp -r "$_cwd/src" "$_targetRoot/$_rootDirName/src"
cp "$_cwd/.env.example" "$_targetRoot/$_rootDirName/.env"
cp "$_cwd/app.py" "$_targetRoot/$_rootDirName/app.py"
cp "$_cwd/requirements.txt" "$_targetRoot/$_rootDirName/requirements.txt"
find "$_targetRoot/$_rootDirName" | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
cd "$_targetRoot"
zip -r "$_targetRoot/$_rootDirName.zip" ./${_rootDirName}/*
